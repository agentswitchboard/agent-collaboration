//! PR 307's store-fault and caller-overlay campaign against the current custody ports.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A failed campaign fixture reports the precise failed frontier."
)]

use central_link_protocol::{ReplyPayload, RequestScope, RequestStatusQuery};
use hegel::{TestCase, generators};
use router_clock::{ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxDecision, InboxOfferExpiry, InboxStore, MachineLocalStore, NewOutboxItem, OutboxAccepted,
    OutboxCancellation, OutboxQueuedWrite, OutboxStorage, PendingWrite, PendingWriteState,
};
use switchboard_message_custody::{
    custody_link::{CustodyLink, CustodyLinkError},
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    flush_marks::{FLUSH_JITTER_MILLIS, mark_in_window},
    outbox_dispatch::FlushReport,
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use workspace_ids::{MachineId, RequestId};
use workspace_model::{
    Caller, ItemRef, WorkspaceAnswer, WorkspaceReply, WorkspaceResult,
    central_mail::{MessageBody, MessageRecord},
};

use super::{
    batch_replay::{
        KnownRecipient, NoFlushJitter, ScriptedBatch, ScriptedBatchLane, ScriptedMemberFate,
        new_admission,
    },
    faulty_outbox::{FaultSite, FaultStage, FaultyOutbox, StoreFaultPlan},
    outbox_timing::{manual_clock, open_outbox, thread_post},
    worker_turns::{ObservedWorker, WorkerClock},
};

pub(super) const MARK_MS: i64 = 1_763_380_800_000;
pub(super) const START_MS: i64 = MARK_MS - 20_000;
const HEALED_WITHIN_MARKS: usize = 4;

const FLUSH_FAULTS: [FaultSite; 9] = [
    FaultSite::Insert,
    FaultSite::EarliestWaiting,
    FaultSite::SentWithoutAnswer,
    FaultSite::EndOverdue,
    FaultSite::RetentionPrune,
    FaultSite::ClaimBatch,
    FaultSite::RecordOutcome,
    FaultSite::ReturnNotTransmitted,
    FaultSite::ReturnUnanswered,
];

/// A test creates this SQLite custody and scripted peer for each fault history.
pub(super) struct FaultCampaign {
    pub(super) root: tempfile::TempDir,
    pub(super) custody: CustodyLink,
    pub(super) clock: Arc<ManualClock>,
    pub(super) peer: Arc<ScriptedBatchLane>,
    pub(super) faults: StoreFaultPlan,
    machine: MachineId,
}

impl FaultCampaign {
    pub(super) async fn start() -> Self {
        let clock = manual_clock(START_MS);
        Self::start_with_clock(clock.clone(), clock).await
    }

    pub(super) async fn start_with_clock(
        clock: Arc<ManualClock>,
        observed_clock: Arc<dyn RuntimeClock>,
    ) -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("machine_local.sqlite");
        let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
        let peer = Arc::new(ScriptedBatchLane::new(
            new_admission(machine.clone(), clock.clone()),
            clock.clone(),
            Default::default(),
        ));
        let faults = StoreFaultPlan::default();
        let inner = open_outbox(&path).await;
        let work = OutboxWork::new(
            Box::new(FaultyOutbox {
                inner: Box::new(inner),
                plan: faults.clone(),
            }),
            observed_clock.clone(),
        );
        let inbox = RecipientInbox::new(
            InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
            machine.clone(),
            Arc::new(KnownRecipient),
            observed_clock.clone(),
        );
        let custody = CustodyLink::new(
            peer.clone(),
            observed_clock,
            Arc::new(NoFlushJitter),
            work,
            inbox,
        );
        Self {
            root,
            custody,
            clock,
            peer,
            faults,
            machine,
        }
    }
}

pub(super) struct WorkerFaultCampaign {
    pub(super) root: tempfile::TempDir,
    pub(super) worker: ObservedWorker,
    pub(super) clock: Arc<ManualClock>,
    pub(super) peer: Arc<ScriptedBatchLane>,
    pub(super) faults: StoreFaultPlan,
    machine: MachineId,
    caller: Caller,
    accepted: BTreeMap<RequestId, i64>,
    next_id: u16,
}

impl WorkerFaultCampaign {
    pub(super) async fn start() -> Self {
        let observed = WorkerClock::observing(manual_clock(START_MS));
        let fixture =
            FaultCampaign::start_with_clock(observed.manual.clone(), observed.clone()).await;
        let campaign = Self::from_fixture(fixture, observed.clone()).await;
        observed.wait_after(0).await;
        campaign
    }

    pub(super) async fn from_fixture(fixture: FaultCampaign, observed: Arc<WorkerClock>) -> Self {
        // A confirmed inbox row gives the select a far-future retention wait
        // even after the scripted lane disconnects and all writes have ended.
        let Caller::Session(recipient) = thread_post(1).caller else {
            panic!("the fixture uses a session caller");
        };
        let arrival = fixture.clock.utc_now();
        let note = MessageRecord::queued_central(
            "019f0000-0000-7000-8000-000000000191".parse().unwrap(),
            recipient,
            MessageBody::Notice { items: Vec::new() },
            arrival,
        );
        let path = fixture.root.path().join("machine_local.sqlite");
        let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
        inbox
            .record_delivery(
                &note,
                InboxDecision::Accepted,
                arrival.timestamp_millis(),
                InboxOfferExpiry::SystemWithoutExpiry,
            )
            .await
            .unwrap();
        inbox
            .mark_receipt_confirmed(&note.message_id, arrival.timestamp_millis())
            .await
            .unwrap();
        Self {
            root: fixture.root,
            worker: ObservedWorker::start(fixture.custody, observed),
            clock: fixture.clock,
            peer: fixture.peer,
            faults: fixture.faults,
            machine: fixture.machine,
            caller: thread_post(1).caller,
            accepted: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub(super) async fn stop(self) {
        self.worker.stop().await;
    }

    pub(super) async fn submit(&mut self, fault: Option<FaultStage>) -> NewOutboxItem {
        let item = thread_post(self.next_id);
        self.next_id += 1;
        assert_eq!(item.caller, self.caller);
        let OutboxQueuedWrite::Workspace(request) = item.payload.clone() else {
            panic!("fixture must submit a thread post");
        };
        if let Some(fault) = fault {
            self.faults.arm(FaultSite::Insert, fault);
        }
        let reply = self
            .worker
            .calls
            .submit(item.caller.clone(), SubmissionPayload::Workspace(request))
            .await;
        let visible = self
            .pending()
            .await
            .iter()
            .any(|stored| stored.request_id == item.request);
        match (fault, reply) {
            (None, Submission::InOutbox(receipt)) => {
                assert_eq!(receipt.id, item.request);
                assert!(visible, "a receipt means the store accepted the write");
            }
            (
                Some(FaultStage::BeforeCall),
                Submission::Refused(SubmitRefusal::OutboxUnavailable),
            ) => {
                assert!(!visible, "a known begin failure cannot leave a row");
            }
            (Some(FaultStage::AfterSuccess), Submission::Uncertain { request, scope }) => {
                assert_eq!(request, item.request);
                assert_eq!(scope, queued_scope(&item));
                assert!(
                    visible,
                    "a lost acknowledgement must not drop the committed write"
                );
            }
            (fault, reply) => panic!("unexpected submission under {fault:?}: {reply:?}"),
        }
        if visible {
            self.accepted.insert(
                item.request.clone(),
                self.clock.utc_now().timestamp_millis(),
            );
        }
        item
    }

    pub(super) fn accept_seeded(&mut self, item: &NewOutboxItem, created_at_ms: i64) {
        self.accepted.insert(item.request.clone(), created_at_ms);
    }

    pub(super) async fn pending(&mut self) -> Vec<PendingWrite> {
        self.worker
            .calls
            .pending_writes(self.caller.clone())
            .await
            .unwrap()
    }

    pub(super) fn disconnect(&self) {
        self.peer.admitted.send_replace(None);
    }

    pub(super) fn reconnect(&self) {
        self.peer.admitted.send_replace(Some(new_admission(
            self.machine.clone(),
            self.clock.clone(),
        )));
    }

    pub(super) async fn next_mark(
        &mut self,
        script: Option<ScriptedBatch>,
    ) -> Result<FlushReport, CustodyLinkError> {
        self.wait_for_worker_read().await?;
        let now = self.clock.utc_now().timestamp_millis();
        let mark = (now.div_euclid(60_000) + 1) * 60_000;
        let elapsed = Duration::from_millis(u64::try_from(mark - now).unwrap());
        self.peer.decide_next_batch(script);
        let result = self.worker.advance(elapsed).await;
        self.peer.decide_next_batch(None);
        result
    }

    async fn wait_for_worker_read(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let before = self.worker.clock.checkpoint();
        self.pending().await;
        self.worker.clock.wait_after(before).await;
        self.worker.take_reports()
    }

    async fn fault_waiting_expiry(&mut self, stage: FaultStage) {
        let mut expiring = thread_post(self.next_id);
        self.next_id += 1;
        let now = self.clock.utc_now().timestamp_millis();
        let created = now - 899_999;
        expiring.flush_at_ms = now + 60_000;
        let mut store = open_outbox(&self.root.path().join("machine_local.sqlite")).await;
        store.insert(&expiring, created).await.unwrap();
        self.accept_seeded(&expiring, created);
        self.wait_for_worker_read().await.unwrap();
        self.faults.arm(FaultSite::EndOverdue, stage);
        assert!(self.worker.advance(Duration::from_millis(1)).await.is_err());
    }

    async fn attach_fault_to_phase(&mut self, site: FaultSite, stage: FaultStage) {
        if matches!(
            site,
            FaultSite::ClaimBatch
                | FaultSite::RecordOutcome
                | FaultSite::RetentionPrune
                | FaultSite::ReturnNotTransmitted
                | FaultSite::ReturnUnanswered
        ) {
            // A fresh write submitted inside an unused mark can already have
            // left by the time the test installs its store fault.
            let now = self.clock.utc_now().timestamp_millis();
            if let Some(mark) = mark_in_window(SyncTime::from_unix_millis(now)) {
                let remaining = mark.unix_millis() + FLUSH_JITTER_MILLIS - now;
                self.worker
                    .advance(Duration::from_millis(u64::try_from(remaining).unwrap()))
                    .await
                    .unwrap();
            }
            self.submit(None).await;
        }
        self.wait_for_worker_read().await.unwrap();
        let before = self.faults.fired().len();
        let original_admission = self.peer.admitted.borrow().clone();
        let transmitted_before = self.peer.batch_commands().len();
        if site == FaultSite::Insert {
            self.submit(Some(stage)).await;
        } else if site == FaultSite::EndOverdue {
            self.fault_waiting_expiry(stage).await;
        } else {
            if site == FaultSite::EarliestWaiting {
                self.worker
                    .advance(Duration::from_millis(10))
                    .await
                    .unwrap();
            }
            self.faults.arm(site, stage);
            match site {
                FaultSite::EarliestWaiting => {
                    assert!(self.wait_for_worker_read().await.is_err());
                }
                FaultSite::SentWithoutAnswer => {
                    let before = self.worker.clock.checkpoint();
                    self.peer.admitted.send_replace(original_admission.clone());
                    self.worker.clock.wait_after(before).await;
                    assert!(self.worker.take_reports().is_err());
                }
                FaultSite::ReturnNotTransmitted => {
                    let report = self
                        .next_mark(Some(ScriptedBatch::Untouched))
                        .await
                        .unwrap();
                    assert!(!report.issues.is_empty(), "faulted return must be visible");
                }
                FaultSite::ReturnUnanswered => {
                    let report = self
                        .next_mark(Some(ScriptedBatch::Members(vec![
                            ScriptedMemberFate::RecordedAnswerLost,
                        ])))
                        .await
                        .unwrap();
                    assert!(!report.issues.is_empty(), "faulted return must be visible");
                }
                FaultSite::ClaimBatch => {
                    assert!(self.next_mark(None).await.is_err());
                    assert_eq!(
                        self.peer.batch_commands().len(),
                        transmitted_before,
                        "a failed claim must leave the admitted connection idle"
                    );
                }
                FaultSite::RecordOutcome | FaultSite::RetentionPrune => {
                    let report = self.next_mark(None).await.unwrap();
                    assert!(
                        !report.issues.is_empty(),
                        "store fault must reach the report"
                    );
                }
                FaultSite::Insert
                | FaultSite::EndOverdue
                | FaultSite::ScopedStatus
                | FaultSite::RecordSenderNotice => {
                    panic!("phase not used for {site:?}")
                }
            }
        }
        assert_eq!(
            self.faults.fired().get(before..),
            Some([(site, stage)].as_slice()),
            "the named fault must actually fire"
        );
        assert!(self.faults.is_drained());
        assert_eq!(
            *self.peer.admitted.borrow(),
            original_admission,
            "a store error is not a link failure"
        );
    }

    async fn check_overlay(&mut self) {
        self.wait_for_worker_read().await.unwrap();
        let accepted = self.peer.decisions();
        let pending = self.pending().await;
        let mut seen = BTreeMap::new();
        for item in pending {
            let id = item.request_id;
            let inserted_at = self.accepted.get(&id).unwrap();
            assert!(
                seen.insert(id.clone(), ()).is_none(),
                "no duplicate local row for {id}"
            );
            match item.state {
                PendingWriteState::Unsent { .. } => assert!(
                    !accepted.contains_key(&id),
                    "unsent item {id} cannot be in Central"
                ),
                PendingWriteState::SentAnswerLost { .. } => {}
                PendingWriteState::Answered(answer) => {
                    assert_central_post(&answer, accepted.get(&id).unwrap())
                }
                PendingWriteState::Refused(reason) => {
                    panic!(
                        "the scripted Central must not refuse a generated thread post: {reason:?}"
                    );
                }
                PendingWriteState::Cancelled { reason } => {
                    assert!(
                        self.clock.utc_now().timestamp_millis() >= inserted_at + 900_000,
                        "{id} expired before its original deadline"
                    );
                    if reason == OutboxCancellation::NotSentWithin15Minutes {
                        assert!(
                            !accepted.contains_key(&id),
                            "expired unsent item {id} cannot be in Central"
                        );
                    }
                }
            }
        }
        assert!(
            seen.keys().eq(self.accepted.keys()),
            "every accepted write remains in the caller's view"
        );
    }

    pub(super) async fn heal_and_drain(&mut self) {
        assert!(self.faults.is_drained());
        if self.peer.admitted.borrow().is_none() {
            self.reconnect();
        }
        for _ in 0..HEALED_WITHIN_MARKS {
            let report = self.next_mark(None).await.unwrap();
            assert!(
                report.issues.is_empty(),
                "healthy store reported {:?}",
                report.issues
            );
            if self
                .pending()
                .await
                .iter()
                .all(|item| is_terminal(&item.state))
            {
                break;
            }
        }
        self.check_overlay().await;
        let pending = self.pending().await;
        assert!(
            pending.iter().all(|item| is_terminal(&item.state)),
            "everything accepted must settle in {HEALED_WITHIN_MARKS} healthy marks: {pending:?}"
        );
        for id in self.peer.decisions().keys() {
            assert!(
                self.accepted.contains_key(id),
                "Central applied an unaccepted write: {id}"
            );
        }
    }
}

fn is_terminal(state: &PendingWriteState) -> bool {
    matches!(
        state,
        PendingWriteState::Answered(_)
            | PendingWriteState::Refused(_)
            | PendingWriteState::Cancelled { .. }
    )
}

fn queued_scope(item: &NewOutboxItem) -> RequestScope {
    let OutboxQueuedWrite::Workspace(request) = &item.payload else {
        panic!("expected a thread post");
    };
    RequestScope::Workspace(request.workspace_id.clone().unwrap())
}

fn assert_central_post(answer: &OutboxAccepted, wire: &ReplyPayload) {
    assert!(
        matches!((answer, wire), (
        OutboxAccepted::ThreadPost { post_id, position, actors },
        ReplyPayload::Workspace(WorkspaceReply {
            result: WorkspaceResult::Done(WorkspaceAnswer::Created { item: ItemRef::Post(peer_id), position: peer_position }),
            actors: peer_actors,
        }),
    ) if post_id == peer_id && position == peer_position && actors == peer_actors),
        "local answer must preserve the first Central effect"
    );
}

#[tokio::test]
async fn every_307_flush_site_fails_at_both_boundaries_then_heals_and_drains() {
    for site in FLUSH_FAULTS {
        for stage in [FaultStage::BeforeCall, FaultStage::AfterSuccess] {
            let mut campaign = WorkerFaultCampaign::start().await;
            if site != FaultSite::Insert {
                campaign.submit(None).await;
            }
            campaign.attach_fault_to_phase(site, stage).await;
            campaign.heal_and_drain().await;
            assert_eq!(
                campaign.peer.decisions().len(),
                campaign.accepted.len() - usize::from(site == FaultSite::EndOverdue),
                "{site:?} {stage:?}: Central applied exactly the accepted writes that were still live"
            );
            campaign.stop().await;
        }
    }
}

#[tokio::test]
async fn a_scoped_store_read_fault_is_unavailable_without_a_central_fallback() {
    let mut campaign = WorkerFaultCampaign::start().await;
    let item = campaign.submit(None).await;
    for stage in [FaultStage::BeforeCall, FaultStage::AfterSuccess] {
        campaign.faults.arm(FaultSite::ScopedStatus, stage);
        let result = campaign
            .worker
            .calls
            .request_status(
                item.caller.clone(),
                RequestStatusQuery {
                    request_id: item.request.clone(),
                    scope: queued_scope(&item),
                },
            )
            .await;
        assert_eq!(result, CustodyRequestStatus::StatusUnavailable);
        assert_eq!(
            campaign.faults.fired().last(),
            Some(&(FaultSite::ScopedStatus, stage))
        );
        assert!(
            campaign.peer.take_calls().is_empty(),
            "a read fault is not a Central miss"
        );
    }
    campaign.stop().await;
}

#[hegel::test(test_cases = 250)]
fn generated_store_faults_and_link_orders_preserve_the_callers_overlay(case: TestCase) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut campaign = WorkerFaultCampaign::start().await;
        campaign
            .attach_fault_to_phase(FaultSite::Insert, FaultStage::AfterSuccess)
            .await;
        campaign.check_overlay().await;
        let steps = case.draw(generators::integers::<u8>().min_value(3).max_value(24));
        // Keep the oracle independent: generated commands only choose the
        // environment and failure stage. SQLite and the Central script own
        // the actual state, with the asserted overlay checked after each step.
        for _ in 0..steps {
            let current = case.draw(generators::integers::<u8>().min_value(0).max_value(7));
            let stage = if case.draw(generators::booleans()) {
                FaultStage::BeforeCall
            } else {
                FaultStage::AfterSuccess
            };
            match current {
                0 | 1 => {
                    campaign.submit(None).await;
                }
                2 => {
                    campaign
                        .attach_fault_to_phase(FaultSite::Insert, stage)
                        .await;
                }
                3 => {
                    let fates = (0..campaign.accepted.len())
                        .map(|_| {
                            case.draw(generators::sampled_from(
                                [
                                    ScriptedMemberFate::Answered,
                                    ScriptedMemberFate::RecordedAnswerLost,
                                    ScriptedMemberFate::LostBeforeCentral,
                                ]
                                .as_slice(),
                            ))
                        })
                        .collect();
                    campaign
                        .next_mark(Some(ScriptedBatch::Members(fates)))
                        .await
                        .unwrap();
                }
                4 => {
                    campaign
                        .next_mark(Some(ScriptedBatch::Untouched))
                        .await
                        .unwrap();
                }
                5 => {
                    if campaign.peer.admitted.borrow().is_some() {
                        campaign.disconnect();
                    } else {
                        campaign.reconnect();
                    }
                    campaign.next_mark(None).await.unwrap();
                }
                6 => {
                    if campaign.peer.admitted.borrow().is_none() {
                        campaign.reconnect();
                    }
                    let site = case.draw(generators::sampled_from(
                        [
                            FaultSite::EarliestWaiting,
                            FaultSite::SentWithoutAnswer,
                            FaultSite::EndOverdue,
                            FaultSite::ClaimBatch,
                            FaultSite::RecordOutcome,
                            FaultSite::RetentionPrune,
                            FaultSite::ReturnNotTransmitted,
                            FaultSite::ReturnUnanswered,
                        ]
                        .as_slice(),
                    ));
                    campaign.attach_fault_to_phase(site, stage).await;
                }
                _ => campaign.heal_and_drain().await,
            }
            campaign.check_overlay().await;
        }
        campaign.heal_and_drain().await;
        campaign.stop().await;
    });
}
