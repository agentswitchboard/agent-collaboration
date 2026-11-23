#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A scripted peer fixture fails at the unexpected command or observation."
)]

use central_link_client::{
    AdmittedConnection, CheckedBatchResults, ClientAction, CorrelatedReceiptResults,
    CorrelatedRequestReply, CorrelatedStatusReply, ImmediateRequestCommand, LinkClient,
    LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane, RegisteredConnection, RequestStatusCommand,
    SubmitReceiptsCommand, UntouchedCommand, WriteBatchCommand, WriteBatchDisposition,
    WriteBatchMemberDisposition,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, ReceiptResult, ReplyPayload, RequestPayload,
    RequestStatusAnswer, RequestStatusQuery, decode_text, encode_text,
};
use hegel::{TestCase, generators};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, OutboxAccepted, OutboxRequestStatus, OutboxStorage,
    PendingWriteState,
};
use switchboard_message_custody::flush_clock::FlushJitter;
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_submission::{Submission, SubmissionPayload},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::{Notify, oneshot, watch};
use workspace_ids::{MachineId, RequestId, SessionId};
use workspace_model::{
    Caller, ChangePosition, ItemRef, WorkspaceAnswer, WorkspaceReply, WorkspaceResult,
    central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRefusal, CentralMailResult,
        MessageStatus,
    },
};

use super::outbox_timing::{manual_clock, open_outbox, thread_post, welcome_anchor};

const SHARED_MARK_MS: i64 = 1_763_380_800_000;

pub(super) struct KnownRecipient;

pub(super) struct NoFlushJitter;

impl FlushJitter for NoFlushJitter {
    fn jitter_millis(&self, _: SyncTime) -> i64 {
        0
    }
}

impl RecipientSessionDirectory for KnownRecipient {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(std::future::ready(RecipientSessionAnswer::KnownOnce))
    }
}

#[derive(Default)]
struct PeerHistory {
    attempts: Vec<Vec<RequestId>>,
    decisions: BTreeMap<RequestId, ReplyPayload>,
    commands: Vec<WriteBatchCommand>,
    recorded_writes: BTreeMap<RequestId, (Caller, RequestPayload)>,
}

pub(super) enum ScriptedCommand {
    RequestStatus(RequestStatusCommand),
    ImmediateRequest(ImmediateRequestCommand),
}

/// A test selects the peer's fate for one member of a transmitted batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ScriptedMemberFate {
    Answered,
    RecordedAnswerLost,
    LostBeforeCentral,
}

/// A test scripts the next lane submission or the fates of its transmitted members.
pub(super) enum ScriptedBatch {
    Untouched,
    Members(Vec<ScriptedMemberFate>),
}

/// A test controls when a status disposition becomes available and supplies the lane's acceptance stamp.
pub(super) struct ScriptedStatusGate {
    pub(super) entered: Notify,
    pub(super) accepted: watch::Sender<Option<ClockInstant>>,
}

/// A test holds one write disposition after observing the batch command.
#[derive(Default)]
pub(super) struct ScriptedBatchGate {
    pub(super) entered: Notify,
    pub(super) release: Notify,
}

pub(super) struct ScriptedBatchLane {
    pub(super) admitted: watch::Sender<Option<AdmittedConnection>>,
    clock: Arc<ManualClock>,
    lost_first_answers: BTreeSet<RequestId>,
    history: Mutex<PeerHistory>,
    calls: Mutex<Vec<ScriptedCommand>>,
    events: Mutex<std::collections::VecDeque<LinkLaneEvent>>,
    refused_machine: Mutex<Option<MachineId>>,
    next_batch: Mutex<Option<ScriptedBatch>>,
    status_answers: Mutex<Vec<(Caller, RequestStatusQuery, RequestStatusAnswer)>>,
    held_status: Mutex<Option<Arc<ScriptedStatusGate>>>,
    held_batch: Mutex<Option<Arc<ScriptedBatchGate>>>,
    held_receipt_completion: Mutex<Option<oneshot::Receiver<()>>>,
    receipt_commands: watch::Sender<Vec<(ClockInstant, SubmitReceiptsCommand)>>,
}

impl ScriptedBatchLane {
    pub(super) fn new(
        admitted: AdmittedConnection,
        clock: Arc<ManualClock>,
        lost_first_answers: BTreeSet<RequestId>,
    ) -> Self {
        Self {
            admitted: watch::channel(Some(admitted)).0,
            clock,
            lost_first_answers,
            history: Mutex::new(PeerHistory::default()),
            calls: Mutex::new(Vec::new()),
            events: Mutex::new(std::collections::VecDeque::new()),
            refused_machine: Mutex::new(None),
            next_batch: Mutex::new(None),
            status_answers: Mutex::new(Vec::new()),
            held_status: Mutex::new(None),
            held_batch: Mutex::new(None),
            held_receipt_completion: Mutex::new(None),
            receipt_commands: watch::channel(Vec::new()).0,
        }
    }

    pub(super) fn take_calls(&self) -> Vec<ScriptedCommand> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }

    pub(super) fn batch_commands(&self) -> Vec<WriteBatchCommand> {
        self.history.lock().unwrap().commands.clone()
    }

    pub(super) fn decide_next_batch(&self, batch: Option<ScriptedBatch>) {
        *self.next_batch.lock().unwrap() = batch;
    }

    pub(super) fn decisions(&self) -> BTreeMap<RequestId, ReplyPayload> {
        self.history.lock().unwrap().decisions.clone()
    }

    pub(super) fn hold_next_status(&self) -> Arc<ScriptedStatusGate> {
        let gate = Arc::new(ScriptedStatusGate {
            entered: Notify::new(),
            accepted: watch::channel(None).0,
        });
        *self.held_status.lock().unwrap() = Some(gate.clone());
        gate
    }

    pub(super) fn hold_next_batch(&self) -> Arc<ScriptedBatchGate> {
        let gate = Arc::new(ScriptedBatchGate::default());
        *self.held_batch.lock().unwrap() = Some(gate.clone());
        gate
    }

    pub(super) fn hold_next_receipt_completion(&self) -> oneshot::Sender<()> {
        let (release, held) = oneshot::channel();
        assert!(
            self.held_receipt_completion
                .lock()
                .unwrap()
                .replace(held)
                .is_none()
        );
        release
    }

    pub(super) fn watch_receipt_commands(
        &self,
    ) -> watch::Receiver<Vec<(ClockInstant, SubmitReceiptsCommand)>> {
        self.receipt_commands.subscribe()
    }

    pub(super) fn script_status_answer(
        &self,
        caller: Caller,
        query: RequestStatusQuery,
        answer: RequestStatusAnswer,
    ) {
        self.status_answers
            .lock()
            .unwrap()
            .push((caller, query, answer));
    }

    pub(super) fn push_event(&self, event: LinkLaneEvent) {
        self.events.lock().unwrap().push_back(event);
    }

    pub(super) fn refuse_messages_to(&self, machine: MachineId) {
        *self.refused_machine.lock().unwrap() = Some(machine);
    }

    fn respond_to_batch(&self, batch: WriteBatchCommand) -> WriteBatchDisposition {
        let Some(admitted) = self.admitted.borrow().clone() else {
            return WriteBatchDisposition::NotReached {
                cause: UntouchedCommand::ConnectionUnavailable,
            };
        };
        assert_eq!(batch.connection, admitted.connection);
        let script = self.next_batch.lock().unwrap().take();
        let scripted_members = match script {
            Some(ScriptedBatch::Untouched) => {
                return WriteBatchDisposition::NotReached {
                    cause: UntouchedCommand::WriterBusy,
                };
            }
            Some(ScriptedBatch::Members(members)) => Some(members),
            None => None,
        };
        let wire = LinkFrame::Data(DataFrame::Batch {
            requests: batch.requests.clone(),
        });
        let text = encode_text(&wire, FrameDirection::ToCentral).unwrap();
        let LinkFrame::Data(DataFrame::Batch { requests }) =
            decode_text(&text, FrameDirection::ToCentral).unwrap()
        else {
            panic!("custody did not send a batch frame");
        };
        let mut history = self.history.lock().unwrap();
        history.commands.push(batch);
        let first_batch = history.attempts.is_empty();
        let mut sent_requests = Vec::new();
        let mut members = Vec::new();
        for (index, request) in requests.requests().iter().enumerate() {
            let id = request.payload().write_request_id().unwrap();
            sent_requests.push(id.clone());
            let fate = scripted_members
                .as_ref()
                .and_then(|members| members.get(index))
                .copied()
                .unwrap_or_else(|| {
                    if first_batch && self.lost_first_answers.contains(id) {
                        ScriptedMemberFate::RecordedAnswerLost
                    } else {
                        ScriptedMemberFate::Answered
                    }
                });
            let correlation_id = request.correlation_id().clone();
            let observed_write = (request.caller().clone(), request.payload().clone());
            let original_write = history
                .recorded_writes
                .entry(id.clone())
                .or_insert_with(|| observed_write.clone());
            assert_eq!(
                original_write, &observed_write,
                "a replay must preserve caller, target and payload"
            );
            if fate == ScriptedMemberFate::LostBeforeCentral {
                members.push(WriteBatchMemberDisposition::AnswerLost { correlation_id });
                continue;
            }
            let position = history.decisions.len() + 1;
            let refused_machine = self.refused_machine.lock().unwrap().clone();
            let answer = history.decisions.entry(id.clone()).or_insert_with(|| {
                peer_answer(request.payload(), position, refused_machine.as_ref())
            });
            let disposition = if fate == ScriptedMemberFate::RecordedAnswerLost {
                WriteBatchMemberDisposition::AnswerLost { correlation_id }
            } else {
                WriteBatchMemberDisposition::Answered {
                    accepted_at: self.clock.monotonic_now(),
                    reply: CorrelatedRequestReply {
                        correlation_id,
                        result: Box::new(answer.clone()),
                    },
                }
            };
            members.push(disposition);
        }
        history.attempts.push(sent_requests);
        if first_batch && !self.lost_first_answers.is_empty() {
            self.admitted.send_replace(None);
        }
        WriteBatchDisposition::MemberResults(CheckedBatchResults::new(&requests, members).unwrap())
    }
}

impl ReadyLinkLane for ScriptedBatchLane {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>> {
        self.admitted.subscribe()
    }

    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>> {
        panic!("message custody requires only admission")
    }

    fn submit_write_batch(
        &self,
        batch: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>> {
        let gate = self.held_batch.lock().unwrap().take();
        let disposition = self.respond_to_batch(batch);
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            disposition
        })
    }

    fn submit_receipts(
        &self,
        command: SubmitReceiptsCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedReceiptResults>> + Send + '_>>
    {
        let accepted_at = self.clock.monotonic_now();
        self.receipt_commands
            .send_modify(|commands| commands.push((accepted_at, command.clone())));
        let held = self.held_receipt_completion.lock().unwrap().take();
        let reply = CorrelatedReceiptResults {
            correlation_id: command.correlation_id,
            results: vec![ReceiptResult::Recorded; command.receipts.receipts().len()],
        };
        Box::pin(async move {
            if let Some(held) = held {
                held.await.unwrap();
            }
            LinkLaneDisposition::Answered { reply, accepted_at }
        })
    }

    fn request_status(
        &self,
        command: RequestStatusCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedStatusReply>> + Send + '_>> {
        let answer = self
            .status_answers
            .lock()
            .unwrap()
            .iter()
            .find(|(owner, query, _)| owner == &command.caller && query == &command.query)
            .map_or(RequestStatusAnswer::NotFound, |(_, _, answer)| {
                answer.clone()
            });
        let reply = CorrelatedStatusReply {
            correlation_id: command.correlation_id.clone(),
            result: Box::new(answer),
        };
        self.calls
            .lock()
            .unwrap()
            .push(ScriptedCommand::RequestStatus(command));
        let gate = self.held_status.lock().unwrap().take();
        Box::pin(async move {
            let accepted_at = if let Some(gate) = gate {
                let mut accepted = gate.accepted.subscribe();
                gate.entered.notify_one();
                match accepted.wait_for(Option::is_some).await {
                    Ok(instant) => (*instant).unwrap(),
                    Err(_) => return LinkLaneDisposition::AnswerLost,
                }
            } else {
                self.clock.monotonic_now()
            };
            LinkLaneDisposition::Answered { reply, accepted_at }
        })
    }

    fn submit_immediate_request(
        &self,
        command: ImmediateRequestCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedRequestReply>> + Send + '_>>
    {
        let reply = CorrelatedRequestReply {
            correlation_id: command.request.correlation_id().clone(),
            result: Box::new(ReplyPayload::Workspace(WorkspaceReply {
                result: WorkspaceResult::Done(WorkspaceAnswer::MessageStatus {
                    status: MessageStatus::NotFound {},
                }),
                actors: Vec::new(),
            })),
        };
        self.calls
            .lock()
            .unwrap()
            .push(ScriptedCommand::ImmediateRequest(command));
        Box::pin(std::future::ready(LinkLaneDisposition::Answered {
            reply,
            accepted_at: self.clock.monotonic_now(),
        }))
    }

    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>> {
        Box::pin(std::future::ready(self.events.lock().unwrap().pop_front()))
    }
}

pub(super) fn post_created(position: usize) -> ReplyPayload {
    let post_id = format!("019f0000-0000-7000-8000-{position:012x}")
        .parse()
        .unwrap();
    ReplyPayload::Workspace(WorkspaceReply {
        result: WorkspaceResult::Done(WorkspaceAnswer::Created {
            item: ItemRef::Post(post_id),
            position: ChangePosition::try_from(u64::try_from(position).unwrap()).unwrap(),
        }),
        actors: Vec::new(),
    })
}

fn peer_answer(
    payload: &RequestPayload,
    position: usize,
    refused_machine: Option<&MachineId>,
) -> ReplyPayload {
    match payload {
        RequestPayload::Workspace(_) => post_created(position),
        RequestPayload::Mail(request) => {
            let CentralMailOperation::SendMessage { to, .. } = &request.operation else {
                panic!("script expects a direct message");
            };
            let result = if refused_machine == Some(&to.machine_id) {
                CentralMailResult::Refused(CentralMailRefusal::UnknownSession {
                    target: to.clone(),
                })
            } else {
                CentralMailResult::Done(CentralMailAnswer::Queued {
                    message_id: format!("019f0000-0000-7000-8000-{position:012x}")
                        .parse()
                        .unwrap(),
                })
            };
            ReplyPayload::Mail(result)
        }
    }
}

pub(super) fn new_admission(machine: MachineId, clock: Arc<ManualClock>) -> AdmittedConnection {
    let mut client = LinkClient::new(machine, clock.clone());
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("the scripted socket needs a fresh connection id");
    };
    AdmittedConnection {
        connection: attempt,
        clock_anchor: welcome_anchor(clock.as_ref(), clock.utc_now().timestamp_millis()),
    }
}

async fn check_partial_replay(answers: &[bool]) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("machine_local.sqlite");
    let started_at = SHARED_MARK_MS - 20_000;
    let clock = manual_clock(started_at);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let items: Vec<_> = (1..=u16::try_from(answers.len()).unwrap())
        .map(thread_post)
        .collect();
    let lost: BTreeSet<_> = items
        .iter()
        .zip(answers)
        .filter(|(_, answered)| !**answered)
        .map(|(item, _)| item.request.clone())
        .collect();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        clock.clone(),
        lost.clone(),
    ));
    let outbox = open_outbox(&database).await;
    let mut custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        OutboxWork::new(Box::new(outbox), clock.clone()),
        RecipientInbox::new(
            InboxStore::new(MachineLocalStore::open(&database).await.unwrap()),
            machine.clone(),
            Arc::new(KnownRecipient),
            clock.clone(),
        ),
    );
    for item in &items {
        let switchboard_machine_core_store::OutboxQueuedWrite::Workspace(request) = &item.payload
        else {
            panic!("the replay fixture supplies thread posts");
        };
        let submission = custody
            .submit(
                item.caller.clone(),
                SubmissionPayload::Workspace(request.clone()),
            )
            .await;
        assert!(matches!(submission, Submission::InOutbox(receipt)
            if receipt.id == item.request && receipt.flush_at.unix_millis() == SHARED_MARK_MS));
    }

    assert!(custody.flush_writes().await.unwrap().answered.is_empty());
    assert!(
        lane.history.lock().unwrap().attempts.is_empty(),
        "no frame before the shared minute"
    );
    clock.advance(Duration::from_secs(20)).unwrap();
    let first = custody.flush_writes().await.unwrap();
    assert!(first.issues.is_empty());
    assert_eq!(
        first.answered.len(),
        items.len() - lost.len(),
        "retain replies received before the drop"
    );
    let mut observed = open_outbox(&database).await;
    for item in &items {
        let state = observed
            .pending_for(&item.caller)
            .await
            .unwrap()
            .into_iter()
            .find(|write| write.request_id == item.request)
            .unwrap()
            .state;
        assert_eq!(
            matches!(state, PendingWriteState::SentAnswerLost { .. }),
            lost.contains(&item.request)
        );
    }

    lane.admitted
        .send_replace(Some(new_admission(machine, clock.clone())));
    clock.advance(Duration::from_secs(60)).unwrap();
    let replay = custody.flush_writes().await.unwrap();
    assert!(replay.issues.is_empty());
    let (sent_again, decisions) = {
        let history = lane.history.lock().unwrap();
        assert_eq!(history.attempts.len(), 2);
        (
            history.attempts.get(1).cloned().unwrap(),
            history.decisions.clone(),
        )
    };
    assert_eq!(sent_again, lost.into_iter().collect::<Vec<_>>());
    assert_eq!(decisions.len(), items.len());
    for item in items {
        let status = observed
            .request_status(&item.caller, &item.request)
            .await
            .unwrap();
        let expected = decisions.get(&item.request).unwrap();
        assert!(matches!((status, expected), (
            OutboxRequestStatus::Answered(OutboxAccepted::ThreadPost { post_id, position, actors }),
            ReplyPayload::Workspace(WorkspaceReply {
                result: WorkspaceResult::Done(WorkspaceAnswer::Created { item: ItemRef::Post(expected_id), position: expected_position }),
                actors: expected_actors,
            })
        ) if &post_id == expected_id && &position == expected_position && &actors == expected_actors));
    }
}

#[hegel::test(test_cases = 64)]
fn a_drop_replays_only_the_batch_members_whose_answers_were_lost(case: TestCase) {
    let count = case.draw(generators::integers::<u8>().min_value(0).max_value(5));
    let mut answers = vec![true, false];
    answers.extend((0..count).map(|_| case.draw(generators::booleans())));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(check_partial_replay(&answers));
}
