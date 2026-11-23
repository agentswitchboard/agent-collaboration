use central_link_client::{LinkLaneEvent, ValidatedDeliveryOffer};
use central_link_protocol::{DeliveryMessage, ReplyPayload, RequestScope, RequestStatusQuery};
use router_clock::{ClockInstant, ManualClock};
use std::{
    future::{Future, ready},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxDecision, InboxStore, MachineLocalStore, NewOutboxItem, OutboxCancellation, OutboxStorage,
    RecordDelivery,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_task::{CustodyEventSink, CustodyTaskEvent, CustodyTaskHandle, custody_task},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::{
    sync::{Notify, mpsc, watch},
    task::JoinHandle,
};
use workspace_ids::SessionId;
use workspace_model::{
    Caller, ResolvedSender, Timestamp,
    central_mail::{
        CentralMailAnswer, CentralMailResult, MessageBody, MessageOutcome, MessageRecord,
        MessageRefusal,
    },
};

use super::{
    batch_replay::{NoFlushJitter, ScriptedBatchLane, new_admission},
    custody_task::ObserveTask,
    faulty_outbox::{FaultSite, FaultStage, FaultyOutbox, StoreFaultPlan},
    outbox_timing::{direct_message, manual_clock, open_outbox},
};

const START_MS: i64 = 1_763_380_780_000;

#[derive(Default)]
struct DeferredDirectory {
    lookups: AtomicUsize,
    attempted: Notify,
}

impl RecipientSessionDirectory for DeferredDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        let previous = self.lookups.fetch_add(1, Ordering::SeqCst);
        self.attempted.notify_one();
        Box::pin(ready(if previous == 0 {
            RecipientSessionAnswer::CouldNotTell
        } else {
            RecipientSessionAnswer::KnownOnce
        }))
    }
}

struct NoticeFixture {
    root: tempfile::TempDir,
    clock: Arc<ManualClock>,
    lane: Arc<ScriptedBatchLane>,
    faults: StoreFaultPlan,
    item: NewOutboxItem,
    notice: MessageRecord,
}

impl NoticeFixture {
    #[expect(
        clippy::unwrap_used,
        clippy::panic,
        reason = "A malformed fixture cannot exercise sender notice recovery."
    )]
    async fn prepare() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("machine_local.sqlite");
        let clock = manual_clock(START_MS);
        let item = direct_message(89);
        let Caller::Session(target) = &item.caller else {
            panic!("sender must be a session");
        };
        let notice = MessageRecord::queued_central(
            "019f0000-0000-7000-8000-000000000db1".parse().unwrap(),
            target.clone(),
            MessageBody::DeliveryOutcome {
                message_id: "019f0000-0000-7000-8000-000000000db2".parse().unwrap(),
                request_id: item.request.clone(),
                outcome: MessageOutcome::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
            },
            Timestamp::from_timestamp_millis(START_MS).unwrap(),
        );
        let mut store = open_outbox(&path).await;
        store.insert(&item, START_MS - 900_000).await.unwrap();
        assert_eq!(
            store
                .claim_next_batch(
                    item.flush_at_ms,
                    1,
                    START_MS - 1,
                    START_MS - 1,
                    &std::collections::BTreeSet::new()
                )
                .await
                .unwrap()
                .healthy
                .writes
                .len(),
            1
        );
        assert_eq!(
            store
                .return_unanswered(std::slice::from_ref(&item.request), START_MS)
                .await
                .unwrap()
                .healthy
                .len(),
            1
        );
        let lane = Arc::new(ScriptedBatchLane::new(
            new_admission(target.machine_id.clone(), clock.clone()),
            clock.clone(),
            Default::default(),
        ));
        Self {
            root,
            clock,
            lane,
            faults: StoreFaultPlan::default(),
            item,
            notice,
        }
    }

    #[expect(
        clippy::unwrap_used,
        reason = "The fixture requires the actual reopened SQLite connection."
    )]
    async fn reopen(&self) -> InboxStore {
        InboxStore::new(
            MachineLocalStore::open(&self.root.path().join("machine_local.sqlite"))
                .await
                .unwrap(),
        )
    }

    fn queue_offer(&self) {
        let admitted = self.lane.admitted.borrow().clone();
        if let (Some(admitted), Ok(offer)) = (
            admitted,
            ValidatedDeliveryOffer::new(
                Box::new(DeliveryMessage::System(self.notice.clone())),
                None,
            ),
        ) {
            self.lane.push_event(LinkLaneEvent::Deliver {
                connection: admitted.connection,
                offer,
                sender: ResolvedSender::Central {},
            });
        }
    }

    async fn start(
        &self,
        directory: Arc<DeferredDirectory>,
    ) -> (
        CustodyTaskHandle,
        watch::Sender<bool>,
        mpsc::UnboundedReceiver<CustodyTaskEvent>,
        JoinHandle<()>,
    ) {
        let (sender, observations) = mpsc::unbounded_channel();
        let (calls, stop, worker) = self
            .start_with_sink(directory, Arc::new(ObserveTask(sender)))
            .await;
        (calls, stop, observations, worker)
    }

    async fn start_with_sink(
        &self,
        directory: Arc<DeferredDirectory>,
        sink: Arc<dyn CustodyEventSink>,
    ) -> (CustodyTaskHandle, watch::Sender<bool>, JoinHandle<()>) {
        let database = self.root.path().join("machine_local.sqlite");
        let inbox = RecipientInbox::new(
            self.reopen().await,
            self.notice.target.machine_id.clone(),
            directory,
            self.clock.clone(),
        );
        let outbox = Box::new(FaultyOutbox {
            inner: Box::new(open_outbox(&database).await),
            plan: self.faults.clone(),
        });
        let custody = CustodyLink::new(
            self.lane.clone(),
            self.clock.clone(),
            Arc::new(NoFlushJitter),
            OutboxWork::new(outbox, self.clock.clone()),
            inbox,
        );
        let (stop, stopped) = watch::channel(false);
        let (calls, worker) = custody_task(custody, stopped, sink);
        (calls, stop, tokio::spawn(worker))
    }

    async fn status(&self, calls: &CustodyTaskHandle) -> CustodyRequestStatus {
        calls
            .request_status(
                self.item.caller.clone(),
                RequestStatusQuery {
                    request_id: self.item.request.clone(),
                    scope: RequestScope::Organization,
                },
            )
            .await
    }

    fn projected_status(&self) -> CustodyRequestStatus {
        let MessageBody::DeliveryOutcome { message_id, .. } = &self.notice.body else {
            return CustodyRequestStatus::NotFound;
        };
        CustodyRequestStatus::Answered(ReplyPayload::Mail(CentralMailResult::Done(
            CentralMailAnswer::Queued {
                message_id: message_id.clone(),
            },
        )))
    }
}

#[expect(
    clippy::panic,
    reason = "The fixture identifies which durable sender projection failed."
)]
async fn projection_event(
    events: &mut mpsc::UnboundedReceiver<CustodyTaskEvent>,
) -> CustodyTaskEvent {
    tokio::select! {
        biased;
        Some(event) = events.recv() => event,
        () = async { for _ in 0..50_000 { tokio::task::yield_now().await; } } => {
            panic!("no sender projection event followed the owed work");
        }
    }
}

struct StoppingNoticeSink;

impl CustodyEventSink for StoppingNoticeSink {
    #[expect(
        clippy::panic,
        reason = "Cut the worker at the exact start of notice hand-off."
    )]
    fn record_event(&self, event: CustodyTaskEvent) {
        if matches!(event, CustodyTaskEvent::Flush(report) if !report.sender_notices.is_empty()) {
            panic!("fixture stops before accepting the sender notice");
        }
    }
}

#[tokio::test]
async fn a_crash_at_the_sender_notice_sink_replays_the_same_owed_notice() {
    let fixture = NoticeFixture::prepare().await;
    fixture.lane.admitted.send_replace(None);
    let mut inbox = fixture.reopen().await;
    inbox
        .record_delivery(&fixture.notice, InboxDecision::Accepted, START_MS)
        .await
        .unwrap();
    inbox
        .mark_receipt_confirmed(&fixture.notice.message_id, START_MS)
        .await
        .unwrap();
    let (_calls, _stop, first_run) = fixture
        .start_with_sink(
            Arc::new(DeferredDirectory::default()),
            Arc::new(StoppingNoticeSink),
        )
        .await;
    assert!(first_run.await.unwrap_err().is_panic());
    assert_eq!(
        inbox.pending_handoffs().await.unwrap().healthy.as_slice(),
        std::slice::from_ref(&fixture.notice),
        "the typed notice must still be owed when a sink crashes before accepting it"
    );
    let mut outbox = open_outbox(&fixture.root.path().join("machine_local.sqlite")).await;
    let retained = outbox
        .sender_outcome(&fixture.item.caller, &fixture.item.request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.inbox_message_id, fixture.notice.message_id);

    let (calls, stop, mut events, restarted) =
        fixture.start(Arc::new(DeferredDirectory::default())).await;
    assert!(
        matches!(projection_event(&mut events).await, CustodyTaskEvent::Flush(report)
        if matches!(report.sender_notices.as_slice(), [sent] if sent.notice == retained))
    );
    assert_eq!(fixture.status(&calls).await, fixture.projected_status());
    assert!(inbox.pending_handoffs().await.unwrap().healthy.is_empty());
    stop.send_replace(true);
    restarted.await.unwrap();
}

#[tokio::test]
async fn startup_drains_an_acknowledged_sender_notice_and_reconciles_lost_commit_acks() {
    for stage in [
        None,
        Some(FaultStage::BeforeCall),
        Some(FaultStage::AfterSuccess),
    ] {
        let fixture = NoticeFixture::prepare().await;
        let mut prior = fixture.reopen().await;
        assert_eq!(
            prior
                .record_delivery(&fixture.notice, InboxDecision::Accepted, START_MS)
                .await
                .unwrap(),
            RecordDelivery::Inserted
        );
        assert_eq!(
            prior
                .record_delivery(&fixture.notice, InboxDecision::Accepted, START_MS)
                .await
                .unwrap(),
            RecordDelivery::Duplicate
        );
        prior
            .mark_receipt_confirmed(&fixture.notice.message_id, START_MS)
            .await
            .unwrap();
        drop(prior);
        fixture.lane.admitted.send_replace(None);
        if let Some(stage) = stage {
            fixture.faults.arm(FaultSite::RecordSenderNotice, stage);
        }
        let (calls, stop, mut events, running) =
            fixture.start(Arc::new(DeferredDirectory::default())).await;

        if stage == Some(FaultStage::BeforeCall) {
            assert_eq!(
                fixture.status(&calls).await,
                CustodyRequestStatus::Ended {
                    reason: OutboxCancellation::NotConfirmedWithin15Minutes
                }
            );
            assert!(matches!(
                projection_event(&mut events).await,
                CustodyTaskEvent::Failed(_)
            ));
            assert_eq!(
                fixture
                    .reopen()
                    .await
                    .pending_handoffs()
                    .await
                    .unwrap()
                    .healthy
                    .as_slice(),
                std::slice::from_ref(&fixture.notice)
            );
            let retry = ClockInstant::ZERO.after(Duration::from_secs(20));
            fixture.clock.not_due_check_now(retry).await;
            fixture.clock.advance(Duration::from_secs(20)).unwrap();
        } else {
            assert_eq!(
                fixture.status(&calls).await,
                fixture.projected_status(),
                "startup must apply a durable acknowledged notice or read back its lost commit"
            );
        }
        assert!(
            matches!(projection_event(&mut events).await, CustodyTaskEvent::Flush(report)
            if matches!(report.sender_notices.as_slice(), [known]
                if known.caller == fixture.item.caller && known.notice.inbox_message_id == fixture.notice.message_id))
        );
        assert_eq!(fixture.status(&calls).await, fixture.projected_status());
        assert!(
            fixture
                .reopen()
                .await
                .pending_handoffs()
                .await
                .unwrap()
                .healthy
                .is_empty()
        );
        assert!(fixture.lane.take_calls().is_empty());
        assert!(fixture.faults.is_drained());
        stop.send_replace(true);
        running.await.unwrap();
        assert!(
            events.try_recv().is_err(),
            "the durable notice reports only once"
        );
    }
}

#[tokio::test]
async fn a_deferred_sender_notice_is_projected_after_lookup_heals_without_a_reoffer() {
    let fixture = NoticeFixture::prepare().await;
    fixture.queue_offer();
    let directory = Arc::new(DeferredDirectory::default());
    let (calls, stop, mut events, running) = fixture.start(directory.clone()).await;
    directory.attempted.notified().await;
    assert_eq!(
        fixture.status(&calls).await,
        CustodyRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes
        }
    );
    for advance in [20, 60] {
        if directory.lookups.load(Ordering::SeqCst) == 2 {
            break;
        }
        fixture.clock.advance(Duration::from_secs(advance)).unwrap();
        if advance == 20 {
            fixture
                .clock
                .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(80)))
                .await;
        }
    }
    let report = loop {
        match projection_event(&mut events).await {
            CustodyTaskEvent::Flush(report) => break report,
            CustodyTaskEvent::Receipt(_) => {}
            event => panic!("unexpected event during deferred projection: {event:?}"),
        }
    };
    assert_eq!(directory.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(report.sender_notices.len(), 1);
    assert_eq!(
        report.sender_notices[0].notice.inbox_message_id,
        fixture.notice.message_id
    );
    assert_eq!(fixture.status(&calls).await, fixture.projected_status());
    stop.send_replace(true);
    running.await.unwrap();
}
