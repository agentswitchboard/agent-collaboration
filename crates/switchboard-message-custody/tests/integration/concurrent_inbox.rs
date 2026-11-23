use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RegisteredConnection, RequestStatusCommand, SubmitReceiptsCommand, WriteBatchCommand,
    WriteBatchDisposition,
};
use central_link_protocol::{ReceiptResult, RequestScope, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, MachineLocalStore, OutboxCancellation,
    OutboxStorage,
};
use switchboard_message_custody::{
    custody_link::{CustodyLink, CustodyLinkObservation},
    custody_status::CustodyRequestStatus,
    custody_task::{CustodyEventSink, CustodyTaskEvent, custody_task},
    outbox_work::OutboxWork,
    receipt_delivery::ReceiptFrameOutcome,
    recipient_inbox::{InboxOfferResult, RecipientInbox},
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::{Mutex, Notify, mpsc, watch};
use workspace_ids::SessionId;
use workspace_model::{
    Caller, ResolvedSender, Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use super::{
    batch_replay::{NoFlushJitter, ScriptedBatchLane, new_admission},
    inbox_custody::{direct_offer, recipient_session},
    outbox_timing::{manual_clock, open_outbox, thread_post},
};

struct HoldStatusLane {
    peer: ScriptedBatchLane,
    pending_offers: Mutex<mpsc::Receiver<LinkLaneEvent>>,
    offer_gate: watch::Sender<bool>,
    status_gate: watch::Sender<bool>,
    status_entered: Notify,
    unexpected_status: Notify,
    status_calls: AtomicUsize,
}

impl ReadyLinkLane for HoldStatusLane {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>> {
        self.peer.watch_admitted()
    }
    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>> {
        self.peer.watch_registered()
    }

    fn submit_write_batch(
        &self,
        command: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>> {
        self.peer.submit_write_batch(command)
    }

    fn submit_receipts(
        &self,
        command: SubmitReceiptsCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedReceiptResults>> + Send + '_>>
    {
        self.peer.submit_receipts(command)
    }

    fn request_status(
        &self,
        command: RequestStatusCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedStatusReply>> + Send + '_>> {
        Box::pin(async move {
            if self.status_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.status_entered.notify_one();
            } else {
                self.unexpected_status.notify_one();
            }
            let mut release = self.status_gate.subscribe();
            let _ = release.wait_for(|released| *released).await;
            self.peer.request_status(command).await
        })
    }

    fn submit_immediate_request(
        &self,
        command: ImmediateRequestCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedRequestReply>> + Send + '_>>
    {
        self.peer.submit_immediate_request(command)
    }

    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>> {
        Box::pin(async {
            let mut release = self.offer_gate.subscribe();
            let _ = release.wait_for(|released| *released).await;
            self.pending_offers.lock().await.recv().await
        })
    }
}

struct RecordLookup(mpsc::UnboundedSender<SessionId>);

impl RecipientSessionDirectory for RecordLookup {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        let _ = self.0.send(session_id.clone());
        Box::pin(std::future::ready(RecipientSessionAnswer::KnownOnce))
    }
}

struct RecordEvents(mpsc::UnboundedSender<CustodyTaskEvent>);

impl CustodyEventSink for RecordEvents {
    fn record_event(&self, event: CustodyTaskEvent) {
        let _ = self.0.send(event);
    }
}

#[derive(Default)]
struct HeldDirectory {
    entered: Notify,
    cancelled: Notify,
}

struct LookupLifetime<'directory>(&'directory Notify);

impl Drop for LookupLifetime<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl RecipientSessionDirectory for HeldDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            let _lifetime = LookupLifetime(&self.cancelled);
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}

#[expect(
    clippy::panic,
    reason = "The fixture names the observation that made no progress."
)]
async fn custody_turns_to_complete<Output>(
    operation: impl Future<Output = Output>,
    observation: &str,
) -> Output {
    tokio::select! {
        biased;
        output = operation => output,
        () = async { for _ in 0..50_000 { tokio::task::yield_now().await; } } => {
            panic!("recipient lookup blocked {observation}");
        }
    }
}

#[derive(Default)]
struct RetryDirectory {
    calls: AtomicUsize,
}

impl RecipientSessionDirectory for RetryDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        let answer = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            RecipientSessionAnswer::CouldNotTell
        } else {
            RecipientSessionAnswer::KnownOnce
        };
        Box::pin(std::future::ready(answer))
    }
}

#[tokio::test]
async fn an_offline_intake_retry_does_not_delay_receipts_on_a_backwards_welcome() {
    const LOCAL_MARK_MS: i64 = 1_763_380_800_000;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let session = recipient_session();
    let clock = manual_clock(LOCAL_MARK_MS - 20_000);
    let admission = new_admission(session.machine_id.clone(), clock.clone());
    let lane = Arc::new(ScriptedBatchLane::new(
        admission.clone(),
        clock.clone(),
        Default::default(),
    ));
    lane.admitted.send_replace(None);

    let database = MachineLocalStore::open(&path).await.unwrap();
    let mut commits = database.subscribe_commits();
    let mut inbox_store = InboxStore::new(database);
    let owed = MessageRecord::queued_central(
        "019f0000-0000-7000-8000-000000000b31".parse().unwrap(),
        session.clone(),
        MessageBody::Notice { items: Vec::new() },
        Timestamp::from_timestamp_millis(LOCAL_MARK_MS - 20_000).unwrap(),
    );
    inbox_store
        .record_delivery(&owed, InboxDecision::Accepted, LOCAL_MARK_MS - 20_000, None)
        .await
        .unwrap();
    lane.push_event(LinkLaneEvent::Deliver {
        arrived_at: clock.utc_now(),
        connection: admission.connection,
        offer: direct_offer("019f0000-0000-7000-8000-000000000b32", "retry this locally"),
        sender: ResolvedSender::Central {},
    });
    let directory = Arc::new(RetryDirectory::default());
    let inbox = RecipientInbox::new(
        inbox_store,
        session.machine_id.clone(),
        directory.clone(),
        clock.clone(),
    );
    let outbox = OutboxWork::new(Box::new(open_outbox(&path).await), clock.clone());
    let custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        outbox,
        inbox,
    );
    let (stop, stopped) = watch::channel(false);
    let (events, mut observations) = mpsc::unbounded_channel();
    let (_calls, worker) = custody_task(custody, stopped, Arc::new(RecordEvents(events)));
    let running = tokio::spawn(worker);
    custody_turns_to_complete(
        clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20))),
        "the local intake retry",
    )
    .await;
    clock.advance(Duration::from_secs(20)).unwrap();
    drop(
        custody_turns_to_complete(
            commits.wait_for(|count| *count == 2),
            "the offline offer commit",
        )
        .await
        .unwrap(),
    );
    assert_eq!(directory.calls.load(Ordering::SeqCst), 2);
    assert!(observations.try_recv().is_err());

    let mut replacement = new_admission(session.machine_id.clone(), clock.clone());
    replacement.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(LOCAL_MARK_MS - 1_800_000).unwrap();
    lane.admitted.send_replace(Some(replacement));
    let outcome = custody_turns_to_complete(
        observations.recv(),
        "receipt progress on the current Central anchor",
    )
    .await;
    assert!(
        matches!(outcome, Some(CustodyTaskEvent::Receipt(ReceiptFrameOutcome::Confirmed { results }))
        if results == [ReceiptResult::Recorded, ReceiptResult::Recorded])
    );
    let mut reopened = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    assert!(
        reopened
            .pending_receipts()
            .await
            .unwrap()
            .healthy
            .is_empty()
    );
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_held_directory_does_not_block_status_deadlines_or_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let session = recipient_session();
    let start_ms = 1_763_380_780_000;
    let clock = manual_clock(start_ms);
    let admission = new_admission(session.machine_id.clone(), clock.clone());
    let lane = Arc::new(ScriptedBatchLane::new(
        admission.clone(),
        clock.clone(),
        Default::default(),
    ));
    lane.admitted.send_replace(None);
    let directory = Arc::new(HeldDirectory::default());
    let mut inbox_store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let owed = MessageRecord::queued_central(
        "019f0000-0000-7000-8000-000000000b24".parse().unwrap(),
        session.clone(),
        MessageBody::Notice { items: Vec::new() },
        Timestamp::from_timestamp_millis(start_ms).unwrap(),
    );
    inbox_store
        .record_delivery(
            &owed,
            InboxDecision::Refused {
                reason: InboxRefusalReason::UnknownSession,
            },
            start_ms,
            None,
        )
        .await
        .unwrap();
    let inbox = RecipientInbox::new(
        inbox_store,
        session.machine_id.clone(),
        directory.clone(),
        clock.clone(),
    );
    let mut store = open_outbox(&path).await;
    let item = thread_post(63);
    store.insert(&item, start_ms - 895_000).await.unwrap();
    let outbox = OutboxWork::new(Box::new(store), clock.clone());
    let custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        outbox,
        inbox,
    );
    let offer = LinkLaneEvent::Deliver {
        arrived_at: clock.utc_now(),
        connection: admission.connection,
        offer: direct_offer("019f0000-0000-7000-8000-000000000b23", "directory is held"),
        sender: ResolvedSender::Central {},
    };
    lane.push_event(offer);
    let (stop, stopped) = watch::channel(false);
    let (events, mut observations) = mpsc::unbounded_channel();
    let (calls, task) = custody_task(custody, stopped, Arc::new(RecordEvents(events)));
    let running = tokio::spawn(task);
    custody_turns_to_complete(directory.entered.notified(), "directory entry").await;
    assert!(
        custody_turns_to_complete(calls.waiting_for_session(session), "the waiting view")
            .await
            .unwrap()
            .is_empty()
    );
    let query = RequestStatusQuery {
        request_id: item.request.clone(),
        scope: RequestScope::Workspace("019f0000-0000-7000-8000-000000000021".parse().unwrap()),
    };
    assert!(matches!(
        custody_turns_to_complete(calls.request_status(item.caller, query), "sender status").await,
        CustodyRequestStatus::InOutbox { .. }
    ));
    let due = ClockInstant::ZERO.after(Duration::from_secs(5));
    custody_turns_to_complete(clock.not_due_check_now(due), "deadline registration").await;
    clock.advance(Duration::from_secs(5)).unwrap();
    let ending = custody_turns_to_complete(observations.recv(), "the original outbox deadline")
        .await
        .unwrap();
    assert!(matches!(ending, CustodyTaskEvent::Flush(report)
        if matches!(report.ended.as_slice(), [ended] if ended.request_id == item.request
            && ended.reason == OutboxCancellation::NotSentWithin15Minutes)));
    assert!(lane.batch_commands().is_empty());
    stop.send_replace(true);
    custody_turns_to_complete(running, "worker shutdown")
        .await
        .unwrap();
    custody_turns_to_complete(directory.cancelled.notified(), "lookup cancellation").await;
    let mut reopened = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    assert!(matches!(
        reopened.pending_receipts().await.unwrap().healthy.as_slice(),
        [receipt] if receipt.message_id == owed.message_id
    ));
}

#[tokio::test]
async fn an_unanswered_lookup_defers_at_its_injected_deadline() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let session = recipient_session();
    let clock = manual_clock(1_763_380_780_000);
    let directory = Arc::new(HeldDirectory::default());
    let admission = new_admission(session.machine_id.clone(), clock.clone());
    let lane = Arc::new(ScriptedBatchLane::new(
        admission.clone(),
        clock.clone(),
        Default::default(),
    ));
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        session.machine_id,
        directory.clone(),
        clock.clone(),
    );
    let work = OutboxWork::new(Box::new(open_outbox(&path).await), clock.clone());
    let mut custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        work,
        inbox,
    );
    lane.push_event(LinkLaneEvent::Deliver {
        arrived_at: clock.utc_now(),
        connection: admission.connection.clone(),
        offer: direct_offer("019f0000-0000-7000-8000-000000000b25", "unanswered lookup"),
        sender: ResolvedSender::Central {},
    });
    let observing = tokio::spawn(async move { custody.observe_next_event().await });
    custody_turns_to_complete(directory.entered.notified(), "directory entry").await;
    clock.advance(Duration::from_secs(29)).unwrap();
    assert!(!observing.is_finished());
    clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(
        custody_turns_to_complete(observing, "its injected timeout")
            .await
            .unwrap()
            .unwrap(),
        CustodyLinkObservation::Delivery {
            connection: admission.connection,
            result: InboxOfferResult::LookupDeferred
        }
    );
    custody_turns_to_complete(
        directory.cancelled.notified(),
        "timed-out lookup cancellation",
    )
    .await;
    let mut reopened = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    assert!(
        reopened
            .pending_receipts()
            .await
            .unwrap()
            .healthy
            .is_empty()
    );
}

#[tokio::test]
async fn a_full_offer_queue_is_saved_while_the_senders_status_reply_is_outstanding() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let session = recipient_session();
    let clock = manual_clock(1_763_380_780_000);
    let admission = new_admission(session.machine_id.clone(), clock.clone());
    let (offers, pending_offers) = mpsc::channel(2);
    let lane = Arc::new(HoldStatusLane {
        peer: ScriptedBatchLane::new(admission.clone(), clock.clone(), Default::default()),
        pending_offers: Mutex::new(pending_offers),
        offer_gate: watch::channel(false).0,
        status_gate: watch::channel(false).0,
        status_entered: Notify::new(),
        unexpected_status: Notify::new(),
        status_calls: AtomicUsize::new(0),
    });
    let (lookup, mut lookups) = mpsc::unbounded_channel();
    let outbox = OutboxWork::new(Box::new(open_outbox(&path).await), clock.clone());
    let database = MachineLocalStore::open(&path).await.unwrap();
    let mut commits = database.subscribe_commits();
    let inbox = RecipientInbox::new(
        InboxStore::new(database),
        session.machine_id.clone(),
        Arc::new(RecordLookup(lookup)),
        clock.clone(),
    );
    let custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        outbox,
        inbox,
    );
    let (stop, stopped) = watch::channel(false);
    let (events, _observations) = mpsc::unbounded_channel();
    let (calls, task) = custody_task(custody, stopped, Arc::new(RecordEvents(events)));
    let running = tokio::spawn(task);
    let status = tokio::spawn({
        let calls = calls.clone();
        let caller = Caller::Session(session.clone());
        async move {
            calls
                .request_status(
                    caller,
                    RequestStatusQuery {
                        request_id: "019f0000-0000-7000-8000-000000000a21".parse().unwrap(),
                        scope: RequestScope::Organization,
                    },
                )
                .await
        }
    });
    lane.status_entered.notified().await;

    let ids = [
        "019f0000-0000-7000-8000-000000000b21",
        "019f0000-0000-7000-8000-000000000b22",
    ];
    for id in ids {
        offers
            .try_send(LinkLaneEvent::Deliver {
                arrived_at: clock.utc_now(),
                connection: admission.connection.clone(),
                offer: direct_offer(id, "held during status"),
                sender: ResolvedSender::Central {},
            })
            .unwrap();
    }
    assert_eq!(
        offers.capacity(),
        0,
        "fixture fills the queue before the offer taker starts"
    );
    lane.offer_gate.send_replace(true);
    for _ in ids {
        tokio::select! {
            biased;
            () = lane.unexpected_status.notified() => panic!("the offer consumer awaited a status reply instead of saving an offer"),
            lookup = lookups.recv() => assert_eq!(lookup, Some(session.session_id.clone())),
        }
    }

    drop(
        custody_turns_to_complete(
            commits.wait_for(|count| *count == 2),
            "the two durable offers",
        )
        .await
        .unwrap(),
    );
    let saved = calls.waiting_for_session(session).await.unwrap();
    assert_eq!(
        saved
            .iter()
            .map(|record| record.message_id.as_str())
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(lane.status_calls.load(Ordering::SeqCst), 1);
    assert!(
        !status.is_finished(),
        "the original request is still awaiting a lane answer"
    );
    lane.status_gate.send_replace(true);
    assert_eq!(status.await.unwrap(), CustodyRequestStatus::NotFound);
    stop.send_replace(true);
    running.await.unwrap();
}
