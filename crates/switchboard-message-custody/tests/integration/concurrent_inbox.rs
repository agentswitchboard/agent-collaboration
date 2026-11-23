use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RegisteredConnection, RequestStatusCommand, SubmitReceiptsCommand, WriteBatchCommand,
    WriteBatchDisposition,
};
use central_link_protocol::{RequestScope, RequestStatusQuery};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use switchboard_machine_core_store::{InboxStore, MachineLocalStore};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_task::{CustodyEventSink, CustodyTaskEvent, custody_task},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::{Mutex, Notify, mpsc, watch};
use workspace_ids::SessionId;
use workspace_model::{Caller, ResolvedSender};

use super::{
    batch_replay::{NoFlushJitter, ScriptedBatchLane, new_admission},
    inbox_custody::{direct_offer, recipient_session},
    outbox_timing::{manual_clock, open_outbox},
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

#[tokio::test]
async fn a_full_offer_queue_is_saved_while_the_senders_status_reply_is_outstanding() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let session = recipient_session();
    let clock = manual_clock(1_763_380_780_000);
    let admission = new_admission(session.machine_id.clone(), clock.clone());
    let (offers, pending_offers) = mpsc::channel(2);
    let lane = Arc::new(HoldStatusLane {
        peer: ScriptedBatchLane::new(admission.clone(), Default::default()),
        pending_offers: Mutex::new(pending_offers),
        offer_gate: watch::channel(false).0,
        status_gate: watch::channel(false).0,
        status_entered: Notify::new(),
        unexpected_status: Notify::new(),
        status_calls: AtomicUsize::new(0),
    });
    let (lookup, mut lookups) = mpsc::unbounded_channel();
    let outbox = OutboxWork::new(Box::new(open_outbox(&path).await), clock.clone());
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        session.machine_id.clone(),
        Arc::new(RecordLookup(lookup)),
        clock.clone(),
    );
    let custody = CustodyLink::new(lane.clone(), clock, Arc::new(NoFlushJitter), outbox, inbox);
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
