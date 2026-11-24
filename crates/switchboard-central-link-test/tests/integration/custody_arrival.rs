use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RegisteredConnection, RequestStatusCommand, SubmitReceiptsCommand, WriteBatchCommand,
    WriteBatchDisposition,
};
use central_link_protocol::ReplyPayload;
use router_clock::{ManualClock, RuntimeClock};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use switchboard_central_link::link_lane::LinkLaneHandle;
use switchboard_machine_core_store::{InboxStore, MachineLocalOutbox, MachineLocalStore};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::custody_task,
    flush_clock::{FlushJitter, MachineFlushJitter, wake_for_mark},
    flush_marks::next_mark_after,
    outbox_work::OutboxWork,
    receipt_delivery::ReceiptFrameOutcome,
    recipient_inbox::RecipientInbox,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::{Mutex, Notify, mpsc, watch};
use workspace_ids::{MessageId, SessionId, SessionRef};
use workspace_model::{
    Caller, PostText, Timestamp,
    central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
        MessageDelivery, MessageStatus, NoticeCadence, SendMessageRequest,
    },
};

use super::{
    CustodyJourney, MACHINE_LINK_FIXTURE_MACHINE, RecordCustody, next_flush, next_receipt,
};

struct QueueObservedLane {
    driver: LinkLaneHandle,
    deliveries: Mutex<mpsc::Receiver<LinkLaneEvent>>,
}

impl ReadyLinkLane for QueueObservedLane {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>> {
        self.driver.watch_admitted()
    }
    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>> {
        self.driver.watch_registered()
    }
    fn submit_write_batch(
        &self,
        command: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>> {
        self.driver.submit_write_batch(command)
    }
    fn submit_receipts(
        &self,
        command: SubmitReceiptsCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedReceiptResults>> + Send + '_>>
    {
        self.driver.submit_receipts(command)
    }
    fn request_status(
        &self,
        command: RequestStatusCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedStatusReply>> + Send + '_>> {
        self.driver.request_status(command)
    }
    fn submit_immediate_request(
        &self,
        command: ImmediateRequestCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedRequestReply>> + Send + '_>>
    {
        self.driver.submit_immediate_request(command)
    }
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>> {
        Box::pin(async { self.deliveries.lock().await.recv().await })
    }
}

#[derive(Default)]
struct FirstLookupGate {
    started: AtomicBool,
    held: Notify,
    release: Notify,
}

impl RecipientSessionDirectory for FirstLookupGate {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(async move {
            if !self.started.swap(true, Ordering::SeqCst) {
                self.held.notify_one();
                self.release.notified().await;
            }
            RecipientSessionAnswer::KnownOnce
        })
    }
}

async fn next_driver_offer(driver: &LinkLaneHandle) -> (LinkLaneEvent, Timestamp, MessageId, u32) {
    loop {
        let event = driver.next_event().await.unwrap();
        if let LinkLaneEvent::Deliver {
            arrived_at, offer, ..
        } = &event
        {
            let accepted = *arrived_at;
            let message_id = match offer.message() {
                central_link_protocol::DeliveryMessage::Direct(record) => record.message_id.clone(),
                central_link_protocol::DeliveryMessage::System(_) => continue,
            };
            let time_left = u32::from(offer.time_left().unwrap());
            return (event, accepted, message_id, time_left);
        }
    }
}

#[tokio::test]
async fn a_real_socket_offer_keeps_its_arrival_while_queued_behind_a_held_lookup() {
    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
    let journey = CustodyJourney::start(clock.clone()).await;
    let mut ready = journey.lane.watch_admitted();
    let admitted = ready
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    let machine = MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap();
    let session = SessionRef {
        machine_id: machine,
        session_id: "arrival-fixture-session".parse().unwrap(),
    };
    let caller = Caller::Session(session.clone());
    let jitter = Arc::new(MachineFlushJitter::new(&session.machine_id));
    let database = journey.central.root_path().join("queued-arrival.sqlite");
    let outbox = MachineLocalOutbox::new(MachineLocalStore::open(&database).await.unwrap());
    let inbox_store = MachineLocalStore::open(&database).await.unwrap();
    let mut commits = inbox_store.subscribe_commits();
    let directory = Arc::new(FirstLookupGate::default());
    let (queued, deliveries) = mpsc::channel(2);
    let lane = Arc::new(QueueObservedLane {
        driver: journey.lane.clone(),
        deliveries: Mutex::new(deliveries),
    });
    let custody = CustodyLink::new(
        lane,
        clock.clone(),
        jitter.clone(),
        OutboxWork::new(Box::new(outbox), clock.clone()),
        RecipientInbox::new(
            InboxStore::new(inbox_store),
            session.machine_id.clone(),
            directory.clone(),
            clock.clone(),
        ),
    );
    let (event_sender, mut events) = mpsc::unbounded_channel();
    let (calls, group) = custody_task(custody, Arc::new(RecordCustody(event_sender)));
    let registration = calls
        .submit(
            caller.clone(),
            SubmissionPayload::Mail(CentralMailRequest {
                request_id: "019f0000-0000-7000-8000-000000000151".parse().unwrap(),
                operation: CentralMailOperation::SetNoticeCadence {
                    cadence: NoticeCadence::M15,
                },
            }),
        )
        .await;
    assert!(matches!(
        registration,
        Submission::Answered(ReplyPayload::Mail(CentralMailResult::Done(
            CentralMailAnswer::NoticeCadenceSet { .. }
        )))
    ));

    let mut mark = None;
    for (request, body) in [
        ("019f0000-0000-7000-8000-000000000152", "first lookup held"),
        (
            "019f0000-0000-7000-8000-000000000153",
            "second waits in the queue",
        ),
    ] {
        let submission = calls
            .submit(
                caller.clone(),
                SubmissionPayload::SendMessage {
                    request: request.parse().unwrap(),
                    message: SendMessageRequest {
                        to: session.clone(),
                        body: PostText::try_from(body.to_owned()).unwrap(),
                        delivery: MessageDelivery::Auto,
                        reply_to: None,
                    },
                },
            )
            .await;
        assert!(matches!(submission, Submission::InOutbox(_)));
        if let Submission::InOutbox(receipt) = submission {
            if let Some(first_mark) = mark {
                assert_eq!(receipt.flush_at, first_mark);
            }
            mark = Some(receipt.flush_at);
        }
    }
    let mark = mark.unwrap();
    let write_wake = wake_for_mark(admitted.clock_anchor, mark, jitter.jitter_millis(mark));
    clock.not_due_check_now(write_wake).await;
    clock
        .advance(write_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    assert_eq!(next_flush(&mut events).await.answered.len(), 2);

    let (first, _, _, _) = next_driver_offer(&journey.lane).await;
    queued.send(first).await.unwrap();
    directory.held.notified().await;
    let (second, accepted_at, message_id, time_left) = next_driver_offer(&journey.lane).await;
    assert_eq!(
        accepted_at,
        clock.utc_now(),
        "the real driver must stamp at acceptance"
    );
    queued.send(second).await.unwrap();
    let mut reader = InboxStore::new(MachineLocalStore::open(&database).await.unwrap());
    assert!(
        reader
            .receipt_for(&message_id)
            .await
            .unwrap()
            .healthy
            .is_none()
    );

    clock.advance(Duration::from_secs(15)).unwrap();
    directory.release.notify_one();
    drop(commits.wait_for(|count| *count >= 2).await.unwrap());
    let receipt = reader
        .receipt_for(&message_id)
        .await
        .unwrap()
        .healthy
        .unwrap();
    assert_eq!(
        receipt.received_at_ms,
        accepted_at.timestamp_millis(),
        "the storage arrival must be the socket stamp, not the later dequeue"
    );
    assert_eq!(
        reader
            .expiry_for(&message_id)
            .await
            .unwrap()
            .healthy
            .unwrap()
            .timestamp_millis(),
        accepted_at.timestamp_millis() + i64::from(time_left),
    );
    let receipt_mark = next_mark_after(mark);
    let receipt_wake = wake_for_mark(
        admitted.clock_anchor,
        receipt_mark,
        jitter.jitter_millis(receipt_mark),
    );
    clock
        .advance(receipt_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    assert!(matches!(
        next_receipt(&mut events).await,
        ReceiptFrameOutcome::Confirmed { .. }
    ));
    assert!(matches!(
        calls.message_status(caller, message_id).await.unwrap(),
        Some(MessageStatus::Delivered { arrived_at, .. }) if arrived_at.timestamp_millis() == accepted_at.timestamp_millis()
    ));
    group.request_stop();
    group.join().await.unwrap();
    journey.close().await;
}

#[tokio::test]
#[cfg(debug_assertions)]
async fn custody_stop_waits_for_an_in_progress_inbox_commit() {
    use central_link_client::ValidatedDeliveryOffer;
    use central_link_protocol::DeliveryMessage;
    use std::{future::poll_fn, pin::pin, task::Poll};
    use switchboard_machine_core_store::InboxDecision;
    use workspace_model::{
        ResolvedSender,
        central_mail::{MessageBody, MessageRecord},
    };

    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
    let journey = CustodyJourney::start(clock.clone()).await;
    let session = SessionRef {
        machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
        session_id: "held-inbox-commit".parse().unwrap(),
    };
    let database = journey.central.root_path().join("held-keep.sqlite");
    let outbox = MachineLocalOutbox::new(MachineLocalStore::open(&database).await.unwrap());
    let mut inbox = InboxStore::new(MachineLocalStore::open(&database).await.unwrap());
    let mut reader = InboxStore::new(MachineLocalStore::open(&database).await.unwrap());
    let mut commit = inbox.hold_next_commit();
    let directory = Arc::new(FirstLookupGate::default());
    let (queued, deliveries) = mpsc::channel(1);
    let custody = CustodyLink::new(
        Arc::new(QueueObservedLane {
            driver: journey.lane.clone(),
            deliveries: Mutex::new(deliveries),
        }),
        clock.clone(),
        Arc::new(MachineFlushJitter::new(&session.machine_id)),
        OutboxWork::new(Box::new(outbox), clock.clone()),
        RecipientInbox::new(
            inbox,
            session.machine_id.clone(),
            directory.clone(),
            clock.clone(),
        ),
    );
    let (events, _observations) = mpsc::unbounded_channel();
    let (_calls, group) = custody_task(custody, Arc::new(RecordCustody(events)));
    let message = "019f0000-0000-7000-8000-000000000155"
        .parse::<MessageId>()
        .unwrap();
    let record = MessageRecord::queued_central(
        message.clone(),
        session,
        MessageBody::Notice { items: Vec::new() },
        clock.utc_now(),
    );
    let admitted = journey.lane.watch_admitted().borrow().clone().unwrap();
    queued
        .send(LinkLaneEvent::Deliver {
            connection: admitted.connection,
            arrived_at: clock.utc_now(),
            offer: ValidatedDeliveryOffer::new(Box::new(DeliveryMessage::System(record)), None)
                .unwrap(),
            sender: ResolvedSender::Central {},
        })
        .await
        .unwrap();
    directory.held.notified().await;
    directory.release.notify_one();
    commit.until_held().await.unwrap();
    assert!(
        reader
            .message_for(&message)
            .await
            .unwrap()
            .healthy
            .is_none()
    );

    group.request_stop();
    let mut join = pin!(group.join());
    assert!(
        poll_fn(|context| Poll::Ready(join.as_mut().poll(context)))
            .await
            .is_pending()
    );
    commit.release();
    assert_eq!(join.await, Ok(()));

    assert_eq!(
        reader
            .message_for(&message)
            .await
            .unwrap()
            .healthy
            .unwrap()
            .message_id,
        message
    );
    assert_eq!(
        reader
            .receipt_for(&message)
            .await
            .unwrap()
            .healthy
            .unwrap()
            .decision,
        InboxDecision::Accepted
    );
    journey.close().await;
}
