#![expect(
    clippy::panic,
    reason = "The fixture reports the exact task failure that blocks the next observation."
)]

use super::RealLeases;
use central_link_client::ReadyLinkLane;
use central_link_protocol::{ReceiptResult, ReplyPayload, RequestScope, RequestStatusQuery};
use collaboration_central_tokio::test_support::{
    MACHINE_LINK_FIXTURE_MACHINE, MachineLinkFixture, start_machine_link_fixture,
};
use router_clock::{ManualClock, RuntimeClock};
use std::{future::Future, pin::Pin, sync::Arc};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
    link_lane::{LinkLaneHandle, link_lane_channel},
    link_policy::LinkStatus,
};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalOutbox, MachineLocalStore, OutboxCentralRefusal, OutboxRefusal,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::{CustodyEventSink, CustodyTaskEvent, custody_task},
    flush_clock::{FlushJitter, MachineFlushJitter, wake_for_mark},
    flush_marks::next_mark_after,
    outbox_dispatch::FlushReport,
    outbox_work::OutboxWork,
    receipt_delivery::ReceiptFrameOutcome,
    recipient_inbox::RecipientInbox,
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use workspace_ids::{MachineId, RequestId, SessionId, SessionRef};
use workspace_model::{
    Caller, PostText,
    central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRefusal, CentralMailRequest,
        CentralMailResult, MessageBody, MessageDelivery, MessageStatus, NoticeCadence,
        SendMessageRequest,
    },
};

#[path = "custody_arrival.rs"]
mod custody_arrival;

struct CustodyJourney {
    central: Arc<MachineLinkFixture>,
    shutdown: watch::Sender<bool>,
    _settings: watch::Sender<DriverSettings<u8>>,
    lane: LinkLaneHandle,
    driver: JoinHandle<()>,
}

impl CustodyJourney {
    async fn start(clock: Arc<ManualClock>) -> Self {
        let central = Arc::new(start_machine_link_fixture(clock.clone()).await.unwrap());
        let address = format!(
            "ws://{}/v1/organizations/{}/link",
            central.origin().strip_prefix("http://").unwrap(),
            central.organization_id.as_str()
        );
        let (settings, settings_rx) = watch::channel(DriverSettings {
            binding: Some(1),
            address: DialAddress::test_loopback(&address),
        });
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (status_tx, mut status) = watch::channel(LinkStatus::NotEnrolled);
        let (inbox, lane) = link_lane_channel();
        let (acquired_tx, mut acquisitions) = mpsc::unbounded_channel();
        let (ended_tx, _endings) = mpsc::unbounded_channel();
        let driver = tokio::spawn(
            LinkDriver {
                machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap(),
                clock: clock.clone(),
                lease_source: Arc::new(RealLeases {
                    central: central.clone(),
                    mac_clock: clock,
                    acquisitions: acquired_tx,
                    endings: ended_tx,
                }),
                settings: settings_rx,
                shutdown: shutdown_rx,
                status: status_tx,
                lane,
            }
            .run(),
        );
        assert_eq!(acquisitions.recv().await, Some(None));
        let actual = status
            .wait_for(|status| {
                matches!(
                    status,
                    LinkStatus::Welcomed { .. }
                        | LinkStatus::Fault { .. }
                        | LinkStatus::EnrollmentEnded { .. }
                )
            })
            .await
            .unwrap()
            .clone();
        assert_eq!(
            actual,
            LinkStatus::Welcomed {
                machine_id: MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap()
            }
        );
        Self {
            central,
            shutdown,
            _settings: settings,
            lane: inbox,
            driver,
        }
    }

    async fn close(self) {
        self.shutdown.send_replace(true);
        self.driver.await.unwrap();
        let central = Arc::into_inner(self.central);
        assert!(
            central.is_some(),
            "the driver must release its fixture lease source at shutdown"
        );
        central.unwrap().close().await.unwrap();
    }
}

struct KnownDeliverySession {
    session_id: SessionId,
}

impl RecipientSessionDirectory for KnownDeliverySession {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        session_id: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        let answer = if session_id == &self.session_id {
            RecipientSessionAnswer::KnownOnce
        } else {
            RecipientSessionAnswer::UnknownSession
        };
        Box::pin(std::future::ready(answer))
    }
}

struct RecordCustody(mpsc::UnboundedSender<CustodyTaskEvent>);

impl CustodyEventSink for RecordCustody {
    fn record_event(&self, event: CustodyTaskEvent) {
        let _ = self.0.send(event);
    }
}

async fn next_flush(events: &mut mpsc::UnboundedReceiver<CustodyTaskEvent>) -> FlushReport {
    loop {
        match events.recv().await.unwrap() {
            CustodyTaskEvent::Flush(report) => return report,
            CustodyTaskEvent::Receipt(_) => {}
            failure @ (CustodyTaskEvent::Operator(_) | CustodyTaskEvent::Failed(_)) => {
                panic!("custody failed before observing a real batch: {failure:?}");
            }
        }
    }
}

async fn next_receipt(
    events: &mut mpsc::UnboundedReceiver<CustodyTaskEvent>,
) -> ReceiptFrameOutcome {
    loop {
        match events.recv().await.unwrap() {
            CustodyTaskEvent::Receipt(receipt) => return receipt,
            CustodyTaskEvent::Flush(report) if report.issues.is_empty() => {}
            failure => panic!("custody failed before observing a real receipt: {failure:?}"),
        }
    }
}

#[tokio::test]
async fn real_central_accepts_and_receipts_same_mac_delivery_and_refuses_an_unknown_machine() {
    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
    let journey = CustodyJourney::start(clock.clone()).await;
    let mut ready = journey.lane.watch_admitted();
    let admitted = ready
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    let machine: MachineId = MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap();
    let session = SessionRef {
        machine_id: machine.clone(),
        session_id: "real-custody-session".parse().unwrap(),
    };
    let caller = Caller::Session(session.clone());
    let directory = Arc::new(KnownDeliverySession {
        session_id: session.session_id.clone(),
    });
    let jitter = Arc::new(MachineFlushJitter::new(&machine));
    let database = journey.central.root_path().join("mac-custody.sqlite");
    let outbox = MachineLocalOutbox::new(MachineLocalStore::open(&database).await.unwrap());
    let inbox_store = MachineLocalStore::open(&database).await.unwrap();
    let mut inbox_commits = inbox_store.subscribe_commits();
    let inbox = InboxStore::new(inbox_store);
    let custody = CustodyLink::new(
        Arc::new(journey.lane.clone()),
        clock.clone(),
        jitter.clone(),
        OutboxWork::new(Box::new(outbox), clock.clone()),
        RecipientInbox::new(inbox, machine, directory, clock.clone()),
    );
    let (stop, stopped) = watch::channel(false);
    let (event_sender, mut events) = mpsc::unbounded_channel();
    let (calls, task) = custody_task(custody, stopped, Arc::new(RecordCustody(event_sender)));
    let task = tokio::spawn(task);

    let register = calls
        .submit(
            caller.clone(),
            SubmissionPayload::Mail(CentralMailRequest {
                request_id: "019f0000-0000-7000-8000-000000000051".parse().unwrap(),
                operation: CentralMailOperation::SetNoticeCadence {
                    cadence: NoticeCadence::M15,
                },
            }),
        )
        .await;
    assert!(matches!(
        register,
        Submission::Answered(ReplyPayload::Mail(CentralMailResult::Done(
            CentralMailAnswer::NoticeCadenceSet { .. }
        )))
    ));
    let request: RequestId = "019f0000-0000-7000-8000-000000000052".parse().unwrap();
    let message = SendMessageRequest {
        to: session.clone(),
        body: PostText::try_from("same-Mac real custody".to_owned()).unwrap(),
        delivery: MessageDelivery::Auto,
        reply_to: None,
    };
    let Submission::InOutbox(receipt) = calls
        .submit(
            caller.clone(),
            SubmissionPayload::SendMessage {
                request: request.clone(),
                message: message.clone(),
            },
        )
        .await
    else {
        panic!("same-Mac sends must return an outbox receipt before Central answers");
    };
    assert_eq!(receipt.id, request);
    assert!(
        calls
            .waiting_for_session(session.clone())
            .await
            .unwrap()
            .is_empty(),
        "the message cannot reach the local inbox before a delivery offer"
    );
    let query = RequestStatusQuery {
        request_id: request.clone(),
        scope: RequestScope::Organization,
    };
    assert_eq!(
        calls.request_status(caller.clone(), query.clone()).await,
        CustodyRequestStatus::InOutbox {
            flush_at: receipt.flush_at
        }
    );

    let first_wake = wake_for_mark(
        admitted.clock_anchor,
        receipt.flush_at,
        jitter.jitter_millis(receipt.flush_at),
    );
    clock.not_due_check_now(first_wake).await;
    assert!(events.try_recv().is_err());
    clock
        .advance(first_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    let flush = next_flush(&mut events).await;
    assert!(flush.issues.is_empty(), "{:?}", flush.issues);
    let [answer] = flush.answered.as_slice() else {
        panic!("expect one answered Central write");
    };
    assert_eq!(answer.request, request);
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        &answer.reply
    else {
        panic!(
            "Central must accept the same-Mac message: {:?}",
            answer.reply
        );
    };

    drop(inbox_commits.wait_for(|count| *count > 0).await.unwrap());
    let waiting = calls.waiting_for_session(session.clone()).await.unwrap();
    assert!(
        matches!(waiting.as_slice(), [record] if &record.message_id == message_id
        && matches!(&record.body, MessageBody::Direct { body, .. } if body == &message.body))
    );
    let following = next_mark_after(receipt.flush_at);
    let next_wake = wake_for_mark(
        admitted.clock_anchor,
        following,
        jitter.jitter_millis(following),
    );
    clock
        .advance(next_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    assert_eq!(
        next_receipt(&mut events).await,
        ReceiptFrameOutcome::Confirmed {
            results: vec![ReceiptResult::Recorded]
        }
    );
    assert!(
        matches!(calls.message_status(caller.clone(), message_id.clone()).await.unwrap(),
        Some(MessageStatus::Delivered { arrived_at, .. }) if arrived_at.timestamp_millis() == waiting[0].created_at.timestamp_millis()),
        "Central must report the durably kept same-Mac message as delivered"
    );
    assert_eq!(
        calls.request_status(caller.clone(), query).await,
        CustodyRequestStatus::Answered(answer.reply.clone())
    );

    let missing = SessionRef {
        machine_id: "019f0000-0000-7000-8000-000000000099".parse().unwrap(),
        session_id: session.session_id.clone(),
    };
    let refused_id: RequestId = "019f0000-0000-7000-8000-000000000053".parse().unwrap();
    let declined = calls
        .submit(
            caller.clone(),
            SubmissionPayload::SendMessage {
                request: refused_id.clone(),
                message: SendMessageRequest {
                    to: missing.clone(),
                    ..message
                },
            },
        )
        .await;
    let Submission::InOutbox(refused_receipt) = declined else {
        panic!("a send refusal must come back from Central after the outbox accepts the request");
    };
    let refusal_wake = wake_for_mark(
        admitted.clock_anchor,
        refused_receipt.flush_at,
        jitter.jitter_millis(refused_receipt.flush_at),
    );
    clock
        .advance(refusal_wake.saturating_duration_since(clock.monotonic_now()))
        .unwrap();
    let refused = next_flush(&mut events).await;
    let expected = CentralMailRefusal::UnknownSession {
        target: missing.clone(),
    };
    assert!(
        matches!(refused.answered.as_slice(), [answer] if answer.request == refused_id
        && answer.reply == ReplyPayload::Mail(CentralMailResult::Refused(expected.clone())))
    );
    assert_eq!(
        calls
            .request_status(
                caller,
                RequestStatusQuery {
                    request_id: refused_id,
                    scope: RequestScope::Organization
                }
            )
            .await,
        CustodyRequestStatus::Refused(OutboxRefusal::Central(OutboxCentralRefusal::Mail(expected)))
    );
    assert!(calls.waiting_for_session(missing).await.unwrap().is_empty());
    assert_eq!(calls.waiting_for_session(session).await.unwrap(), waiting);

    stop.send_replace(true);
    task.await.unwrap();
    journey.close().await;
}
