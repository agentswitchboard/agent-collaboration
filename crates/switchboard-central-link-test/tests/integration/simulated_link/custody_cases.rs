#![expect(
    clippy::panic,
    reason = "The test reports the unexpected protocol or custody result."
)]

use std::{collections::VecDeque, sync::Arc, time::Duration};

#[path = "loss_cases.rs"]
mod loss_cases;

#[path = "recovery_cases.rs"]
mod recovery_cases;

use central_link_protocol::{
    DataFrame, LinkFrame, ReceiptKind, ReceiptResult, ReplyPayload, RequestScope,
    RequestStatusQuery,
};
use collaboration_central_tokio::test_support::MACHINE_LINK_FIXTURE_MACHINE;
use router_clock::{ClockInstant, RuntimeClock};
use switchboard_machine_core_store::{InboxStore, MachineLocalOutbox, MachineLocalStore};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{OutboxReceipt, Submission, SubmissionPayload},
    custody_task::{CustodyTaskEvent, CustodyTaskHandle, custody_task},
    flush_clock::{FlushJitter, MachineFlushJitter, shared_time_at, wake_for_mark},
    flush_marks::{mark_in_window, next_mark_after},
    outbox_dispatch::FlushReport,
    outbox_work::OutboxWork,
    receipt_delivery::ReceiptFrameOutcome,
    recipient_inbox::RecipientInbox,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use workspace_ids::{MessageId, RequestId, SessionRef};
use workspace_model::{
    Caller, PostText,
    central_mail::{
        CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
        MessageDelivery, MessageStatus, NoticeCadence, SendMessageRequest,
    },
};

use super::{
    SimulatedLink,
    packet_flight::{FrameKind, InFlightFrame},
    run_link_case,
};
use crate::custody_journey::{KnownDeliverySession, RecordCustody, next_receipt};

struct NetworkCustody {
    link: SimulatedLink,
    session: SessionRef,
    calls: CustodyTaskHandle,
    events: mpsc::UnboundedReceiver<CustodyTaskEvent>,
    receipt_outcomes: VecDeque<ReceiptFrameOutcome>,
    stop: watch::Sender<bool>,
    worker: JoinHandle<()>,
    jitter: Arc<MachineFlushJitter>,
    inbox: InboxStore,
}

impl NetworkCustody {
    async fn start(link: SimulatedLink) -> Self {
        let machine_id = MACHINE_LINK_FIXTURE_MACHINE.parse().unwrap();
        let session = SessionRef {
            machine_id,
            session_id: "simulated-custody-session".parse().unwrap(),
        };
        let db_path = link.central.root_path().join("simulated-custody.sqlite");
        let opened = MachineLocalStore::open(&db_path).await.unwrap();
        let inbox = InboxStore::new(MachineLocalStore::open(&db_path).await.unwrap());
        let jitter = Arc::new(MachineFlushJitter::new(&session.machine_id));
        let custody = CustodyLink::new(
            Arc::new(link.lane.clone()),
            link.clock.clone(),
            jitter.clone(),
            OutboxWork::new(
                Box::new(MachineLocalOutbox::new(opened)),
                link.clock.clone(),
            ),
            RecipientInbox::new(
                InboxStore::new(MachineLocalStore::open(&db_path).await.unwrap()),
                session.machine_id.clone(),
                Arc::new(KnownDeliverySession {
                    session_id: session.session_id.clone(),
                }),
                link.clock.clone(),
            ),
        );
        let (stop, stopped) = watch::channel(false);
        let (sender, events) = mpsc::unbounded_channel();
        let (calls, worker) = custody_task(custody, stopped, Arc::new(RecordCustody(sender)));
        let worker = tokio::spawn(worker);
        let case = Self {
            link,
            session,
            calls,
            events,
            receipt_outcomes: VecDeque::new(),
            stop,
            worker,
            jitter,
            inbox,
        };
        let answer = case
            .calls
            .submit(
                case.caller(),
                SubmissionPayload::Mail(CentralMailRequest {
                    request_id: request_id(0),
                    operation: CentralMailOperation::SetNoticeCadence {
                        cadence: NoticeCadence::M15,
                    },
                }),
            )
            .await;
        assert!(
            matches!(
                answer,
                Submission::Answered(ReplyPayload::Mail(CentralMailResult::Done(
                    CentralMailAnswer::NoticeCadenceSet { .. }
                )))
            ),
            "{answer:?}"
        );
        case
    }

    fn caller(&self) -> Caller {
        Caller::Session(self.session.clone())
    }

    async fn enqueue(&self, request: u8) -> OutboxReceipt {
        let submission = self
            .calls
            .submit(
                self.caller(),
                SubmissionPayload::SendMessage {
                    request: request_id(request),
                    message: SendMessageRequest {
                        to: self.session.clone(),
                        body: PostText::try_from(format!("simulated custody {request}")).unwrap(),
                        delivery: MessageDelivery::Auto,
                        reply_to: None,
                    },
                },
            )
            .await;
        let Submission::InOutbox(receipt) = submission else {
            panic!("expected a durable outbox receipt, got {submission:?}");
        };
        receipt
    }

    async fn status(&self, request_id: RequestId) -> CustodyRequestStatus {
        self.calls
            .request_status(
                self.caller(),
                RequestStatusQuery {
                    request_id,
                    scope: RequestScope::Organization,
                },
            )
            .await
    }

    async fn wait_for_replay(&self, request: &RequestId) {
        for _ in 0..2_000 {
            match self.status(request.clone()).await {
                CustodyRequestStatus::Sent => self.link.network.step().await,
                CustodyRequestStatus::InOutbox { .. } => return,
                status => panic!("expected an unanswered write back in the outbox, got {status:?}"),
            }
        }
        panic!("unanswered request {request} was never returned to its outbox");
    }

    fn advance_to(&self, at: ClockInstant) {
        let now = self.link.clock.monotonic_now();
        assert!(at >= now, "the injected clock advances monotonically");
        self.link
            .clock
            .advance(at.saturating_duration_since(now))
            .unwrap();
    }

    async fn first_flush(&self, receipt: &OutboxReceipt) -> ClockInstant {
        let wake = wake_for_mark(
            self.link.admission.clock_anchor,
            receipt.flush_at,
            self.jitter.jitter_millis(receipt.flush_at),
        );
        self.link.clock.not_due_check_now(wake).await;
        assert!(
            self.link
                .network
                .frames()
                .iter()
                .all(|frame| frame.kind != FrameKind::Batch),
            "Mac must not start a write batch before the selected minute window"
        );
        self.advance_to(wake);
        wake
    }

    async fn finish_write(&mut self, receipt: &OutboxReceipt) -> MessageId {
        let flush = self.next_write_report().await;
        assert!(flush.issues.is_empty(), "{:?}", flush.issues);
        let answer = flush
            .answered
            .iter()
            .find(|answer| answer.request == receipt.id)
            .unwrap();
        let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
            &answer.reply
        else {
            panic!(
                "expected a successful canonical direct send, got {:?}",
                answer.reply
            );
        };
        assert_eq!(
            self.status(receipt.id.clone()).await,
            CustodyRequestStatus::Answered(answer.reply.clone()),
        );
        message_id.clone()
    }

    async fn next_write_report(&mut self) -> FlushReport {
        loop {
            match self.events.recv().await.unwrap() {
                CustodyTaskEvent::Flush(report) => return report,
                // Replays and receipts can share a mark and finish in either order.
                CustodyTaskEvent::Receipt(outcome) => self.receipt_outcomes.push_back(outcome),
                failure @ (CustodyTaskEvent::Operator(_) | CustodyTaskEvent::Failed(_)) => {
                    panic!("custody failed before reporting the write: {failure:?}");
                }
            }
        }
    }

    async fn wait_for_kept(&mut self, message: &MessageId) {
        // The recipient worker publishes no application receipt event at intake.
        // Query the real inbox until its commit becomes observable.
        loop {
            if self
                .inbox
                .receipt_for(message)
                .await
                .unwrap()
                .healthy
                .is_some()
            {
                break;
            }
        }
        let waiting = self
            .calls
            .waiting_for_session(self.session.clone())
            .await
            .unwrap();
        assert_eq!(
            waiting
                .iter()
                .filter(|record| &record.message_id == message)
                .count(),
            1,
            "the Mac must keep one inbox record per delivered message"
        );
    }

    fn wake_at_next_mark(&self) -> ClockInstant {
        let shared_time = shared_time_at(
            self.link.admission.clock_anchor,
            self.link.clock.monotonic_now(),
        );
        let mark = next_mark_after(mark_in_window(shared_time).unwrap_or(shared_time));
        wake_for_mark(
            self.link.admission.clock_anchor,
            mark,
            self.jitter.jitter_millis(mark),
        )
    }

    async fn advance_to_next_mark(&self) {
        let wake = self.wake_at_next_mark();
        self.link.clock.not_due_check_now(wake).await;
        self.advance_to(wake);
    }

    async fn confirm_receipt(&mut self, message: &MessageId) {
        let after = self.link.network.frames().len();
        assert!(
            self.inbox
                .receipt_for(message)
                .await
                .unwrap()
                .healthy
                .is_some()
        );
        self.advance_to_next_mark().await;
        let frame = self.link.network.seen(after, FrameKind::Receipt).await;
        let LinkFrame::Data(DataFrame::Receipts { receipts, .. }) = frame.frame else {
            panic!("expected receipt frame");
        };
        assert!(
            receipts
                .receipts()
                .iter()
                .any(|receipt| &receipt.message_id == message
                    && matches!(receipt.kind, ReceiptKind::Delivered { .. }))
        );
        self.wait_for_confirmation(&[ReceiptResult::Recorded]).await;
        assert!(matches!(
            self.central_status(message).await,
            MessageStatus::Delivered { .. }
        ));
    }

    async fn wait_for_confirmation(&mut self, expected: &[ReceiptResult]) {
        loop {
            match self.next_receipt_outcome().await {
                ReceiptFrameOutcome::Confirmed { results } => {
                    assert_eq!(results, expected);
                    return;
                }
                ReceiptFrameOutcome::RetryPending | ReceiptFrameOutcome::StaleCompletion => {}
            }
        }
    }

    async fn next_receipt_outcome(&mut self) -> ReceiptFrameOutcome {
        if let Some(outcome) = self.receipt_outcomes.pop_front() {
            outcome
        } else {
            next_receipt(&mut self.events).await
        }
    }

    async fn central_status(&self, message: &MessageId) -> MessageStatus {
        // The current minute can put a receipt or heartbeat ahead of reads
        // on the single command lane. Retry on a simulation step, at fixed
        // injected time, until the status query itself gets a turn.
        loop {
            match self
                .calls
                .message_status(self.caller(), message.clone())
                .await
            {
                Ok(Some(status)) => return status,
                Ok(None) => panic!("Central cannot find its queued message {message}"),
                Err(_) => self.link.network.step().await,
            }
        }
    }

    async fn central_queued_messages(&self) -> Vec<MessageId> {
        let runtime = self
            .link
            .central
            .host
            .central(&self.link.central.organization_id)
            .await
            .unwrap();
        runtime
            .queued_for_session(&self.session)
            .await
            .unwrap()
            .messages
            .into_iter()
            .map(|message| message.message_id)
            .collect()
    }

    fn assert_minute_writes(&self) {
        for InFlightFrame { kind, utc_ms, .. } in self.link.network.frames() {
            if matches!(kind, FrameKind::Batch | FrameKind::Receipt) {
                let offset_ms = (utc_ms + 3_000).rem_euclid(60_000) - 3_000;
                assert!(
                    (-3_000..3_000).contains(&offset_ms),
                    "{kind:?} left outside the minute window at offset {offset_ms}ms"
                );
            }
        }
    }

    async fn close(self) {
        self.assert_minute_writes();
        self.stop.send_replace(true);
        self.worker.await.unwrap();
        self.link.close().await;
    }
}

fn request_id(number: u8) -> RequestId {
    format!("019f0000-0000-7000-8000-0000000000{number:02x}")
        .parse()
        .unwrap()
}

#[test]
fn a_write_batch_can_arrive_after_its_minute_mark() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::Batch);

        let wake = case.first_flush(&receipt).await;
        let sent = case.link.network.seen(after, FrameKind::Batch).await;
        assert_eq!(sent.sent_at, wake);
        assert!(
            sent.sent_at < case.link.clock.wall_deadline(receipt.flush_at),
            "hold a frame which started before the minute mark itself"
        );
        case.advance_to(wake + Duration::from_secs(7));
        assert!(
            mark_in_window(shared_time_at(
                case.link.admission.clock_anchor,
                case.link.clock.monotonic_now(),
            ))
            .is_none(),
            "the minute window closes before Central receives the batch"
        );
        assert_eq!(
            case.status(receipt.id.clone()).await,
            CustodyRequestStatus::Sent
        );
        assert!(case.central_queued_messages().await.is_empty());
        assert!(
            case.link
                .network
                .frames()
                .iter()
                .all(|frame| frame.kind != FrameKind::WriteAnswer)
        );

        case.link.network.release();
        let message = case.finish_write(&receipt).await;
        case.wait_for_kept(&message).await;
        case.confirm_receipt(&message).await;
        case.close().await;
    });
}

fn queued_message(frame: &InFlightFrame) -> MessageId {
    let LinkFrame::Data(DataFrame::Reply { result, .. }) = &frame.frame else {
        panic!("expected a write answer");
    };
    let ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) =
        result.as_ref()
    else {
        panic!("expected a queued direct send, got {result:?}");
    };
    message_id.clone()
}

fn offered_message(frame: &InFlightFrame) -> &MessageId {
    let LinkFrame::Data(DataFrame::Deliver { message, .. }) = &frame.frame else {
        panic!("expected a delivery offer");
    };
    message.message_id()
}

fn offers_for(case: &NetworkCustody, message: &MessageId) -> Vec<InFlightFrame> {
    case.link
        .network
        .frames()
        .into_iter()
        .filter(|frame| frame.kind == FrameKind::Offer && offered_message(frame) == message)
        .collect()
}

fn assert_one_offer_per_socket(case: &NetworkCustody, message: &MessageId) {
    let offers = offers_for(case, message);
    let connections: std::collections::BTreeSet<_> =
        offers.iter().map(|frame| frame.connection_port).collect();
    assert_eq!(
        connections.len(),
        offers.len(),
        "Central reoffered a message on the same link"
    );
}

fn assert_replayed_member(first: &InFlightFrame, replay: &InFlightFrame, id: &RequestId) {
    use central_link_protocol::{LinkRequest, LinkRequestBatch};
    fn member<'batch>(requests: &'batch LinkRequestBatch, id: &RequestId) -> &'batch LinkRequest {
        requests
            .requests()
            .iter()
            .find(|request| request.payload().write_request_id() == Some(id))
            .unwrap()
    }
    let LinkFrame::Data(DataFrame::Batch { requests: first }) = &first.frame else {
        panic!("expected the original write batch");
    };
    let LinkFrame::Data(DataFrame::Batch { requests: replay }) = &replay.frame else {
        panic!("expected a replayed write batch");
    };
    assert_eq!(member(first, id).payload(), member(replay, id).payload());
}

fn assert_kept_receipt(frame: &InFlightFrame, message: &MessageId, saved_at: i64) {
    let LinkFrame::Data(DataFrame::Receipts { receipts, .. }) = &frame.frame else {
        panic!("expected a receipt");
    };
    assert!(
        matches!(receipts.receipts(), [receipt] if &receipt.message_id == message
        && matches!(receipt.kind, ReceiptKind::Delivered { arrived_at, .. }
            if arrived_at.timestamp_millis() == saved_at))
    );
}
