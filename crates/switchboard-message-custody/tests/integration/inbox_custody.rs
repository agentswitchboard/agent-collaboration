#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "Integration fixtures fail at the unexpected peer command or fixture input."
)]

use central_link_client::{
    ClientAction, ConnectionAttempt, CorrelatedReceiptResults, LinkClient, LinkLaneDisposition,
    LinkLaneEvent, SubmitReceiptsCommand, UntouchedCommand, ValidatedDeliveryOffer,
};
use central_link_protocol::{
    CorrelationId, DataFrame, DeliveryMessage, DirectOfferBody, DirectOfferMessage, FrameDirection,
    LinkFrame, Receipt, ReceiptKind, ReceiptResult, TimeLeft, decode_text, encode_text,
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{
    future::Future,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxDecision, InboxRefusalReason, InboxStore, MachineLocalStore,
};
use switchboard_message_custody::{
    custody_task::{CustodyTaskEvent, custody_task},
    receipt_delivery::{ReceiptFrame, ReceiptFrameOutcome},
    recipient_inbox::{InboxOfferResult, RecipientInbox},
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use tokio::sync::{mpsc, watch};
use workspace_ids::{MachineId, SessionId, SessionRef};
use workspace_model::central_mail::{MessageDelivery, MessageRefusal, MessageSender, MessageState};
use workspace_model::{PostText, ResolvedSender};

use super::custody_task::ObserveTask;

const START_MS: i64 = 1_763_380_800_000;
const FIRST_MESSAGE: &str = "019f0000-0000-7000-8000-000000000091";
const SECOND_MESSAGE: &str = "019f0000-0000-7000-8000-000000000092";
const THIRD_MESSAGE: &str = "019f0000-0000-7000-8000-000000000093";

#[tokio::test]
async fn a_queued_admitted_offer_is_kept_after_its_connection_closes() {
    for replacement_already_admitted in [false, true] {
        let (root, mut custody, lane, clock) = super::batch_limits::limits_fixture().await;
        let session = recipient_session();
        let old = lane.admitted.borrow().clone().unwrap();
        lane.push_event(LinkLaneEvent::Deliver {
            arrived_at: clock.utc_now(),
            connection: old.connection.clone(),
            offer: direct_offer(FIRST_MESSAGE, "queued before turnover"),
            sender: ResolvedSender::Central {},
        });
        let replacement =
            super::batch_replay::new_admission(session.machine_id.clone(), clock.clone());
        assert_ne!(old.connection, replacement.connection);
        lane.admitted
            .send_replace(replacement_already_admitted.then_some(replacement.clone()));

        custody.observe_next_event().await.unwrap();
        let mut reopened = InboxStore::new(
            MachineLocalStore::open(&root.path().join("machine_local.sqlite"))
                .await
                .unwrap(),
        );
        let receipts = reopened.pending_receipts().await.unwrap().healthy;
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.message_id.as_str())
                .collect::<Vec<_>>(),
            [FIRST_MESSAGE],
            "the lane's queued offer retains its admission across connection turnover"
        );
        let sent = lane.watch_receipt_commands();
        let (stop, stopped) = watch::channel(false);
        let (observed, mut events) = mpsc::unbounded_channel();
        let (calls, task) = custody_task(custody, stopped, Arc::new(ObserveTask(observed)));
        let worker = tokio::spawn(task);
        assert_eq!(calls.waiting_for_session(session).await.unwrap().len(), 1);
        assert!(sent.borrow().is_empty());
        lane.admitted.send_replace(Some(replacement.clone()));
        let wake = ClockInstant::ZERO + Duration::from_secs(20);
        clock.not_due_check_now(wake).await;
        assert!(
            sent.borrow().is_empty(),
            "an admitted socket must wait for the receipt mark"
        );
        assert_eq!(reopened.pending_receipts().await.unwrap().healthy, receipts);
        clock.advance(Duration::from_secs(20)).unwrap();
        assert_eq!(
            match events.recv().await.unwrap() {
                CustodyTaskEvent::Receipt(outcome) => outcome,
                other => panic!("expected a scheduled receipt completion, got {other:?}"),
            },
            ReceiptFrameOutcome::Confirmed {
                results: vec![ReceiptResult::Recorded]
            },
        );
        {
            let sent = sent.borrow();
            let [(at, command)] = sent.as_slice() else {
                panic!("expected one scheduled receipt, got {sent:?}");
            };
            assert_eq!(*at, wake);
            assert_eq!(command.connection, replacement.connection);
        }
        assert!(
            reopened
                .pending_receipts()
                .await
                .unwrap()
                .healthy
                .is_empty()
        );
        assert_eq!(reopened.pending_handoffs().await.unwrap().healthy.len(), 1);
        stop.send_replace(true);
        worker.await.unwrap();
    }
}

struct ScriptedDirectory(Mutex<RecipientSessionAnswer>);

impl ScriptedDirectory {
    fn set_answer(&self, answer: RecipientSessionAnswer) {
        *self.0.lock().unwrap() = answer;
    }
}

impl RecipientSessionDirectory for ScriptedDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(std::future::ready(*self.0.lock().unwrap()))
    }
}

fn recipient_machine() -> MachineId {
    "019f0000-0000-7000-8000-000000000011".parse().unwrap()
}

pub(super) fn recipient_session() -> SessionRef {
    SessionRef {
        machine_id: recipient_machine(),
        session_id: "recipient-session".parse().unwrap(),
    }
}

pub(super) fn direct_offer(id: &str, text: &str) -> ValidatedDeliveryOffer {
    let message = DeliveryMessage::Direct(DirectOfferMessage {
        message_id: id.parse().unwrap(),
        sender: MessageSender::Central,
        target: recipient_session(),
        body: DirectOfferBody::Direct {
            body: PostText::try_from(text.to_owned()).unwrap(),
            delivery: MessageDelivery::Auto,
            reply_to: None,
        },
        state: MessageState::Queued,
        source: None,
    });
    ValidatedDeliveryOffer::new(
        Box::new(message),
        Some(TimeLeft::try_from(123_000).unwrap()),
    )
    .unwrap()
}

async fn open_recipient(
    path: &Path,
    directory: Arc<ScriptedDirectory>,
    clock: Arc<ManualClock>,
) -> RecipientInbox {
    let store = InboxStore::new(MachineLocalStore::open(path).await.unwrap());
    RecipientInbox::new(store, recipient_machine(), directory, clock)
}

fn new_connection(clock: Arc<ManualClock>) -> ConnectionAttempt {
    let mut client = LinkClient::new(recipient_machine(), clock);
    let Some(ClientAction::Connect { attempt }) = client.poll_time() else {
        panic!("fixture client must supply a connection identity");
    };
    attempt
}

fn decode_receipts(frame: &ReceiptFrame) -> (CorrelationId, Vec<Receipt>) {
    let SubmitReceiptsCommand {
        correlation_id,
        receipts,
        ..
    } = frame.command(ClockInstant::ZERO.after(Duration::from_secs(30)));
    let request = LinkFrame::Data(DataFrame::Receipts {
        correlation_id,
        receipts,
    });
    let text = encode_text(&request, FrameDirection::ToCentral).unwrap();
    let LinkFrame::Data(DataFrame::Receipts {
        correlation_id,
        receipts,
    }) = decode_text(&text, FrameDirection::ToCentral).unwrap()
    else {
        panic!("the inbox command did not encode as a receipt frame");
    };
    (correlation_id, receipts.into())
}

fn answer_receipts(
    clock: &ManualClock,
    correlation_id: CorrelationId,
    results: Vec<ReceiptResult>,
) -> LinkLaneDisposition<CorrelatedReceiptResults> {
    LinkLaneDisposition::Answered {
        accepted_at: clock.monotonic_now(),
        reply: CorrelatedReceiptResults {
            correlation_id,
            results,
        },
    }
}

#[tokio::test]
async fn a_reoffer_after_reconnect_reuses_the_first_durable_decision_and_arrival() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(START_MS)).unwrap());
    let directory = Arc::new(ScriptedDirectory(Mutex::new(
        RecipientSessionAnswer::KnownOnce,
    )));
    let connection = new_connection(clock.clone());
    let mut first = open_recipient(&path, directory.clone(), clock.clone()).await;
    assert_eq!(
        first
            .receive_offer(
                direct_offer(FIRST_MESSAGE, "first delivery"),
                clock.utc_now()
            )
            .await
            .unwrap(),
        InboxOfferResult::Recorded {
            decision: InboxDecision::Accepted
        }
    );
    let original_payload = first
        .waiting_for_session(&recipient_session())
        .await
        .unwrap();
    assert_eq!(original_payload.len(), 1, "first delivery must be stored");
    let frame = first.prepare_receipts(&connection).await.unwrap().unwrap();
    let (_, first_receipts) = decode_receipts(&frame);
    assert!(
        matches!(first_receipts.as_slice(), [Receipt { kind: ReceiptKind::Delivered { arrived_at, waiting: None }, .. }] if arrived_at.timestamp_millis() == START_MS)
    );
    assert_eq!(
        first
            .settle_receipts(frame, Some(&connection), LinkLaneDisposition::AnswerLost)
            .await
            .unwrap(),
        ReceiptFrameOutcome::RetryPending
    );
    drop(first);

    clock.advance(Duration::from_secs(3)).unwrap();
    directory.set_answer(RecipientSessionAnswer::UnknownSession);
    let mut reopened = open_recipient(&path, directory, clock.clone()).await;
    let replacement = new_connection(clock.clone());
    assert_eq!(
        reopened
            .receive_offer(
                direct_offer(FIRST_MESSAGE, "different body on reoffer"),
                clock.utc_now()
            )
            .await
            .unwrap(),
        InboxOfferResult::AlreadyDecided {
            decision: InboxDecision::Accepted
        }
    );
    assert_eq!(
        reopened
            .waiting_for_session(&recipient_session())
            .await
            .unwrap(),
        original_payload
    );
    let replay = reopened
        .prepare_receipts(&replacement)
        .await
        .unwrap()
        .unwrap();
    let (correlation, replayed_receipts) = decode_receipts(&replay);
    assert_eq!(replayed_receipts, first_receipts);
    assert_eq!(
        reopened
            .settle_receipts(
                replay,
                Some(&replacement),
                answer_receipts(&clock, correlation, vec![ReceiptResult::AlreadySettled])
            )
            .await
            .unwrap(),
        ReceiptFrameOutcome::Confirmed {
            results: vec![ReceiptResult::AlreadySettled]
        }
    );
    assert!(
        reopened
            .prepare_receipts(&replacement)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reopened
            .waiting_for_session(&recipient_session())
            .await
            .unwrap(),
        original_payload
    );
}

#[tokio::test]
async fn unknown_ambiguous_and_unavailable_lookup_have_distinct_custody_results() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(START_MS)).unwrap());
    let directory = Arc::new(ScriptedDirectory(Mutex::new(
        RecipientSessionAnswer::CouldNotTell,
    )));
    let mut inbox = open_recipient(&path, directory.clone(), clock.clone()).await;
    let connection = new_connection(clock.clone());

    assert_eq!(
        inbox
            .receive_offer(
                direct_offer(FIRST_MESSAGE, "first arrival"),
                clock.utc_now()
            )
            .await
            .unwrap(),
        InboxOfferResult::LookupDeferred
    );
    assert!(inbox.needs_local_retry());
    assert!(
        inbox.prepare_receipts(&connection).await.unwrap().is_none(),
        "an unresolved lookup has no durable receipt"
    );

    directory.set_answer(RecipientSessionAnswer::UnknownSession);
    assert_eq!(
        inbox
            .receive_offer(direct_offer(SECOND_MESSAGE, "unknown"), clock.utc_now())
            .await
            .unwrap(),
        InboxOfferResult::Recorded {
            decision: InboxDecision::Refused {
                reason: InboxRefusalReason::UnknownSession
            }
        }
    );
    directory.set_answer(RecipientSessionAnswer::AmbiguousSession);
    assert_eq!(
        inbox
            .receive_offer(direct_offer(THIRD_MESSAGE, "ambiguous"), clock.utc_now())
            .await
            .unwrap(),
        InboxOfferResult::Recorded {
            decision: InboxDecision::Refused {
                reason: InboxRefusalReason::AmbiguousSession
            }
        }
    );
    assert!(
        inbox
            .waiting_for_session(&recipient_session())
            .await
            .unwrap()
            .is_empty()
    );

    clock.advance(Duration::from_secs(60)).unwrap();
    directory.set_answer(RecipientSessionAnswer::KnownOnce);
    assert!(inbox.retry_uncommitted().await.is_empty());
    assert!(!inbox.needs_local_retry());
    let original = inbox
        .waiting_for_session(&recipient_session())
        .await
        .unwrap();
    assert_eq!(original.len(), 1);
    let frame = inbox.prepare_receipts(&connection).await.unwrap().unwrap();
    let (_, receipts) = decode_receipts(&frame);
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.message_id.as_str())
            .collect::<Vec<_>>(),
        [FIRST_MESSAGE, SECOND_MESSAGE, THIRD_MESSAGE]
    );
    assert!(matches!(receipts.as_slice(), [
        Receipt { kind: ReceiptKind::Delivered { arrived_at, waiting: None }, .. },
        Receipt { kind: ReceiptKind::Refused { reason: MessageRefusal::UnknownSession }, .. },
        Receipt { kind: ReceiptKind::Refused { reason: MessageRefusal::AmbiguousSession }, .. },
    ] if arrived_at.timestamp_millis() == START_MS));
}

#[tokio::test]
async fn short_stale_mismatched_and_missing_answers_keep_the_receipt_frame_pending() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = Arc::new(ManualClock::starting_at(SyncTime::from_unix_millis(START_MS)).unwrap());
    let directory = Arc::new(ScriptedDirectory(Mutex::new(
        RecipientSessionAnswer::KnownOnce,
    )));
    let connection = new_connection(clock.clone());
    let replacement = new_connection(clock.clone());
    let mut inbox = open_recipient(&path, directory, clock.clone()).await;
    for id in [FIRST_MESSAGE, SECOND_MESSAGE] {
        inbox
            .receive_offer(direct_offer(id, "inbox body"), clock.utc_now())
            .await
            .unwrap();
    }
    let mut frame = inbox.prepare_receipts(&connection).await.unwrap().unwrap();
    let (correlation, receipts) = decode_receipts(&frame);
    let wrong_correlation = "019f0000-0000-7000-8000-000000000099".parse().unwrap();
    let result_pair = vec![ReceiptResult::Recorded, ReceiptResult::AlreadySettled];

    for answer in [
        answer_receipts(&clock, correlation.clone(), vec![ReceiptResult::Recorded]),
        answer_receipts(&clock, wrong_correlation, result_pair.clone()),
        LinkLaneDisposition::AnswerLost,
        LinkLaneDisposition::NotReached {
            cause: UntouchedCommand::WriterBusy,
        },
    ] {
        let stayed_untouched = matches!(&answer, LinkLaneDisposition::NotReached { .. });
        let (previous_correlation, _) = decode_receipts(&frame);
        let disposition = inbox
            .settle_receipts(frame, Some(&connection), answer)
            .await
            .unwrap();
        assert_eq!(disposition, ReceiptFrameOutcome::RetryPending);
        frame = inbox.prepare_receipts(&connection).await.unwrap().unwrap();
        let (retry_correlation, retried_receipts) = decode_receipts(&frame);
        assert_eq!(
            retry_correlation == previous_correlation,
            stayed_untouched,
            "only an untouched attempt can reuse its correlation"
        );
        assert_eq!(retried_receipts, receipts);
    }

    let (correlation, _) = decode_receipts(&frame);
    assert_eq!(
        inbox
            .settle_receipts(
                frame,
                Some(&replacement),
                answer_receipts(&clock, correlation, result_pair.clone())
            )
            .await
            .unwrap(),
        ReceiptFrameOutcome::StaleCompletion
    );
    let next = inbox.prepare_receipts(&replacement).await.unwrap().unwrap();
    let (next_correlation, next_receipts) = decode_receipts(&next);
    assert_eq!(
        next_receipts, receipts,
        "the stale completion kept both decisions pending"
    );
    assert_eq!(
        inbox
            .settle_receipts(
                next,
                Some(&replacement),
                answer_receipts(&clock, next_correlation, result_pair.clone())
            )
            .await
            .unwrap(),
        ReceiptFrameOutcome::Confirmed {
            results: result_pair
        }
    );
    assert!(
        inbox
            .prepare_receipts(&replacement)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        inbox
            .waiting_for_session(&recipient_session())
            .await
            .unwrap()
            .len(),
        2,
        "receipt does not hand messages to the session"
    );
}
