use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use central_link_client::ReadyLinkLane;
use central_link_protocol::{
    CorrelationId, DataFrame, FrameDirection, LinkFrame, ReceiptBatch, ReceiptKind, ReceiptResult,
    encode_text,
};
use router_clock::RuntimeClock;
use switchboard_machine_core_store::{DirectHandoff, InboxDecision, SeenReceiptState};
use switchboard_message_custody::receipt_delivery::ReceiptFrameOutcome;
use tungstenite::protocol::frame::{
    Frame,
    coding::{Data, OpCode},
};
use workspace_ids::MessageId;
use workspace_model::central_mail::MessageStatus;

use super::{
    FrameKind, InFlightFrame, NetworkCustody,
    handover_cases::{advance_local_mark, wait_for_seen},
    offers_for, run_link_case, send_and_keep,
};
use crate::managed_prompt_fixture::{ManagedPromptPort, NoticeResultScript};

#[test]
fn lost_receipt_answer_resends_on_the_new_connection_and_only_matching_full_answer_confirms() {
    run_link_case(|link| async move {
        let (offers, mut written) = tokio::sync::mpsc::unbounded_channel();
        let takes = Arc::new(AtomicUsize::new(0));
        let enabled = Arc::new(AtomicBool::new(false));
        let port = Arc::new(ManagedPromptPort {
            clock: link.clock.clone(),
            enabled: enabled.clone(),
            calls: takes.clone(),
            offers,
            result: NoticeResultScript::ReturnWriterTake,
        });
        let (mut case, ()) = NetworkCustody::setup_with_custody(
            link,
            |_| (),
            |custody| custody.with_notice_port(port),
        )
        .await;
        let message = send_and_keep(&mut case).await;
        let saved = case
            .inbox
            .receipt_for(&message)
            .await
            .unwrap()
            .healthy
            .unwrap();
        assert_eq!(saved.message_id, message);
        assert_eq!(saved.decision, InboxDecision::Accepted);

        enabled.store(true, Ordering::SeqCst);
        let mark = advance_local_mark(&case).await;
        let notice = written.recv().await.unwrap();
        assert_eq!(notice.target(), &case.session);
        assert_eq!(case.link.clock.monotonic_now(), mark);
        let taken_at = case.link.clock.utc_now();
        wait_for_seen(&mut case.inbox, taken_at, SeenReceiptState::Owed).await;

        let (first, answer_lost) = hold_receipt_answer(&case).await;
        let (original_correlation, original_facts) = receipt_facts(&first);
        let facts = original_facts.receipts();
        assert!(
            matches!(facts, [delivered, seen]
                if delivered.message_id == message
                    && matches!(delivered.kind, ReceiptKind::Delivered { arrived_at }
                        if arrived_at.timestamp_millis() == saved.received_at_ms)
                    && seen.message_id == message
                    && seen.kind == (ReceiptKind::Seen { completed_at: taken_at })),
            "the original batch must carry the accepted identity, arrival and notice: {facts:?}",
        );
        assert_recorded_answer(&answer_lost, original_correlation, 2);

        // Central emits Recorded only after commit. Drop that answer before
        // resetting the actual driver connection.
        case.link.network.discard().await;
        assert_receipt_is_owed(&mut case, &message, "lost answer").await;
        case.advance_to(answer_lost.sent_at + Duration::from_secs(7));
        case.link.reset_socket().await;
        case.link.reconnect().await;
        assert!(matches!(
            case.next_receipt_outcome().await,
            ReceiptFrameOutcome::RetryPending | ReceiptFrameOutcome::StaleCompletion
        ));
        drop(
            case.link
                .lane
                .watch_registered()
                .wait_for(Option::is_some)
                .await
                .unwrap(),
        );
        assert_eq!(
            case.central_status(&message).await,
            MessageStatus::Seen {
                completed_at: taken_at
            },
            "Central applied the original receipt before the connection was lost",
        );

        let last_correlation =
            reject_unmatched_answers(&mut case, &first, &answer_lost, &message).await;

        let (resent, matching) = hold_receipt_answer(&case).await;
        let (final_correlation, final_facts) = receipt_facts(&resent);
        assert_ne!(final_correlation, &last_correlation);
        assert_eq!(final_facts, original_facts);
        assert_recorded_answer(&matching, final_correlation, 2);
        case.link.network.release_and_wait().await;
        assert_eq!(
            case.next_receipt_outcome().await,
            ReceiptFrameOutcome::Confirmed {
                results: vec![ReceiptResult::Recorded; 2]
            },
            "only the matching full answer confirms the receipt",
        );
        let rows = case.inbox.scan_direct_handoffs().await.unwrap();
        assert!(rows.corrupt_rows.is_empty());
        let [row] = rows.healthy.as_slice() else {
            panic!("expected exactly one inbox row after confirmation");
        };
        assert_eq!(row.message_id, message);
        assert!(row.delivery_receipt_confirmed);
        assert_eq!(
            row.handoff,
            DirectHandoff::Seen {
                completed_at: taken_at,
                receipt: SeenReceiptState::Recorded,
            }
        );
        assert_eq!(
            case.inbox.receipt_for(&message).await.unwrap().healthy,
            Some(saved)
        );

        let after_confirmation = case.link.network.frames().len();
        case.advance_to_next_mark().await;
        case.wait_until_next_mark_is_armed().await;
        assert!(
            case.link.network.frames().iter().skip(after_confirmation)
                .all(|wire| !matches!(&wire.frame, LinkFrame::Data(DataFrame::Receipts { receipts, .. })
                    if receipts.receipts().iter().any(|receipt| receipt.message_id == message))),
            "the Mac must stop sending the confirmed receipt",
        );
        assert_eq!(
            takes.load(Ordering::SeqCst),
            1,
            "the session was handed the message once"
        );
        assert_eq!(
            offers_for(&case, &message).len(),
            1,
            "Central had already applied the receipt"
        );
        assert!(case.receipt_outcomes.is_empty());
        assert!(
            case.events.try_recv().is_err(),
            "there must be no second acknowledgement"
        );
        case.close().await;
    });
}

async fn reject_unmatched_answers(
    case: &mut NetworkCustody,
    first: &InFlightFrame,
    answer_lost: &InFlightFrame,
    message: &MessageId,
) -> CorrelationId {
    let (original_correlation, original_facts) = receipt_facts(first);
    let mut last_correlation = original_correlation.clone();
    for rejection in [
        InvalidAnswer::StaleSocket,
        InvalidAnswer::WrongCorrelation,
        InvalidAnswer::ShortResults,
    ] {
        let (resent, real_answer) = hold_receipt_answer(case).await;
        let (resent_correlation, facts) = receipt_facts(&resent);
        assert_ne!(resent.connection_port, first.connection_port);
        assert_ne!(resent_correlation, &last_correlation, "{rejection:?}");
        assert_eq!(
            facts, original_facts,
            "{rejection:?} must preserve the receipt facts"
        );
        assert_recorded_answer(&real_answer, resent_correlation, 2);
        last_correlation = resent_correlation.clone();

        // The live server produced a valid reply for the replacement
        // request. Consume it at the socket input seam without feeding it
        // to the real driver, then feed exactly one misleading reply.
        let discarded = case.link.read_probe.discard_matching_read(|bytes| {
            super::super::packet_flight::decode_segment(bytes, false)
                .iter()
                .any(|frame| matches!(frame, LinkFrame::Data(DataFrame::ReceiptResults { .. })))
        });
        case.link.network.release();
        let bytes = discarded.await.unwrap();
        assert_eq!(
            super::super::packet_flight::decode_segment(&bytes, false),
            std::slice::from_ref(&real_answer.frame),
        );
        let forged = rejection.wire_reply(answer_lost, resent_correlation);
        let read_turn = case.link.read_probe.observe_next_read_turn();
        case.link.read_probe.replay_read(forged.clone());
        assert_eq!(
            read_turn.await.unwrap(),
            forged,
            "{rejection:?} reached the driver"
        );
        case.advance_to(resent.sent_at + Duration::from_secs(31));
        assert_eq!(
            case.next_receipt_outcome().await,
            ReceiptFrameOutcome::RetryPending,
            "{rejection:?} must not acknowledge any row",
        );
        assert_receipt_is_owed(case, message, rejection.name()).await;
    }
    last_correlation
}

async fn hold_receipt_answer(case: &NetworkCustody) -> (InFlightFrame, InFlightFrame) {
    let after = case.link.network.hold_next(FrameKind::ReceiptResult);
    case.advance_to_next_mark().await;
    let request = tokio::select! {
        biased;
        frame = case.link.network.seen(after, FrameKind::Receipt) => frame,
        () = case.wait_until_next_mark_is_armed() => {
            panic!("the Mac did not send its owed receipt at the armed mark");
        }
    };
    let answer = case
        .link
        .network
        .seen(after, FrameKind::ReceiptResult)
        .await;
    assert_eq!(answer.connection_port, request.connection_port);
    (request, answer)
}

fn receipt_facts(frame: &InFlightFrame) -> (&CorrelationId, &ReceiptBatch) {
    let LinkFrame::Data(DataFrame::Receipts {
        correlation_id,
        receipts,
    }) = &frame.frame
    else {
        panic!("expected an outgoing receipt");
    };
    (correlation_id, receipts)
}

fn assert_recorded_answer(frame: &InFlightFrame, correlation: &CorrelationId, count: usize) {
    assert!(
        matches!(&frame.frame, LinkFrame::Data(DataFrame::ReceiptResults { correlation_id, results })
            if correlation_id == correlation && *results == vec![ReceiptResult::Recorded; count]),
        "expected Central's matching recorded reply, got {:?}",
        frame.frame,
    );
}

async fn assert_receipt_is_owed(case: &mut NetworkCustody, message: &MessageId, situation: &str) {
    let pending = case.inbox.pending_receipts().await.unwrap();
    assert!(
        matches!(pending.healthy.as_slice(), [row] if &row.message_id == message),
        "{situation}: the delivery receipt must still be owed",
    );
    let rows = case.inbox.scan_direct_handoffs().await.unwrap();
    assert!(
        matches!(rows.healthy.as_slice(), [row] if &row.message_id == message
            && !row.delivery_receipt_confirmed
            && matches!(row.handoff, DirectHandoff::Seen { receipt: SeenReceiptState::Owed, .. })),
        "{situation}: neither receipt may be confirmed",
    );
}

#[derive(Clone, Copy, Debug)]
enum InvalidAnswer {
    StaleSocket,
    WrongCorrelation,
    ShortResults,
}

impl InvalidAnswer {
    fn name(self) -> &'static str {
        match self {
            Self::StaleSocket => "prior socket answer",
            Self::WrongCorrelation => "wrong correlation",
            Self::ShortResults => "short result",
        }
    }

    fn wire_reply(self, previous_answer: &InFlightFrame, current: &CorrelationId) -> Vec<u8> {
        let reply = match self {
            Self::StaleSocket => previous_answer.frame.clone(),
            Self::WrongCorrelation => LinkFrame::Data(DataFrame::ReceiptResults {
                correlation_id: "019f0000-0000-7000-8000-00000000ce19".parse().unwrap(),
                results: vec![ReceiptResult::Recorded; 2],
            }),
            Self::ShortResults => LinkFrame::Data(DataFrame::ReceiptResults {
                correlation_id: current.clone(),
                results: vec![ReceiptResult::Recorded],
            }),
        };
        let text = encode_text(&reply, FrameDirection::ToRouter).unwrap();
        let mut bytes = Vec::new();
        Frame::message(text, OpCode::Data(Data::Text), true)
            .format(&mut bytes)
            .unwrap();
        bytes
    }
}

#[test]
fn reopening_custody_and_the_link_resends_the_same_saved_receipt() {
    use super::super::SimulatedLink;

    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let message = send_and_keep(&mut case).await;
        let saved = case
            .inbox
            .receipt_for(&message)
            .await
            .unwrap()
            .healthy
            .unwrap();
        let (first, lost) = hold_receipt_answer(&case).await;
        let (first_id, first_facts) = receipt_facts(&first);
        assert_recorded_answer(&lost, first_id, 1);
        case.link.network.discard().await;

        let clock = case.link.clock.clone();
        let central = case.link.central.clone();
        let network = case.link.network.clone();
        case.close().await;

        let replacement = SimulatedLink::start(clock, network, central).await;
        drop(
            replacement
                .lane
                .watch_registered()
                .wait_for(Option::is_some)
                .await
                .unwrap(),
        );
        // The replacement registers while the minute window is still open.
        // Arm the fault before starting the reopened custody worker.
        let after_reopen = replacement.network.hold_next(FrameKind::ReceiptResult);
        let (mut reopened, ()) =
            NetworkCustody::start_custody(replacement, |_| (), |custody| custody).await;
        assert_eq!(
            reopened
                .inbox
                .pending_receipts()
                .await
                .unwrap()
                .healthy
                .as_slice(),
            std::slice::from_ref(&saved),
            "the reopened store must still owe the original durable decision",
        );
        let request = reopened
            .link
            .network
            .seen(after_reopen, FrameKind::Receipt)
            .await;
        let answer = reopened
            .link
            .network
            .seen(after_reopen, FrameKind::ReceiptResult)
            .await;
        assert_eq!(answer.connection_port, request.connection_port);
        let (new_id, new_facts) = receipt_facts(&request);
        assert_ne!(request.connection_port, first.connection_port);
        assert_ne!(new_id, first_id);
        assert_eq!(new_facts, first_facts);
        assert_recorded_answer(&answer, new_id, 1);

        reopened.link.network.release_and_wait().await;
        assert_eq!(
            reopened.next_receipt_outcome().await,
            ReceiptFrameOutcome::Confirmed {
                results: vec![ReceiptResult::Recorded]
            },
        );
        assert!(
            reopened
                .inbox
                .pending_receipts()
                .await
                .unwrap()
                .healthy
                .is_empty()
        );
        assert_eq!(
            reopened.inbox.receipt_for(&message).await.unwrap().healthy,
            Some(saved)
        );
        assert_eq!(offers_for(&reopened, &message).len(), 1);
        reopened.close().await;
    });
}
