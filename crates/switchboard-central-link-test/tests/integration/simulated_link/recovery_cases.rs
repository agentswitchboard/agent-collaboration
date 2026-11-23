use central_link_client::ReadyLinkLane;
use central_link_protocol::LinkRequestBatch;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;
use switchboard_machine_core_store::OutboxCancellation;

use super::{
    CustodyRequestStatus, DataFrame, Duration, FrameKind, InFlightFrame, LinkFrame, MessageStatus,
    NetworkCustody, ReceiptResult, assert_one_offer_per_socket, assert_replayed_member,
    offered_message, queued_message, run_link_case,
};
use crate::simulated_link::{CENTRAL, MAC};

#[test]
fn reply_after_answer_loss_cannot_settle_the_retired_batch() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::WriteAnswer);
        case.first_flush(&receipt).await;
        let sent = case.link.network.seen(after, FrameKind::Batch).await;
        let delayed = case.link.network.seen(after, FrameKind::WriteAnswer).await;
        let message = queued_message(&delayed);

        case.advance_to(sent.sent_at + Duration::from_secs(31));
        case.wait_for_replay(&receipt.id).await;
        case.link.network.release();
        // This status result crosses the same TCP stream after the late
        // answer, proving the old answer had a chance to reach the lane.
        assert!(matches!(
            case.central_status(&message).await,
            MessageStatus::Sent {
                deadline_passed: false
            },
        ));
        assert!(matches!(
            case.status(receipt.id.clone()).await,
            CustodyRequestStatus::InOutbox { .. }
        ));
        case.wait_for_kept(&message).await;

        let after = case.link.network.frames().len();
        case.advance_to_next_mark().await;
        let replay = case.link.network.seen(after, FrameKind::Batch).await;
        assert_eq!(replay.connection_port, sent.connection_port);
        assert_replayed_member(&sent, &replay, &receipt.id);
        assert_eq!(case.finish_write(&receipt).await, message);
        case.wait_for_confirmation(&[ReceiptResult::Recorded]).await;
        assert_eq!(super::offers_for(&case, &message).len(), 1);
        case.close().await;
    });
}

#[test]
fn an_old_socket_reply_arrives_after_replacement_admission() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::WriteAnswer);
        case.first_flush(&receipt).await;
        let sent = case.link.network.seen(after, FrameKind::Batch).await;
        let old = case.link.network.seen(after, FrameKind::WriteAnswer).await;
        let canonical = queued_message(&old);

        case.advance_to(sent.sent_at + Duration::from_secs(7));
        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.wait_for_replay(&receipt.id).await;
        case.link.reconnect().await;
        let fresh = case
            .link
            .network
            .seen_matching(
                after,
                |frame| {
                    frame.kind == FrameKind::Offer && frame.connection_port != old.connection_port
                },
                0,
            )
            .await;
        assert_ne!(fresh.connection_port, old.connection_port);
        assert_eq!(offered_message(&fresh), &canonical);
        case.wait_for_kept(&canonical).await;
        case.link.network.release_and_wait().await;
        assert!(matches!(
            case.status(receipt.id.clone()).await,
            CustodyRequestStatus::InOutbox { .. }
        ));

        case.advance_to_next_mark().await;
        let replay = case.link.network.seen(after, FrameKind::Batch).await;
        assert_eq!(replay.connection_port, fresh.connection_port);
        assert_replayed_member(&sent, &replay, &receipt.id);
        assert_eq!(case.finish_write(&receipt).await, canonical);
        case.wait_for_confirmation(&[ReceiptResult::Recorded]).await;
        assert_one_offer_per_socket(&case, &canonical);
        case.close().await;
    });
}

#[test]
fn a_disconnect_between_write_answers_preserves_completed_members() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let first = case.enqueue(1).await;
        let second = case.enqueue(2).await;
        assert_eq!(second.flush_at, first.flush_at);
        let after = case.link.network.hold_after(FrameKind::WriteAnswer, 1);
        case.first_flush(&first).await;
        let batch = case.link.network.seen(after, FrameKind::Batch).await;
        assert_eq!(batch_members(&batch).requests().len(), 2);
        let answered = case.link.network.seen(after, FrameKind::WriteAnswer).await;
        let missing = case
            .link
            .network
            .seen_nth(after, FrameKind::WriteAnswer, 1)
            .await;
        assert_eq!(answered.connection_port, missing.connection_port);
        case.link.network.discard().await;

        case.advance_to(batch.sent_at + Duration::from_secs(7));
        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.wait_for_replay(&second.id).await;
        assert!(
            matches!(
                case.status(first.id.clone()).await,
                CustodyRequestStatus::Answered(_)
            ),
            "the member answered on the old socket must already be settled"
        );
        let accepted = case.finish_write(&first).await;
        assert_eq!(accepted, queued_message(&answered));
        case.link.reconnect().await;
        let renewed = case.link.network.seen(after, FrameKind::Welcome).await;
        for index in 0..2 {
            let _ = case
                .link
                .network
                .seen_matching(
                    after,
                    |frame| {
                        frame.kind == FrameKind::Offer
                            && frame.connection_port == renewed.connection_port
                    },
                    index,
                )
                .await;
        }
        case.wait_for_kept(&accepted).await;
        case.wait_for_kept(&queued_message(&missing)).await;
        case.advance_to_next_mark().await;
        let replay = case.link.network.seen(after, FrameKind::Batch).await;
        assert_eq!(batch_members(&replay).requests().len(), 1);
        assert_replayed_member(&batch, &replay, &second.id);
        let remaining = case.finish_write(&second).await;
        assert_eq!(remaining, queued_message(&missing));
        assert_ne!(accepted, remaining);
        // A receipt and write can contend for this mark. If custody deferred
        // the receipts, observe them at the next scheduled mark.
        if matches!(
            case.central_status(&accepted).await,
            MessageStatus::Sent { .. }
        ) {
            case.advance_to_next_mark().await;
            case.wait_for_confirmation(&[ReceiptResult::Recorded, ReceiptResult::Recorded])
                .await;
        }
        for message in [&accepted, &remaining] {
            assert!(matches!(
                case.central_status(message).await,
                MessageStatus::Delivered { .. }
            ));
        }
        assert_one_offer_per_socket(&case, &accepted);
        assert_one_offer_per_socket(&case, &remaining);
        case.close().await;
    });
}

#[test]
fn a_disconnect_mid_receipts_keeps_both_decisions_and_only_reoffers_the_unrecorded_one() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let first = case.enqueue(1).await;
        let second = case.enqueue(2).await;
        assert_eq!(second.flush_at, first.flush_at);
        let after = case.link.network.frames().len();
        case.first_flush(&first).await;
        let first_offer = case.link.network.seen(after, FrameKind::Offer).await;
        let second_offer = case.link.network.seen_nth(after, FrameKind::Offer, 1).await;
        case.wait_for_kept(offered_message(&first_offer)).await;
        case.wait_for_kept(offered_message(&second_offer)).await;
        let rows = case.inbox.pending_receipts().await.unwrap().healthy;
        let [recorded, unrecorded] = rows.as_slice() else {
            panic!("expected two independently kept receipt decisions");
        };
        let runtime = case
            .link
            .central
            .host
            .central(&case.link.central.organization_id)
            .await
            .unwrap();
        let mut first_commit = runtime
            .runtime()
            .pause_points()
            .hold_engine(PausePoint::AfterEngineCommit, recorded.message_id.as_str());
        let after = case.link.network.frames().len();
        case.advance_to_next_mark().await;
        let sent = case.link.network.seen(after, FrameKind::Receipt).await;
        let LinkFrame::Data(DataFrame::Receipts { receipts, .. }) = &sent.frame else {
            panic!("expected the two-member receipt frame");
        };
        assert!(matches!(receipts.receipts(), [first, second]
            if first.message_id == recorded.message_id && second.message_id == unrecorded.message_id));
        first_commit.until_held().await;

        let after = case.link.network.frames().len();
        case.advance_to(sent.sent_at + Duration::from_secs(7));
        case.link.reset_socket().await;
        case.link.reconnect().await;
        let repeated = case.link.network.seen(after, FrameKind::Offer).await;
        assert_ne!(sent.connection_port, repeated.connection_port);
        assert_eq!(offered_message(&repeated), &unrecorded.message_id);
        assert_eq!(super::offers_for(&case, &recorded.message_id).len(), 1);
        assert!(matches!(
            case.central_status(&recorded.message_id).await,
            MessageStatus::Delivered { .. }
        ));
        assert!(matches!(
            case.central_status(&unrecorded.message_id).await,
            MessageStatus::Sent {
                deadline_passed: false
            }
        ));
        first_commit.release();
        case.advance_to_next_mark().await;
        case.wait_for_confirmation(&[ReceiptResult::Recorded, ReceiptResult::Recorded])
            .await;
        assert!(
            case.inbox
                .pending_receipts()
                .await
                .unwrap()
                .healthy
                .is_empty(),
            "both confirmed receipt decisions must retire from the durable pending set"
        );
        for row in rows {
            case.wait_for_kept(&row.message_id).await;
            assert!(matches!(
                case.central_status(&row.message_id).await,
                MessageStatus::Delivered { .. }
            ));
        }
        case.close().await;
    });
}

#[test]
fn a_partition_past_fifteen_minutes_ends_sent_and_unsent_writes() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let sent_at = case.link.clock.monotonic_now();
        let first = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::Batch);
        case.first_flush(&first).await;
        let _ = case.link.network.seen(after, FrameKind::Batch).await;
        case.link.network.discard().await;
        turmoil::partition(MAC, CENTRAL);
        case.link.reset_socket().await;
        case.wait_for_replay(&first.id).await;
        let unsent_at = case.link.clock.monotonic_now();
        let second = case.enqueue(2).await;
        let first_cutoff = sent_at + Duration::from_secs(15 * 60);
        let second_cutoff = unsent_at + Duration::from_secs(15 * 60);

        let isolated_mark = case.wake_at_next_mark();
        let mut retry = case.link.next_retry().await;
        while retry <= isolated_mark {
            case.link.start_retry(retry);
            case.link.require_isolated_dial_failure().await;
            retry = case.link.next_retry().await;
        }
        case.advance_to(isolated_mark);
        assert!(
            case.link.lane.watch_admitted().borrow().is_none(),
            "the eligible mark passed with no admitted replacement"
        );
        case.link.start_retry(retry);
        case.link.require_isolated_dial_failure().await;

        advance_isolated(&mut case, sent_at + Duration::from_millis(899_999)).await;
        assert!(matches!(
            case.status(first.id.clone()).await,
            CustodyRequestStatus::InOutbox { .. }
        ));
        assert!(matches!(
            case.status(second.id.clone()).await,
            CustodyRequestStatus::InOutbox { .. }
        ));
        advance_isolated(&mut case, first_cutoff).await;
        let ended = case.next_write_report().await;
        assert!(
            matches!(ended.ended.as_slice(), [write] if write.request_id == first.id
            && write.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
        );
        advance_isolated(&mut case, unsent_at + Duration::from_millis(899_999)).await;
        assert!(matches!(
            case.status(second.id.clone()).await,
            CustodyRequestStatus::InOutbox { .. }
        ));
        advance_isolated(&mut case, second_cutoff).await;
        let ended = case.next_write_report().await;
        assert!(
            matches!(ended.ended.as_slice(), [write] if write.request_id == second.id
            && write.reason == OutboxCancellation::NotSentWithin15Minutes)
        );
        assert_eq!(
            case.status(first.id.clone()).await,
            CustodyRequestStatus::Ended {
                reason: OutboxCancellation::NotConfirmedWithin15Minutes,
            }
        );
        assert_eq!(
            case.status(second.id.clone()).await,
            CustodyRequestStatus::Ended {
                reason: OutboxCancellation::NotSentWithin15Minutes,
            }
        );
        advance_isolated(&mut case, second_cutoff + Duration::from_millis(1)).await;
        case.link.reconnect_after_partition().await;
        case.advance_to_next_mark().await;
        case.wait_until_next_mark_is_armed().await;
        // A real request-status round trip on the replacement is the wire
        // barrier after the completed turn with no sendable local rows.
        assert_eq!(
            case.status(super::request_id(3)).await,
            CustodyRequestStatus::NotFound
        );
        assert!(case.central_queued_messages().await.is_empty());
        assert!(matches!(
            case.status(first.id).await,
            CustodyRequestStatus::Ended { .. }
        ));
        assert!(matches!(
            case.status(second.id).await,
            CustodyRequestStatus::Ended { .. }
        ));
        assert_eq!(
            case.link
                .network
                .frames()
                .iter()
                .filter(|frame| frame.kind == FrameKind::Batch)
                .count(),
            1,
            "the closed outbox writes are never submitted after healing"
        );
        case.close().await;
    });
}

async fn advance_isolated(case: &mut NetworkCustody, deadline: router_clock::ClockInstant) {
    if case.link.next_retry().await <= deadline {
        case.link.start_retry(deadline);
        case.link.require_isolated_dial_failure().await;
    } else {
        case.advance_to(deadline);
    }
}

#[test]
fn admission_inside_an_open_window_uses_its_pending_receipt_mark() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let message = super::send_and_keep(&mut case).await;
        // The second mark is UTC 17:48:00 and this installation's wake is
        // 633ms later. Admit after the window opens and before that wake.
        case.advance_to(router_clock::ClockInstant::ZERO + Duration::from_millis(77_900));
        case.link.reset_socket().await;
        case.link.reconnect().await;
        let admitted_at = case.link.clock.monotonic_now();
        assert_eq!(
            admitted_at,
            router_clock::ClockInstant::ZERO + Duration::from_millis(78_900)
        );
        assert_eq!(
            case.wake_at_next_mark(),
            router_clock::ClockInstant::ZERO + Duration::from_millis(80_633),
            "the helper must drive the open mark rather than wait for a later timer"
        );
        let after = case.link.network.frames().len();
        case.confirm_receipt(&message).await;
        let receipt = case.link.network.seen(after, FrameKind::Receipt).await;
        assert_eq!(
            receipt.sent_at,
            router_clock::ClockInstant::ZERO + Duration::from_millis(80_633)
        );
        case.close().await;
    });
}

fn batch_members(frame: &InFlightFrame) -> &LinkRequestBatch {
    let LinkFrame::Data(DataFrame::Batch { requests }) = &frame.frame else {
        panic!("expected a write batch");
    };
    requests
}
