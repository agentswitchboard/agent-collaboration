use central_link_client::ReadyLinkLane;
use router_clock::RuntimeClock;
use runtime_primitives::SyncTime;
#[cfg(debug_assertions)]
use switchboard_machine_core_store::InboxStore;

use super::{DataFrame, FrameKind, LinkFrame, NetworkCustody, run_link_case};
#[cfg(debug_assertions)]
use super::{Duration, ReceiptResult, offered_message, queued_message};
use crate::simulated_link::packet_flight::decode_segment;

#[test]
fn first_byte_is_denied_at_the_closing_window_on_the_injected_transport() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let raw_ready = case.link.writer_gate.hold();
        let after = case.link.network.frames().len();
        let closing = case.link.clock.wall_deadline(SyncTime::from_unix_millis(
            receipt.flush_at.unix_millis() + 3_000,
        ));
        let writing_at = case.link.clock.wall_deadline(SyncTime::from_unix_millis(
            receipt.flush_at.unix_millis() - 1_500,
        ));
        case.advance_to(writing_at);
        let staged = raw_ready.await.unwrap();
        assert!(
            matches!(
                decode_segment(&staged, true).as_slice(),
                [LinkFrame::Data(DataFrame::Batch { .. })]
            ),
            "the held raw write must be the first batch bytes"
        );
        assert_eq!(case.link.clock.monotonic_now(), writing_at);
        assert!(
            case.link
                .network
                .frames()
                .iter()
                .skip(after)
                .all(|frame| frame.kind != FrameKind::Batch)
        );

        case.link.driver_clock.cross_after_pending_poll(closing);
        case.link.writer_gate.release();
        let mut admission = case.link.lane.watch_admitted();
        tokio::select! {
            biased;
            late = case.link.network.seen(after, FrameKind::Batch) => {
                panic!("the first batch byte crossed the closed minute window at {:?}", late.sent_at);
            }
            withdrawn = admission.wait_for(Option::is_none) => {
                assert!(withdrawn.is_ok(), "the driver must withdraw the untouched socket");
            }
        }
        assert_eq!(case.link.clock.monotonic_now(), closing);
        assert!(
            case.link
                .network
                .frames()
                .iter()
                .skip(after)
                .all(|frame| frame.kind != FrameKind::Batch)
        );
        assert!(case.central_queued_messages().await.is_empty());
        case.wait_for_replay(&receipt.id).await;

        case.link.reconnect().await;
        case.advance_to_next_mark().await;
        let sent = case.link.network.seen(after, FrameKind::Batch).await;
        assert!(sent.sent_at > closing);
        let message = case.finish_write(&receipt).await;
        case.wait_for_kept(&message).await;
        case.confirm_receipt(&message).await;
        case.close().await;
    });
}

#[test]
#[cfg(debug_assertions)]
fn a_paused_inbox_commit_cannot_acknowledge_the_offer() {
    run_link_case(|link| async move {
        let (mut case, mut commit) =
            NetworkCustody::setup(link, InboxStore::hold_next_commit).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::Offer);
        case.first_flush(&receipt).await;
        let arrival = case.link.network.seen(after, FrameKind::Offer).await;
        let answer = case.link.network.seen(after, FrameKind::WriteAnswer).await;
        let message = queued_message(&answer);
        assert_eq!(offered_message(&arrival), &message);
        assert_eq!(case.finish_write(&receipt).await, message);
        case.wait_until_next_mark_is_armed().await;
        case.link.network.release_and_wait().await;
        commit.until_held().await.unwrap();

        let after = case.link.network.frames().len();
        case.advance_to_next_mark().await;
        let held_mark = case.link.clock.monotonic_now();
        let kept = case.inbox.message_for(&message).await.unwrap();
        assert!(
            kept.healthy.is_none(),
            "the held inbox payload must not be durable yet"
        );
        if case
            .inbox
            .receipt_for(&message)
            .await
            .unwrap()
            .healthy
            .is_some()
        {
            let premature = case.link.network.seen(after, FrameKind::Receipt).await;
            panic!(
                "receipt left at {:?} before the message was durably kept",
                premature.sent_at
            );
        }
        assert!(
            case.link
                .network
                .frames()
                .iter()
                .skip(after)
                .all(|frame| frame.kind != FrameKind::Receipt)
        );
        assert_eq!(
            case.central_queued_messages().await.len(),
            1,
            "Central must still have an undelivered message"
        );
        case.advance_to(held_mark + Duration::from_secs(7));
        let missed_window = case.link.clock.monotonic_now();
        commit.release();
        case.wait_for_kept(&message).await;
        let network = case.link.network.clone();
        // Keep time frozen until the old turn either tries to send or
        // explicitly releases its prepared decision back to pending.
        tokio::select! {
            biased;
            premature = network.seen(after, FrameKind::Receipt) => {
                panic!("receipt left at {:?} after its minute window closed", premature.sent_at);
            }
            outcome = case.next_receipt_outcome() => {
                assert_eq!(outcome, super::ReceiptFrameOutcome::RetryPending);
            }
        }
        assert_eq!(case.link.clock.monotonic_now(), missed_window);
        assert_eq!(
            case.inbox.pending_receipts().await.unwrap().healthy.len(),
            1
        );
        assert_eq!(
            case.central_queued_messages().await,
            std::slice::from_ref(&message)
        );
        let next_mark = case.wake_at_next_mark();
        case.advance_to_next_mark().await;
        let wire = case.link.network.seen(after, FrameKind::Receipt).await;
        assert_eq!(
            wire.sent_at, next_mark,
            "receipt must wait for the next eligible mark after keeping the inbox payload"
        );
        case.wait_for_confirmation(&[ReceiptResult::Recorded]).await;
        case.close().await;
    });
}
