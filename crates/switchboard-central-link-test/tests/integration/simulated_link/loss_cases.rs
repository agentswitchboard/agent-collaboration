use super::{
    CustodyRequestStatus, DataFrame, Duration, FrameKind, LinkFrame, MessageId, MessageStatus,
    NetworkCustody, ReceiptFrameOutcome, ReceiptResult, assert_kept_receipt,
    assert_one_offer_per_socket, assert_replayed_member, next_receipt, offered_message, offers_for,
    queued_message, run_link_case,
};

#[test]
fn lost_write_batch_is_resent_on_a_later_mark() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::Batch);
        case.first_flush(&receipt).await;
        let lost = case.link.network.seen(after, FrameKind::Batch).await;
        case.link.network.discard().await;
        assert!(case.central_queued_messages().await.is_empty());
        assert_eq!(
            case.status(receipt.id.clone()).await,
            CustodyRequestStatus::Sent
        );
        assert!(
            case.calls
                .waiting_for_session(case.session.clone())
                .await
                .unwrap()
                .is_empty()
        );

        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.wait_for_replay(&receipt.id).await;
        case.link.reconnect().await;
        case.advance_to_next_mark().await;
        let replay = case.link.network.seen(after, FrameKind::Batch).await;
        assert_replayed_member(&lost, &replay, &receipt.id);
        assert_ne!(replay.connection_port, lost.connection_port);
        let message = case.finish_write(&receipt).await;
        case.wait_for_kept(&message).await;
        case.confirm_receipt(&message).await;
        case.close().await;
    });
}

#[test]
fn lost_write_answer_replays_the_original_central_result() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::WriteAnswer);
        case.first_flush(&receipt).await;
        let sent = case.link.network.seen(after, FrameKind::Batch).await;
        let lost = case.link.network.seen(after, FrameKind::WriteAnswer).await;
        let canonical = queued_message(&lost);
        assert_eq!(
            case.central_queued_messages().await,
            std::slice::from_ref(&canonical)
        );
        case.link.network.discard().await;

        case.advance_to(sent.sent_at + Duration::from_secs(7));
        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.wait_for_replay(&receipt.id).await;
        case.link.reconnect().await;
        let offer = case.link.network.seen(after, FrameKind::Offer).await;
        assert_eq!(offered_message(&offer), &canonical);
        case.wait_for_kept(&canonical).await;
        // The pending receipt and write can become due on the same mark.
        // The real driver arbitrates the lane, so observe the resulting batch.
        case.advance_to_next_mark().await;
        let replay = case.link.network.seen(after, FrameKind::Batch).await;
        assert_replayed_member(&sent, &replay, &receipt.id);
        assert_eq!(case.finish_write(&receipt).await, canonical);
        case.wait_for_confirmation(&[ReceiptResult::Recorded]).await;
        assert_one_offer_per_socket(&case, &canonical);
        case.close().await;
    });
}

#[test]
fn lost_offer_is_reoffered_on_reconnect() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let receipt = case.enqueue(1).await;
        let after = case.link.network.hold_next(FrameKind::Offer);
        case.first_flush(&receipt).await;
        let lost = case.link.network.seen(after, FrameKind::Offer).await;
        let message = case.finish_write(&receipt).await;
        assert_eq!(offered_message(&lost), &message);
        case.link.network.discard().await;
        assert!(
            case.inbox
                .receipt_for(&message)
                .await
                .unwrap()
                .healthy
                .is_none()
        );

        let after = case.link.network.frames().len();
        case.advance_to_next_mark().await;
        let pong = case.link.network.seen(after, FrameKind::Pong).await;
        assert_eq!(pong.connection_port, lost.connection_port);
        assert_one_offer_per_socket(&case, &message);

        case.advance_to(pong.sent_at + Duration::from_secs(7));
        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.link.reconnect().await;
        let replay = case.link.network.seen(after, FrameKind::Offer).await;
        assert_ne!(replay.connection_port, lost.connection_port);
        assert_eq!(offered_message(&replay), &message);
        case.wait_for_kept(&message).await;
        case.confirm_receipt(&message).await;
        case.close().await;
    });
}

#[test]
fn lost_receipt_reoffers_the_same_durably_kept_message() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let message = send_and_keep(&mut case).await;
        let before = case
            .inbox
            .receipt_for(&message)
            .await
            .unwrap()
            .healthy
            .unwrap();
        let after = case.link.network.hold_next(FrameKind::Receipt);
        case.advance_to_next_mark().await;
        let lost = case.link.network.seen(after, FrameKind::Receipt).await;
        assert_kept_receipt(&lost, &message, before.received_at_ms);
        case.link.network.discard().await;

        case.advance_to(lost.sent_at + Duration::from_secs(7));
        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.link.reconnect().await;
        let offer = case.link.network.seen(after, FrameKind::Offer).await;
        assert_ne!(offer.connection_port, lost.connection_port);
        assert_eq!(offered_message(&offer), &message);
        case.wait_for_kept(&message).await;
        case.confirm_receipt(&message).await;
        case.wait_for_kept(&message).await;
        assert_eq!(
            case.inbox.receipt_for(&message).await.unwrap().healthy,
            Some(before),
            "the repeat must preserve the original kept arrival"
        );
        assert_one_offer_per_socket(&case, &message);
        case.close().await;
    });
}

#[test]
fn lost_receipt_result_retries_the_durable_decision_without_a_reoffer() {
    run_link_case(|link| async move {
        let mut case = NetworkCustody::start(link).await;
        let message = send_and_keep(&mut case).await;
        let after = case.link.network.hold_next(FrameKind::ReceiptResult);
        case.advance_to_next_mark().await;
        let lost_receipt = case.link.network.seen(after, FrameKind::Receipt).await;
        let lost_result = case
            .link
            .network
            .seen(after, FrameKind::ReceiptResult)
            .await;
        let LinkFrame::Data(DataFrame::ReceiptResults { results, .. }) = &lost_result.frame else {
            panic!("expected Central's receipt result");
        };
        assert_eq!(results, &[ReceiptResult::Recorded]);
        case.link.network.discard().await;
        assert_eq!(
            case.inbox.pending_receipts().await.unwrap().healthy.len(),
            1
        );

        case.advance_to(lost_result.sent_at + Duration::from_secs(7));
        let after = case.link.network.frames().len();
        case.link.reset_socket().await;
        case.link.reconnect().await;
        assert!(matches!(
            case.central_status(&message).await,
            MessageStatus::Delivered { .. }
        ));
        let previous = next_receipt(&mut case.events).await;
        assert!(
            matches!(
                previous,
                ReceiptFrameOutcome::RetryPending | ReceiptFrameOutcome::StaleCompletion
            ),
            "an unanswered receipt must remain pending, got {previous:?}"
        );
        assert_eq!(
            case.inbox.pending_receipts().await.unwrap().healthy.len(),
            1
        );
        case.confirm_receipt(&message).await;
        let replay = case.link.network.seen(after, FrameKind::Receipt).await;
        assert_ne!(replay.connection_port, lost_receipt.connection_port);
        let LinkFrame::Data(DataFrame::Receipts {
            receipts: first, ..
        }) = lost_receipt.frame
        else {
            panic!("expected the original receipt");
        };
        let LinkFrame::Data(DataFrame::Receipts {
            receipts: second, ..
        }) = replay.frame
        else {
            panic!("expected the retried receipt");
        };
        assert_eq!(first, second);
        assert!(
            case.inbox
                .pending_receipts()
                .await
                .unwrap()
                .healthy
                .is_empty()
        );
        assert_eq!(
            offers_for(&case, &message).len(),
            1,
            "Central already recorded the receipt"
        );
        case.close().await;
    });
}

async fn send_and_keep(case: &mut NetworkCustody) -> MessageId {
    let receipt = case.enqueue(1).await;
    case.first_flush(&receipt).await;
    let message = case.finish_write(&receipt).await;
    case.wait_for_kept(&message).await;
    message
}
