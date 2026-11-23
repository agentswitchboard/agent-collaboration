use super::{FIFTEEN_MINUTES_MS, MARK, ids, item, message};
use crate::{
    LocalOutboxRefusal, OutboxCancellation, OutboxInsert, OutboxRequestStatus, OutboxStorage,
    OutboxStoreError, PendingWriteState, StoredOutboxWrite,
};
use workspace_ids::RequestId;

/// A waiting write is never claimed at or past its deadline; the same claim
/// ends it, by target and by whether it had left the Mac, and drops its text.
pub(crate) async fn an_overdue_write_is_never_claimed_and_ends_with_its_reason(
    store: &mut dyn OutboxStorage,
) {
    let stored_at = MARK - 1_000;
    let deadline = stored_at + FIFTEEN_MINUTES_MS;
    let unsent = message(70, "never left", MARK);
    let lost = message(71, "answer lost", MARK);
    let post = item(72, "a thread post", MARK);
    for write in [&unsent, &lost, &post] {
        store.insert(write, stored_at).await.unwrap();
    }
    store.mark_sent(&ids(&[&lost]), MARK).await.unwrap();
    store
        .return_unanswered(&ids(&[&lost]), MARK + 1_000)
        .await
        .unwrap()
        .expect_clean();

    let early = store
        .claim_next_batch(MARK, 1, deadline - 1)
        .await
        .unwrap()
        .expect_clean();
    store
        .return_unanswered(&ids(&[&unsent]), deadline - 1)
        .await
        .unwrap()
        .expect_clean();
    let claimed = store
        .claim_next_batch(MARK, 64, deadline)
        .await
        .unwrap()
        .expect_clean();

    assert_eq!(
        early
            .writes
            .iter()
            .map(|w| w.request.clone())
            .collect::<Vec<_>>(),
        ids(&[&unsent]),
        "one millisecond before its deadline a direct message is still sent"
    );
    assert!(early.ended.is_empty());
    assert_eq!(
        claimed
            .writes
            .iter()
            .map(|w| w.request.clone())
            .collect::<Vec<_>>(),
        Vec::<RequestId>::new(),
        "at its deadline nothing is claimed: posts have the same 15 minutes"
    );
    let ended: Vec<_> = claimed
        .ended
        .iter()
        .map(|write| (write.request_id.clone(), write.caller.clone(), write.reason))
        .collect();
    assert_eq!(
        ended,
        vec![
            (
                unsent.request.clone(),
                unsent.caller.clone(),
                OutboxCancellation::NotConfirmedWithin15Minutes
            ),
            (
                lost.request.clone(),
                lost.caller.clone(),
                OutboxCancellation::NotConfirmedWithin15Minutes
            ),
            (
                post.request.clone(),
                post.caller.clone(),
                OutboxCancellation::NotSentWithin15Minutes
            ),
        ],
        "the two messages had left the Mac (may have arrived); the post never did"
    );
    let never_left = message(73, "never left", MARK);
    store.insert(&never_left, stored_at).await.unwrap();
    let ended_later = store.end_overdue(deadline).await.unwrap().expect_clean();
    assert_eq!(
        ended_later
            .iter()
            .map(|write| (write.request_id.clone(), write.reason))
            .collect::<Vec<_>>(),
        vec![(
            never_left.request.clone(),
            OutboxCancellation::NotSentWithin15Minutes
        )]
    );
    assert_eq!(
        store
            .request_status(&never_left.caller, &never_left.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotSentWithin15Minutes
        }
    );
    let pending = store.pending_for(&never_left.caller).await.unwrap();
    assert!(
        pending
            .iter()
            .any(|write| write.request_id == never_left.request
                && write.state
                    == PendingWriteState::Cancelled {
                        reason: OutboxCancellation::NotSentWithin15Minutes
                    }),
        "the ended write reads as cancelled, with no text: {pending:?}"
    );
    assert!(matches!(
        store.insert(&never_left, deadline + 1).await.unwrap(),
        OutboxInsert::AlreadyStored(OutboxRequestStatus::Ended { .. })
    ));
    let mut changed = never_left.clone();
    changed.payload = message(73, "different words", MARK).payload;
    assert_eq!(
        store.insert(&changed, deadline + 1).await.unwrap(),
        OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict),
        "the text is gone but its digest still tells a different write"
    );
    assert!(
        store.waiting().await.unwrap().expect_clean().is_empty(),
        "ended writes no longer wait"
    );
    assert!(matches!(
        store.mark_sent(&ids(&[&never_left]), deadline + 1).await,
        Err(OutboxStoreError::UnexpectedState { .. })
    ));
}

/// A sent write past its deadline ends instead of returning to waiting, so
/// a replay after a long outage never commits a second time; a write before
/// its deadline returns as before.
pub(crate) async fn a_sent_write_past_its_deadline_ends_instead_of_returning(
    store: &mut dyn OutboxStorage,
) {
    let stored_at = MARK - 1_000;
    let post = item(80, "posted then lost", MARK);
    let fresh = item(81, "lost, still fresh", MARK);
    store.insert(&post, stored_at).await.unwrap();
    store
        .insert(&fresh, stored_at + FIFTEEN_MINUTES_MS - 60_000)
        .await
        .unwrap();
    store.mark_sent(&ids(&[&post]), MARK).await.unwrap();
    store
        .mark_sent(&ids(&[&fresh]), stored_at + FIFTEEN_MINUTES_MS - 30_000)
        .await
        .unwrap();

    let ended = store
        .return_unanswered(&ids(&[&post, &fresh]), stored_at + FIFTEEN_MINUTES_MS)
        .await
        .unwrap()
        .expect_clean();
    let overdue_waiting = store
        .end_overdue(stored_at + FIFTEEN_MINUTES_MS)
        .await
        .unwrap()
        .expect_clean();
    let claimed = store
        .claim_next_batch(
            stored_at + FIFTEEN_MINUTES_MS + 60_000,
            64,
            stored_at + FIFTEEN_MINUTES_MS,
        )
        .await
        .unwrap()
        .expect_clean();

    assert_eq!(
        ended
            .iter()
            .map(|write| (write.request_id.clone(), write.reason))
            .collect::<Vec<_>>(),
        vec![(
            post.request.clone(),
            OutboxCancellation::NotConfirmedWithin15Minutes
        )]
    );
    assert!(overdue_waiting.is_empty(), "nothing else is overdue");
    assert_eq!(
        store
            .request_status(&post.caller, &post.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes
        }
    );
    assert_eq!(
        claimed
            .writes
            .iter()
            .map(|w| w.request.clone())
            .collect::<Vec<_>>(),
        ids(&[&fresh]),
        "the overdue post is never resent; the fresh one is"
    );
    assert!(store.sent_without_answer().await.unwrap().expect_clean() == ids(&[&fresh]));
}
/// A direct message goes out with its row's creation stamp as sent_at,
/// whatever the submitter wrote, and every replay carries the same value
/// (design 2026-11-17); a thread post is sent exactly as stored.
pub(crate) async fn a_direct_message_goes_out_with_its_creation_stamp_as_sent_at(
    store: &mut dyn OutboxStorage,
) {
    let stored_at = MARK - 7_000;
    let direct = message(90, "stamped", MARK);
    let post = item(91, "as stored", MARK);
    store.insert(&direct, stored_at).await.unwrap();
    store.insert(&post, stored_at).await.unwrap();

    let first = store
        .claim_next_batch(MARK, 64, MARK)
        .await
        .unwrap()
        .expect_clean()
        .writes;
    store
        .return_unanswered(&ids(&[&direct, &post]), MARK + 1_000)
        .await
        .unwrap()
        .expect_clean();
    let replay = store
        .claim_next_batch(MARK + 60_000, 64, MARK + 60_000)
        .await
        .unwrap()
        .expect_clean()
        .writes;

    let sent_at = |writes: &[StoredOutboxWrite]| {
        writes
            .iter()
            .find(|write| write.request == direct.request)
            .map(|write| write.sent_at.timestamp_millis())
    };
    assert_eq!(
        sent_at(&first),
        Some(stored_at),
        "the outbox row supplies the creation stamp"
    );
    assert_eq!(
        sent_at(&replay),
        Some(stored_at),
        "the replay carries the same value"
    );
    for writes in [&first, &replay] {
        let sent_post = writes
            .iter()
            .find(|write| write.request == post.request)
            .unwrap();
        assert_eq!(sent_post.payload, post.payload.clone());
    }
}

pub(crate) async fn replay_keeps_the_original_request_id_and_deadline(
    store: &mut dyn OutboxStorage,
) {
    let created_at = MARK - 1_000;
    let original_deadline = created_at + FIFTEEN_MINUTES_MS;
    let write = message(93, "same request throughout", MARK);
    let request_ids = ids(&[&write]);

    store.insert(&write, created_at).await.unwrap();
    assert_eq!(
        store.earliest_deadline().await.unwrap().expect_clean(),
        Some(original_deadline)
    );

    let first = store.mark_sent(&request_ids, MARK).await.unwrap();
    assert_eq!(first[0].request, write.request);
    assert!(
        store
            .return_unanswered(&request_ids, MARK + 1_000)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    assert_eq!(
        store.waiting().await.unwrap().expect_clean(),
        [(MARK, write.request.clone())]
    );
    assert_eq!(
        store.earliest_deadline().await.unwrap().expect_clean(),
        Some(original_deadline)
    );

    let replay = store
        .mark_sent(&request_ids, original_deadline - 1)
        .await
        .unwrap();
    assert_eq!(replay, first);
    let ended = store
        .return_unanswered(&request_ids, original_deadline)
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].request_id, write.request);
    assert_eq!(
        ended[0].reason,
        OutboxCancellation::NotConfirmedWithin15Minutes
    );
    assert!(store.waiting().await.unwrap().expect_clean().is_empty());
}
