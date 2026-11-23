use super::{FIFTEEN_MINUTES_MS, MARK, ids, item, message, open};
use crate::{OutboxCancellation, OutboxRequestStatus, OutboxStorage, OutboxStoreError};
use router_clock::{ManualClock, RuntimeClock};
use std::time::Duration;

#[tokio::test]
async fn claim_threshold_skips_short_deadlines_without_consuming_the_limit() {
    let (_directory, mut store) = open().await;
    let threshold = MARK + 5_000;
    let short = item(101, "deadline before threshold", MARK);
    let equal = item(102, "deadline at threshold", MARK);
    let first = item(103, "first eligible", MARK);
    let second = item(104, "second eligible", MARK);
    let third = item(105, "third eligible", MARK);
    for (write, offset) in [
        (&short, -1),
        (&equal, 0),
        (&first, 1),
        (&second, 2),
        (&third, 3),
    ] {
        store
            .insert(write, threshold - FIFTEEN_MINUTES_MS + offset)
            .await
            .unwrap();
    }

    let batch = store
        .claim_next_batch(MARK, 2, MARK, threshold)
        .await
        .unwrap()
        .expect_clean();

    assert!(batch.ended.is_empty());
    assert_eq!(
        batch
            .writes
            .into_iter()
            .map(|write| write.request)
            .collect::<Vec<_>>(),
        ids(&[&first, &second]),
    );
    for write in [&short, &equal, &third] {
        assert_eq!(
            store
                .request_status(&write.caller, &write.request)
                .await
                .unwrap(),
            OutboxRequestStatus::InOutbox { flush_at_ms: MARK },
        );
    }
    assert_eq!(
        store.earliest_deadline().await.unwrap().expect_clean(),
        Some(threshold - 1)
    );
}

#[tokio::test]
async fn claim_threshold_leaves_both_departure_states_for_expiry_at_the_real_deadline() {
    let (_directory, mut store) = open().await;
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let deadline = MARK + 1_000;
    let created_at = deadline - FIFTEEN_MINUTES_MS;
    let lost = message(106, "earlier attempt may have left", MARK);
    let unsent = message(107, "never left", MARK);
    for write in [&lost, &unsent] {
        store.insert(write, created_at).await.unwrap();
    }
    let now_ms = clock.utc_now().timestamp_millis();
    let first = store
        .claim_next_batch(MARK, 1, now_ms, now_ms)
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(first.writes[0].request, lost.request);
    clock.advance(Duration::from_millis(1)).unwrap();
    let now_ms = clock.utc_now().timestamp_millis();
    assert!(
        store
            .return_unanswered(&ids(&[&lost]), now_ms)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );

    let blocked = store
        .claim_next_batch(MARK, 2, now_ms, deadline)
        .await
        .unwrap()
        .expect_clean();
    assert!(blocked.writes.is_empty());
    assert!(blocked.ended.is_empty());
    assert_eq!(
        store.earliest_deadline().await.unwrap().expect_clean(),
        Some(deadline)
    );
    clock.advance(Duration::from_millis(998)).unwrap();
    assert!(
        store
            .end_overdue(clock.utc_now().timestamp_millis())
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    clock.advance(Duration::from_millis(1)).unwrap();
    let ended = store
        .end_overdue(clock.utc_now().timestamp_millis())
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(
        ended
            .into_iter()
            .map(|write| (write.request_id, write.reason))
            .collect::<Vec<_>>(),
        [
            (
                lost.request,
                OutboxCancellation::NotConfirmedWithin15Minutes
            ),
            (unsent.request, OutboxCancellation::NotSentWithin15Minutes),
        ],
    );
}

#[tokio::test]
async fn claim_threshold_equal_to_now_keeps_normal_claim_and_expiry_boundaries() {
    let (_directory, mut store) = open().await;
    let expired = item(108, "deadline now", MARK);
    let due = item(109, "one millisecond of time remaining", MARK);
    let future_mark = item(110, "live but later mark", MARK + 60_000);
    store
        .insert(&expired, MARK - FIFTEEN_MINUTES_MS)
        .await
        .unwrap();
    for write in [&due, &future_mark] {
        store
            .insert(write, MARK - FIFTEEN_MINUTES_MS + 1)
            .await
            .unwrap();
    }

    let batch = store
        .claim_next_batch(MARK, 3, MARK, MARK)
        .await
        .unwrap()
        .expect_clean();

    assert_eq!(
        batch
            .writes
            .into_iter()
            .map(|write| write.request)
            .collect::<Vec<_>>(),
        ids(&[&due])
    );
    assert_eq!(
        batch
            .ended
            .into_iter()
            .map(|write| (write.request_id, write.reason))
            .collect::<Vec<_>>(),
        [(expired.request, OutboxCancellation::NotSentWithin15Minutes)],
    );
    assert_eq!(
        store
            .request_status(&future_mark.caller, &future_mark.request)
            .await
            .unwrap(),
        OutboxRequestStatus::InOutbox {
            flush_at_ms: MARK + 60_000
        },
    );
}

#[tokio::test]
async fn invalid_claim_threshold_returns_typed_error_before_any_expiry_or_claim() {
    let (_directory, mut store) = open().await;
    let expired = item(111, "expiry still pending", MARK);
    let due = item(112, "claim still pending", MARK);
    store
        .insert(&expired, MARK - FIFTEEN_MINUTES_MS)
        .await
        .unwrap();
    store.insert(&due, MARK).await.unwrap();

    assert!(matches!(
        store.claim_next_batch(MARK, 2, MARK, MARK - 1).await,
        Err(OutboxStoreError::ClaimDeadlineThresholdBeforeNow {
            required_deadline_after_ms, now_ms: MARK,
        }) if required_deadline_after_ms == MARK - 1
    ));
    assert!(matches!(
        store.claim_next_batch(MARK, 2, MARK, i64::MAX).await,
        Err(OutboxStoreError::InvalidTimestamp {
            field: "required_deadline_after_ms"
        })
    ));
    for write in [&expired, &due] {
        assert_eq!(
            store
                .request_status(&write.caller, &write.request)
                .await
                .unwrap(),
            OutboxRequestStatus::InOutbox { flush_at_ms: MARK },
        );
    }
}
