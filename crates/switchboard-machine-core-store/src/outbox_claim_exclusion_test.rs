use super::{FIFTEEN_MINUTES_MS, MARK, ids, item, message, open};
use crate::{MACHINE_LOCAL_DATABASE, MachineLocalOutbox, MachineLocalStore, OutboxCancellation};
use crate::{OutboxRequestStatus, OutboxStorage};
use router_clock::{ManualClock, RuntimeClock};
use std::{collections::BTreeSet, time::Duration};

#[tokio::test]
async fn excluded_due_rows_do_not_take_slots_or_block_rows_behind_them() {
    let (directory, mut setup) = open().await;
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let now = clock.utc_now().timestamp_millis();
    let ahead = item(501, "excluded ahead of eligible work", now);
    let first = item(502, "first eligible", now);
    let boundary = item(503, "excluded at the second slot", now);
    let second = item(504, "second eligible", now);
    let behind = item(505, "excluded behind the first batch", now);
    let third = item(506, "third eligible", now);
    for write in [&ahead, &first, &boundary, &second, &behind, &third] {
        setup.insert(write, now).await.unwrap();
    }
    drop(setup);
    let excluded = BTreeSet::from([
        ahead.request.clone(),
        boundary.request.clone(),
        behind.request.clone(),
    ]);
    let mut store = MachineLocalOutbox::new(
        MachineLocalStore::open(&directory.path().join(MACHINE_LOCAL_DATABASE))
            .await
            .unwrap(),
    );

    for expected in [ids(&[&first, &second]), ids(&[&third])] {
        let batch = store
            .claim_next_batch(now, 2, now, now, &excluded)
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
            expected
        );
    }
    for write in [&ahead, &boundary, &behind] {
        assert_eq!(
            store
                .request_status(&write.caller, &write.request)
                .await
                .unwrap(),
            OutboxRequestStatus::InOutbox { flush_at_ms: now },
        );
    }
}

#[tokio::test]
async fn an_excluded_waiting_row_is_left_for_ordinary_expiry() {
    let (directory, mut setup) = open().await;
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let waiting = message(507, "earlier attempt may have left", MARK);
    let expired = message(508, "unprotected expiry", MARK);
    let eligible = message(509, "later deadline", MARK);
    setup.insert(&waiting, MARK - 1_000).await.unwrap();
    setup.insert(&expired, MARK - 1_000).await.unwrap();
    setup.insert(&eligible, MARK).await.unwrap();
    let now = clock.utc_now().timestamp_millis();
    let early = setup
        .claim_next_batch(now, 1, now, now, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(early.writes[0].request, waiting.request);
    assert!(
        setup
            .return_unanswered(&ids(&[&waiting]), now)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    drop(setup);

    clock
        .advance(Duration::from_millis(
            (FIFTEEN_MINUTES_MS - 999).try_into().unwrap(),
        ))
        .unwrap();
    let now = clock.utc_now().timestamp_millis();
    let excluded = BTreeSet::from([waiting.request.clone()]);
    let mut store = MachineLocalOutbox::new(
        MachineLocalStore::open(&directory.path().join(MACHINE_LOCAL_DATABASE))
            .await
            .unwrap(),
    );
    let batch = store
        .claim_next_batch(now, 1, now, now, &excluded)
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(batch.writes[0].request, eligible.request);
    assert_eq!(batch.ended.len(), 1);
    assert_eq!(batch.ended[0].request_id, expired.request);
    assert_eq!(
        batch.ended[0].reason,
        OutboxCancellation::NotSentWithin15Minutes
    );
    assert_eq!(
        store
            .request_status(&waiting.caller, &waiting.request)
            .await
            .unwrap(),
        OutboxRequestStatus::InOutbox { flush_at_ms: MARK },
    );
    assert!(
        store
            .end_overdue_orphans(now, &BTreeSet::new())
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    let ended = store.end_overdue(now).await.unwrap().expect_clean();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0].request_id, waiting.request);
    assert_eq!(
        ended[0].reason,
        OutboxCancellation::NotConfirmedWithin15Minutes
    );
}
