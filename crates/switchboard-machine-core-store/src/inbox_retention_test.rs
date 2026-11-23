use super::{
    InboxDecision, InboxStore, InboxStoreError, MachineLocalStore, RecordDelivery, clock_millis,
    fixture_expiry, message,
};
use router_clock::ManualClock;
use std::{path::Path, time::Duration};
use workspace_model::Timestamp;

const SIXTY_DAYS: Duration = Duration::from_secs(60 * 24 * 60 * 60);

async fn deliver(store: &mut InboxStore, index: u8, arrived_ms: i64) {
    let saved = store
        .record_delivery(
            &message(index),
            InboxDecision::Accepted,
            arrived_ms,
            fixture_expiry(arrived_ms),
        )
        .await
        .unwrap();
    assert_eq!(saved, RecordDelivery::Inserted);
}

async fn open_inbox(path: &Path) -> InboxStore {
    InboxStore::new(MachineLocalStore::open(path).await.unwrap())
}

#[tokio::test]
async fn handoff_completion_preserves_message_text_without_leaving_a_pending_handoff() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = ManualClock::new(Timestamp::UNIX_EPOCH);
    let mut store = open_inbox(&path).await;
    let delivery = message(0);
    let arrived_ms = clock_millis(&clock);
    deliver(&mut store, 0, arrived_ms).await;
    clock.advance(Duration::from_secs(1)).unwrap();
    let handed_at = clock_millis(&clock);
    store
        .record_handoff(&delivery.message_id, handed_at)
        .await
        .unwrap();

    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let stored: Option<String> =
        sqlx::query_scalar("SELECT message_json FROM inbox_messages WHERE message_id = ?")
            .bind(delivery.message_id.as_str())
            .fetch_one(&mut database.connection)
            .await
            .unwrap();
    assert_eq!(
        stored,
        Some(serde_json::to_string(&delivery).unwrap()),
        "hand-off completion must keep the exact payload"
    );
    drop(store);
    let mut reopened = open_inbox(&path).await;
    assert_eq!(
        reopened
            .message_for(&delivery.message_id)
            .await
            .unwrap()
            .expect_clean(),
        Some(delivery.clone())
    );
    assert!(
        reopened
            .pending_handoffs()
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    assert_eq!(
        reopened
            .pending_receipts()
            .await
            .unwrap()
            .expect_clean()
            .len(),
        1
    );
    assert!(matches!(
        reopened
            .record_handoff(&delivery.message_id, handed_at)
            .await,
        Err(InboxStoreError::UnexpectedState { .. })
    ));
}

#[tokio::test]
async fn a_handed_over_and_receipt_confirmed_message_is_not_pruned_at_twenty_four_hours() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open_inbox(&root.path().join("machine_local.sqlite")).await;
    let clock = ManualClock::new(Timestamp::UNIX_EPOCH);
    let arrived_ms = clock_millis(&clock);
    let delivery = message(0);
    deliver(&mut store, 0, arrived_ms).await;
    store
        .record_handoff(&delivery.message_id, arrived_ms)
        .await
        .unwrap();
    store
        .mark_receipt_confirmed(&delivery.message_id, arrived_ms)
        .await
        .unwrap();
    clock.advance(Duration::from_secs(24 * 60 * 60)).unwrap();
    assert_eq!(
        store
            .prune_past_retention(clock_millis(&clock))
            .await
            .unwrap()
            .expect_clean(),
        0,
        "twenty-four hours is not the retention limit"
    );
    clock.advance(Duration::from_millis(1)).unwrap();
    assert_eq!(
        store
            .prune_past_retention(clock_millis(&clock))
            .await
            .unwrap()
            .expect_clean(),
        0,
        "passing the old twenty-four-hour boundary must not prune a message"
    );
    assert_eq!(
        store
            .message_for(&delivery.message_id)
            .await
            .unwrap()
            .expect_clean(),
        Some(delivery)
    );
}

#[tokio::test]
async fn confirmed_read_and_unread_rows_are_pruned_exactly_sixty_days_from_arrival() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open_inbox(&root.path().join("machine_local.sqlite")).await;
    let clock = ManualClock::new(Timestamp::UNIX_EPOCH);
    let arrived_ms = clock_millis(&clock);
    let read = message(0);
    let unread = message(1);
    for index in 0..2 {
        deliver(&mut store, index, arrived_ms).await;
    }
    let handoff_delay = Duration::from_secs(60);
    clock.advance(handoff_delay).unwrap();
    let updated_ms = clock_millis(&clock);
    for delivery in [&read, &unread] {
        store
            .mark_receipt_confirmed(&delivery.message_id, updated_ms)
            .await
            .unwrap();
    }
    store
        .record_handoff(&read.message_id, updated_ms)
        .await
        .unwrap();
    clock
        .advance(SIXTY_DAYS - handoff_delay - Duration::from_millis(1))
        .unwrap();
    let before_cutoff = clock_millis(&clock);
    assert_eq!(
        store.next_prune_at_ms().await.unwrap().expect_clean(),
        Some(before_cutoff + 1),
        "retention is scheduled from arrival even after either local transition"
    );
    assert_eq!(
        store
            .prune_past_retention(before_cutoff)
            .await
            .unwrap()
            .expect_clean(),
        0
    );
    assert_eq!(
        store
            .record_delivery(
                &read,
                InboxDecision::Accepted,
                before_cutoff,
                fixture_expiry(before_cutoff)
            )
            .await
            .unwrap(),
        RecordDelivery::Duplicate
    );
    clock.advance(Duration::from_millis(1)).unwrap();
    let cutoff = clock_millis(&clock);
    assert_eq!(
        store
            .prune_past_retention(cutoff)
            .await
            .unwrap()
            .expect_clean(),
        2,
        "the original arrival retains both read and unread rows through the same boundary"
    );
    for delivery in [&read, &unread] {
        assert!(
            store
                .message_for(&delivery.message_id)
                .await
                .unwrap()
                .expect_clean()
                .is_none()
        );
    }
    assert_eq!(store.next_prune_at_ms().await.unwrap().expect_clean(), None);
    assert_eq!(
        store
            .record_delivery(
                &read,
                InboxDecision::Accepted,
                cutoff,
                fixture_expiry(cutoff)
            )
            .await
            .unwrap(),
        RecordDelivery::Inserted
    );
}

#[tokio::test]
async fn an_aged_message_is_pruned_only_after_its_owed_receipt_is_confirmed() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open_inbox(&root.path().join("machine_local.sqlite")).await;
    let clock = ManualClock::new(Timestamp::UNIX_EPOCH);
    let arrived_ms = clock_millis(&clock);
    let delivery = message(0);
    deliver(&mut store, 0, arrived_ms).await;
    store
        .record_handoff(&delivery.message_id, arrived_ms)
        .await
        .unwrap();
    clock.advance(SIXTY_DAYS).unwrap();
    let cutoff = clock_millis(&clock);
    assert_eq!(
        store.next_prune_at_ms().await.unwrap().expect_clean(),
        None,
        "an owed receipt must keep the aged row off the prune schedule"
    );
    assert_eq!(
        store
            .prune_past_retention(cutoff)
            .await
            .unwrap()
            .expect_clean(),
        0,
        "a Central receipt that is still owed protects an aged row"
    );
    assert_eq!(
        store
            .message_for(&delivery.message_id)
            .await
            .unwrap()
            .expect_clean(),
        Some(delivery.clone())
    );
    store
        .mark_receipt_confirmed(&delivery.message_id, cutoff)
        .await
        .unwrap();
    assert_eq!(
        store.next_prune_at_ms().await.unwrap().expect_clean(),
        Some(cutoff)
    );
    assert_eq!(
        store
            .prune_past_retention(cutoff)
            .await
            .unwrap()
            .expect_clean(),
        1,
        "confirming a late receipt does not restart the retention window"
    );
}
