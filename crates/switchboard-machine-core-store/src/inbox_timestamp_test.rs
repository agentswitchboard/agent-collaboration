use super::{
    InboxDecision, InboxStore, InboxStoreError, MachineLocalStore, fixture_expiry, message,
};

type InboxClockSnapshot = (String, Option<String>, String, i64, i64);

async fn snapshot(database: &mut MachineLocalStore) -> Vec<InboxClockSnapshot> {
    sqlx::query_as(
        "SELECT message_id, message_json, delivery_state_json, created_at, updated_at
         FROM inbox_messages ORDER BY message_id",
    )
    .fetch_all(&mut database.connection)
    .await
    .unwrap()
}

#[tokio::test]
async fn invalid_incoming_inbox_creation_clock_admits_no_message() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("machine_local.sqlite");
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let event = message(0);
    for invalid in [i64::MIN, i64::MAX] {
        assert!(matches!(
            inbox
                .record_delivery(
                    &event,
                    InboxDecision::Accepted,
                    invalid,
                    fixture_expiry(invalid)
                )
                .await,
            Err(InboxStoreError::InvalidTimestamp {
                field: "created_at"
            })
        ));
    }
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    assert!(snapshot(&mut database).await.is_empty());
}

#[tokio::test]
async fn invalid_incoming_inbox_transition_clocks_leave_pending_custody_intact() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("machine_local.sqlite");
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let event = message(0);
    inbox
        .record_delivery(&event, InboxDecision::Accepted, 0, fixture_expiry(0))
        .await
        .unwrap();
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let before = snapshot(&mut database).await;

    assert!(matches!(
        inbox
            .mark_receipt_confirmed(&event.message_id, i64::MAX)
            .await,
        Err(InboxStoreError::InvalidTimestamp {
            field: "updated_at"
        })
    ));
    assert!(matches!(
        inbox.record_handoff(&event.message_id, i64::MAX).await,
        Err(InboxStoreError::InvalidTimestamp {
            field: "updated_at"
        })
    ));
    assert!(matches!(
        inbox.prune_past_retention(i64::MAX).await,
        Err(InboxStoreError::InvalidTimestamp { field: "now_ms" })
    ));
    assert_eq!(snapshot(&mut database).await, before);
}
