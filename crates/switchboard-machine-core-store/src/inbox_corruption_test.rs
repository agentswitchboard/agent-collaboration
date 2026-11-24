use super::{InboxStore, MachineLocalStore, message};
use crate::{RowCorruption, RowCorruptionReason, StoreScan};
use workspace_model::central_mail::MessageRecord;

const COMPLETE: &str = r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"confirmed"},"handoff":{"kind":"completed"},"family":"direct"}"#;
const PENDING: &str = r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"handoff":{"kind":"pending"},"family":"direct"}"#;
const BROKEN: &str = r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"handoff":{"kind":"notRequired"},"family":"direct"}"#;
const RETENTION: i64 = 60 * 24 * 60 * 60 * 1_000;

async fn plant_messages(
    database: &mut MachineLocalStore,
    with_completed_rows: bool,
) -> Vec<MessageRecord> {
    let mut messages = Vec::new();
    for index in 0..5 {
        let event = message(index);
        let corrupted = index % 2 == 0;
        let completed = with_completed_rows && !corrupted;
        let payload = serde_json::to_string(&event).unwrap();
        let state = if corrupted {
            BROKEN
        } else if completed {
            COMPLETE
        } else {
            PENDING
        };
        sqlx::query(
            "INSERT INTO inbox_messages
             (message_id, message_json, delivery_state_json, created_at, updated_at, expires_at)
             VALUES (?, ?, ?, 0, 0, 60000)",
        )
        .bind(event.message_id.as_str())
        .bind(payload)
        .bind(state)
        .execute(&mut database.connection)
        .await
        .unwrap();
        messages.push(event);
    }
    messages
}

fn expected_faults(messages: &[MessageRecord]) -> Vec<RowCorruption> {
    messages
        .iter()
        .step_by(2)
        .map(|message| RowCorruption {
            key: message.message_id.to_string(),
            field: "delivery_state_json.handoff",
            reason: RowCorruptionReason::InconsistentState,
        })
        .collect()
}

#[tokio::test]
async fn inbox_reads_keep_healthy_handoffs_and_decisions_around_corruption() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let messages = plant_messages(&mut database, false).await;
    let mut inbox = InboxStore::new(database);

    let handoffs = inbox.pending_handoffs().await.unwrap();
    assert_eq!(handoffs.healthy, [messages[1].clone(), messages[3].clone()]);
    assert_eq!(handoffs.corrupt_rows, expected_faults(&messages));
    let receipts = inbox.pending_receipts().await.unwrap();
    assert_eq!(
        receipts
            .healthy
            .iter()
            .map(|receipt| &receipt.message_id)
            .collect::<Vec<_>>(),
        [&messages[1].message_id, &messages[3].message_id]
    );
    assert_eq!(receipts.corrupt_rows, expected_faults(&messages));

    let duplicate = inbox.receipt_for(&messages[0].message_id).await.unwrap();
    let first_fault = expected_faults(&messages).remove(0);
    assert_eq!(
        duplicate,
        StoreScan {
            healthy: None,
            corrupt_rows: vec![first_fault]
        }
    );
}

#[tokio::test]
async fn inbox_retention_removes_healthy_completed_rows_and_keeps_corrupt_custody() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let messages = plant_messages(&mut database, true).await;
    let mut inbox = InboxStore::new(database);

    let next = inbox.next_prune_at_ms().await.unwrap();
    assert_eq!(next.healthy, Some(RETENTION));
    assert_faults(next.corrupt_rows, &messages);
    let scan = inbox.prune_past_retention(RETENTION).await.unwrap();
    assert_eq!(scan.healthy, 2);
    assert_faults(scan.corrupt_rows, &messages);
    let repeated = inbox.prune_past_retention(RETENTION).await.unwrap();
    assert_eq!(repeated.healthy, 0);
    assert_faults(repeated.corrupt_rows, &messages);

    let mut reopened = MachineLocalStore::open(&path).await.unwrap();
    let retained: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT message_id, message_json, delivery_state_json, updated_at
         FROM inbox_messages ORDER BY message_id",
    )
    .fetch_all(&mut reopened.connection)
    .await
    .unwrap();
    let expected: Vec<_> = messages
        .iter()
        .step_by(2)
        .map(|event| {
            (
                event.message_id.to_string(),
                serde_json::to_string(event).unwrap(),
                BROKEN.to_owned(),
                0,
            )
        })
        .collect();
    assert_eq!(retained, expected);
}

fn assert_faults(mut faults: Vec<RowCorruption>, messages: &[MessageRecord]) {
    faults.sort_by(|first, second| first.key.cmp(&second.key));
    assert_eq!(faults, expected_faults(messages));
}

#[tokio::test]
async fn invalid_utf8_inbox_decision_does_not_hide_healthy_receipts_or_handoffs() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let mut events = Vec::new();
    for index in 0..2 {
        let event = message(index);
        sqlx::query(
            "INSERT INTO inbox_messages
             (message_id, message_json, delivery_state_json, created_at, updated_at, expires_at)
             VALUES (?, ?, ?, 0, 0, 60000)",
        )
        .bind(event.message_id.as_str())
        .bind(serde_json::to_string(&event).unwrap())
        .bind(PENDING)
        .execute(&mut database.connection)
        .await
        .unwrap();
        events.push(event);
    }
    sqlx::query("UPDATE inbox_messages SET delivery_state_json = CAST(x'80FF' AS TEXT) WHERE message_id = ?")
        .bind(events[0].message_id.as_str()).execute(&mut database.connection).await.unwrap();
    let mut inbox = InboxStore::new(database);

    let handoffs = inbox.pending_handoffs().await.unwrap();
    assert_eq!(handoffs.healthy, std::slice::from_ref(&events[1]));
    let diagnostic = || RowCorruption {
        key: events[0].message_id.to_string(),
        field: "delivery_state_json",
        reason: RowCorruptionReason::InvalidJson,
    };
    assert_eq!(handoffs.corrupt_rows, [diagnostic()]);
    let receipts = inbox.pending_receipts().await.unwrap();
    assert_eq!(receipts.healthy[0].message_id, events[1].message_id);
    assert_eq!(receipts.corrupt_rows, [diagnostic()]);
}

#[tokio::test]
async fn non_integer_receipt_time_does_not_turn_into_a_healthy_delivery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let event = message(0);
    sqlx::query(
        "INSERT INTO inbox_messages
         (message_id, message_json, delivery_state_json, created_at, updated_at, expires_at)
         VALUES (?, ?, ?, 'invalid', 0, 60000)",
    )
    .bind(event.message_id.as_str())
    .bind(serde_json::to_string(&event).unwrap())
    .bind(PENDING)
    .execute(&mut database.connection)
    .await
    .unwrap();

    let mut inbox = InboxStore::new(database);
    let receipts = inbox.pending_receipts().await.unwrap();
    assert!(receipts.healthy.is_empty());
    assert_eq!(
        receipts.corrupt_rows,
        [RowCorruption {
            key: event.message_id.to_string(),
            field: "created_at",
            reason: RowCorruptionReason::InvalidStorageType,
        }]
    );
}

#[tokio::test]
async fn corrupt_expiry_is_reported_and_kept_out_of_healthy_inbox_work() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    for index in 0..3 {
        let event = message(index);
        sqlx::query(
            "INSERT INTO inbox_messages
             (message_id, message_json, delivery_state_json, created_at, updated_at, expires_at)
             VALUES (?, ?, ?, 0, 0, 60000)",
        )
        .bind(event.message_id.as_str())
        .bind(serde_json::to_string(&event).unwrap())
        .bind(COMPLETE)
        .execute(&mut database.connection)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE inbox_messages SET expires_at = ? WHERE message_id = ?")
        .bind(i64::MAX)
        .bind(message(0).message_id.as_str())
        .execute(&mut database.connection)
        .await
        .unwrap();
    sqlx::query("UPDATE inbox_messages SET expires_at = 'invalid' WHERE message_id = ?")
        .bind(message(1).message_id.as_str())
        .execute(&mut database.connection)
        .await
        .unwrap();

    let mut inbox = InboxStore::new(database);
    let mut faults = Vec::new();
    for (index, reason) in [
        (0, RowCorruptionReason::TimestampOutOfRange),
        (1, RowCorruptionReason::InvalidStorageType),
    ] {
        let id = message(index).message_id;
        let scan = inbox.expiry_for(&id).await.unwrap();
        let problem = RowCorruption {
            key: id.to_string(),
            field: "expires_at",
            reason,
        };
        assert_eq!(
            scan,
            StoreScan {
                healthy: None,
                corrupt_rows: vec![problem]
            },
            "corrupt expires_at must be reported, never coerced"
        );
        faults.extend(scan.corrupt_rows);
    }
    let mut read = inbox.next_prune_at_ms().await.unwrap();
    assert_eq!(read.healthy, Some(RETENTION));
    read.corrupt_rows
        .sort_by(|first, second| first.key.cmp(&second.key));
    assert_eq!(read.corrupt_rows, faults);
    let mut pruned = inbox.prune_past_retention(RETENTION).await.unwrap();
    pruned
        .corrupt_rows
        .sort_by(|first, second| first.key.cmp(&second.key));
    assert_eq!(pruned.healthy, 1);
    assert_eq!(pruned.corrupt_rows, faults);
    let mut reopened = MachineLocalStore::open(&path).await.unwrap();
    let retained: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox_messages")
        .fetch_one(&mut reopened.connection)
        .await
        .unwrap();
    assert_eq!(retained, 2);
}
