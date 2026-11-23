use super::{InboxStore, MachineLocalStore, message};
use crate::{RowCorruption, RowCorruptionReason, StoreScan};
use workspace_model::central_mail::MessageRecord;

const COMPLETE: &str =
    r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"confirmed"},"ended_at_ms":0}"#;
const PENDING: &str =
    r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"ended_at_ms":null}"#;
const BROKEN: &str =
    r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"ended_at_ms":0}"#;
const RETENTION: i64 = 24 * 60 * 60 * 1_000;

async fn plant_messages(
    database: &mut MachineLocalStore,
    with_completed_rows: bool,
) -> Vec<MessageRecord> {
    let mut messages = Vec::new();
    for index in 0..5 {
        let event = message(index);
        let corrupted = index % 2 == 0;
        let completed = with_completed_rows && !corrupted;
        let payload = (!completed).then(|| serde_json::to_string(&event).unwrap());
        let state = if corrupted {
            BROKEN
        } else if completed {
            COMPLETE
        } else {
            PENDING
        };
        sqlx::query(
            "INSERT INTO inbox_messages
             (message_id, message_json, delivery_state_json, created_at, updated_at)
             VALUES (?, ?, ?, 0, 0)",
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
            field: "delivery_state_json.ended_at_ms",
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
    assert_eq!(next.healthy, Some(RETENTION + 1));
    assert_faults(next.corrupt_rows, &messages);
    let scan = inbox.prune_ended(RETENTION + 1).await.unwrap();
    assert_eq!(scan.healthy, 2);
    assert_faults(scan.corrupt_rows, &messages);
    let repeated = inbox.prune_ended(RETENTION + 1).await.unwrap();
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
