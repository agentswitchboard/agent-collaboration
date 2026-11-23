use super::{MARK, RETENTION_MS, accepted_answer, caller, item, open};
use crate::{
    MacOutbox, MachineLocalStore, NewOutboxItem, OutboxRequestKey, OutboxRequestStatus,
    OutboxStoreError, RowCorruption, RowCorruptionReason, StoredSenderOutcome,
};
use workspace_model::{Timestamp, central_mail::MessageOutcome};

const CREATED: i64 = MARK - 1_000;
const DEADLINE: i64 = CREATED + 15 * 60 * 1_000;

/// Injects raw SQL rows without validating through the outbox admission path.
async fn plant_row(
    database: &mut MachineLocalStore,
    write: &NewOutboxItem,
    state: &str,
    payload: String,
    outcome: Option<String>,
    deadline: i64,
) {
    sqlx::query(
        "INSERT INTO outbox_items
         (request_id, caller_json, payload_json, item_state, flush_at_ms, outcome_json,
          created_at, expires_at_ms, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(write.request.as_str())
    .bind(serde_json::to_string(&write.caller).unwrap())
    .bind(payload)
    .bind(state)
    .bind(write.flush_at_ms)
    .bind(outcome)
    .bind(CREATED)
    .bind(deadline)
    .bind(MARK)
    .execute(&mut database.connection)
    .await
    .unwrap();
}

fn body_free_payload(write: &NewOutboxItem) -> String {
    serde_json::json!({
        "requestId": write.request,
        "target": write.payload.content().unwrap().target,
        "contentDigest": "0".repeat(64),
    })
    .to_string()
}

type RawOutboxSnapshot = (String, String, Option<String>, i64, String, i64);

async fn rows(database: &mut MachineLocalStore) -> Vec<RawOutboxSnapshot> {
    sqlx::query_as(
        "SELECT item_state, payload_json, outcome_json, expires_at_ms, request_id, updated_at
         FROM outbox_items ORDER BY request_id",
    )
    .fetch_all(&mut database.connection)
    .await
    .unwrap()
}

const DAMAGED_DEADLINE: i64 = DEADLINE + 60_000;

fn assert_corrupt_deadlines(corruption: Vec<RowCorruption>, writes: &[NewOutboxItem]) {
    let keys: Vec<_> = writes
        .iter()
        .step_by(2)
        .map(|write| write.request.as_str())
        .collect();
    assert_eq!(corruption.len(), keys.len());
    for (row, key) in corruption.iter().zip(keys) {
        assert_eq!(row.key, key);
        assert_eq!(row.field, "expires_at_ms");
        assert_eq!(
            row.reason,
            RowCorruptionReason::DeadlineMismatch {
                expected_ms: DEADLINE,
                stored_ms: DAMAGED_DEADLINE,
            }
        );
    }
}

async fn plant_interleaved_writes(
    database: &mut MachineLocalStore,
    state: &str,
) -> Vec<NewOutboxItem> {
    let mut writes = Vec::new();
    for index in 0..7 {
        // The final damaged row is past the mark as well as the claim limit.
        let mark = if index == 6 { MARK + 60_000 } else { MARK };
        let write = item(130 + index, "retained outbox body", mark);
        let deadline = if index % 2 == 0 {
            DAMAGED_DEADLINE
        } else {
            DEADLINE
        };
        plant_row(
            database,
            &write,
            state,
            serde_json::to_string(&write.payload).unwrap(),
            None,
            deadline,
        )
        .await;
        writes.push(write);
    }
    writes
}

async fn damaged_rows(database: &mut MachineLocalStore) -> Vec<RawOutboxSnapshot> {
    rows(database)
        .await
        .into_iter()
        .filter(|row| row.3 == DAMAGED_DEADLINE)
        .collect()
}

#[tokio::test]
async fn claim_fills_its_limit_with_healthy_rows_and_reports_every_damaged_candidate() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let writes = plant_interleaved_writes(&mut database, "inOutbox").await;
    let damaged = damaged_rows(&mut database).await;
    let mut outbox = MacOutbox::new(&mut database);

    let first = outbox.claim_next_batch(MARK, 2, MARK).await.unwrap();
    assert!(first.healthy.ended.is_empty());
    assert_eq!(
        first
            .healthy
            .writes
            .iter()
            .map(|write| &write.request)
            .collect::<Vec<_>>(),
        [&writes[1].request, &writes[3].request]
    );
    assert_corrupt_deadlines(first.corrupt_rows, &writes);
    let next = outbox.claim_next_batch(MARK, 2, MARK).await.unwrap();
    assert_eq!(
        next.healthy
            .writes
            .iter()
            .map(|write| &write.request)
            .collect::<Vec<_>>(),
        [&writes[5].request]
    );
    assert_corrupt_deadlines(next.corrupt_rows, &writes);
    assert_eq!(damaged_rows(&mut database).await, damaged);
}

#[tokio::test]
async fn expiry_ends_every_healthy_due_write_and_keeps_corrupt_rows_intact() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let writes = plant_interleaved_writes(&mut database, "inOutbox").await;
    let damaged = damaged_rows(&mut database).await;
    let mut outbox = MacOutbox::new(&mut database);

    let expired = outbox.end_overdue(DEADLINE).await.unwrap();
    assert_eq!(
        expired
            .healthy
            .iter()
            .map(|write| &write.request_id)
            .collect::<Vec<_>>(),
        [&writes[1].request, &writes[3].request, &writes[5].request]
    );
    assert!(
        expired
            .healthy
            .iter()
            .all(|write| write.reason == crate::OutboxCancellation::NotSentWithin15Minutes)
    );
    assert_corrupt_deadlines(expired.corrupt_rows, &writes);
    let repeated = outbox.end_overdue(DEADLINE).await.unwrap();
    assert!(repeated.healthy.is_empty());
    assert_corrupt_deadlines(repeated.corrupt_rows, &writes);
    assert_eq!(damaged_rows(&mut database).await, damaged);
}

#[tokio::test]
async fn scheduling_uses_healthy_marks_and_deadlines_despite_damaged_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let writes = plant_interleaved_writes(&mut database, "inOutbox").await;
    let mut outbox = MacOutbox::new(&mut database);

    let deadline = outbox.earliest_deadline().await.unwrap();
    assert_eq!(deadline.healthy, Some(DEADLINE));
    assert_corrupt_deadlines(deadline.corrupt_rows, &writes);
    let mark = outbox.earliest_waiting().await.unwrap();
    assert_eq!(mark.healthy, Some(MARK));
    assert_corrupt_deadlines(mark.corrupt_rows, &writes);
    let waiting = outbox.waiting().await.unwrap();
    assert_eq!(
        waiting.healthy,
        [1, 3, 5].map(|index| (MARK, writes[index].request.clone()))
    );
    assert_corrupt_deadlines(waiting.corrupt_rows, &writes);
}

#[tokio::test]
async fn orphan_recovery_returns_healthy_ids_around_corrupt_claims() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let writes = plant_interleaved_writes(&mut database, "sent").await;
    let recovery = MacOutbox::new(&mut database)
        .sent_without_answer()
        .await
        .unwrap();
    assert_eq!(
        recovery.healthy,
        [1, 3, 5].map(|index| writes[index].request.clone())
    );
    assert_corrupt_deadlines(recovery.corrupt_rows, &writes);
}

#[tokio::test]
async fn unanswered_returns_continue_past_corrupt_claims() {
    for now in [MARK, DEADLINE] {
        let root = tempfile::tempdir().unwrap();
        let mut database = open(&root).await;
        let writes = plant_interleaved_writes(&mut database, "sent").await;
        let damaged = damaged_rows(&mut database).await;
        let mut outbox = MacOutbox::new(&mut database);

        let ids: Vec<_> = writes.iter().map(|write| write.request.clone()).collect();
        let returned = outbox.return_unanswered(&ids, now).await.unwrap();
        assert_corrupt_deadlines(returned.corrupt_rows, &writes);
        if now == DEADLINE {
            assert_eq!(
                returned
                    .healthy
                    .iter()
                    .map(|write| &write.request_id)
                    .collect::<Vec<_>>(),
                [&writes[1].request, &writes[3].request, &writes[5].request]
            );
        } else {
            assert!(returned.healthy.is_empty());
            assert_eq!(outbox.waiting().await.unwrap().expect_clean().len(), 3);
        }
        assert_eq!(damaged_rows(&mut database).await, damaged);
    }
}

#[tokio::test]
async fn retention_prunes_healthy_rows_around_inconsistent_terminal_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let mut corrupt_keys = Vec::new();
    for index in 0..7 {
        let write = item(140 + index, "terminal fixture", MARK);
        let outcome = if index % 2 == 0 {
            corrupt_keys.push(write.request.to_string());
            None
        } else {
            Some(serde_json::to_string(&accepted_answer()).unwrap())
        };
        plant_row(
            &mut database,
            &write,
            "answered",
            body_free_payload(&write),
            outcome,
            DEADLINE,
        )
        .await;
    }
    let before: Vec<_> = rows(&mut database)
        .await
        .into_iter()
        .filter(|row| row.2.is_none())
        .collect();
    let mut outbox = MacOutbox::new(&mut database);

    let scanned = outbox
        .delete_ended_before_retention(MARK + RETENTION_MS)
        .await
        .unwrap();
    assert_eq!(scanned.healthy, 3);
    assert_eq!(scanned.corrupt_rows.len(), 4);
    for row in &scanned.corrupt_rows {
        assert_eq!(row.field, "outcome_json");
        assert_eq!(row.reason, RowCorruptionReason::InconsistentState);
    }
    let mut keys: Vec<_> = scanned
        .corrupt_rows
        .into_iter()
        .map(|row| row.key)
        .collect();
    keys.sort();
    assert_eq!(keys, corrupt_keys);
    assert_eq!(rows(&mut database).await, before);
}

#[tokio::test]
async fn corrupt_rows_are_hidden_from_foreign_status_and_sender_outcome_calls() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = item(115, "corrupt row", MARK);
    plant_row(
        &mut database,
        &write,
        "garbled",
        "{".to_owned(),
        None,
        DEADLINE,
    )
    .await;
    let key = OutboxRequestKey {
        request_id: write.request.clone(),
        scope: write.payload.request_scope(),
    };
    let outcome = StoredSenderOutcome {
        inbox_message_id: super::uuid_v7(116).parse().unwrap(),
        message_id: super::uuid_v7(117).parse().unwrap(),
        request_id: write.request.clone(),
        outcome: MessageOutcome::DeliveryNotConfirmed,
        reported_at: Timestamp::UNIX_EPOCH,
    };
    let foreign = caller(2);
    let mut outbox = MacOutbox::new(&mut database);

    assert_eq!(
        outbox
            .request_status(&foreign, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::NotFound
    );
    assert_eq!(
        outbox.scoped_request_status(&foreign, &key).await.unwrap(),
        OutboxRequestStatus::NotFound
    );
    assert_eq!(
        outbox
            .sender_outcome(&foreign, &write.request)
            .await
            .unwrap(),
        None
    );
    assert!(
        !outbox
            .record_sender_delivery(&foreign, &outcome, MARK)
            .await
            .unwrap()
    );
    for result in [
        outbox
            .request_status(&write.caller, &write.request)
            .await
            .map(|_| ()),
        outbox
            .scoped_request_status(&write.caller, &key)
            .await
            .map(|_| ()),
        outbox
            .sender_outcome(&write.caller, &write.request)
            .await
            .map(|_| ()),
        outbox
            .record_sender_delivery(&write.caller, &outcome, MARK)
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(OutboxStoreError::CorruptItem {
                field: "item_state",
                ..
            })
        ));
    }
}

#[tokio::test]
async fn row_decode_rejects_temporary_or_wrong_operation_settlements() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let transient = item(118, "transient", MARK);
    let wrong_operation = item(119, "wrong operation", MARK);
    for (write, reply) in [
        (
            &transient,
            r#"{"kind":"workspace","reply":{"result":{"kind":"outcomeUnknown"},"actors":[]}}"#
                .to_owned(),
        ),
        (
            &wrong_operation,
            serde_json::to_string(&crate::OutboxAccepted::SendMessage {
                message_id: super::uuid_v7(120).parse().unwrap(),
            })
            .unwrap(),
        ),
    ] {
        plant_row(
            &mut database,
            write,
            "answered",
            body_free_payload(write),
            Some(reply),
            DEADLINE,
        )
        .await;
    }

    let mut outbox = MacOutbox::new(&mut database);
    for write in [transient, wrong_operation] {
        assert!(matches!(
            outbox.request_status(&write.caller, &write.request).await,
            Err(OutboxStoreError::CorruptItem {
                field: "outcome_json",
                ..
            })
        ));
    }
}

#[tokio::test]
async fn claim_advances_past_an_entire_damaged_scan_page() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    for seed in 200..328 {
        let damaged = item(seed, "damaged scan page", MARK);
        plant_row(
            &mut database,
            &damaged,
            "inOutbox",
            serde_json::to_string(&damaged.payload).unwrap(),
            None,
            DAMAGED_DEADLINE,
        )
        .await;
    }
    let healthy = item(328, "first healthy row on next page", MARK);
    plant_row(
        &mut database,
        &healthy,
        "inOutbox",
        serde_json::to_string(&healthy.payload).unwrap(),
        None,
        DEADLINE,
    )
    .await;
    let behind = item(329, "future corrupted row", MARK + 60_000);
    plant_row(
        &mut database,
        &behind,
        "inOutbox",
        serde_json::to_string(&behind.payload).unwrap(),
        None,
        DAMAGED_DEADLINE,
    )
    .await;

    let scan = MacOutbox::new(&mut database)
        .claim_next_batch(MARK, 1, MARK)
        .await
        .unwrap();
    assert_eq!(scan.healthy.writes.len(), 1);
    assert_eq!(scan.healthy.writes[0].request, healthy.request);
    assert_eq!(scan.corrupt_rows.len(), 129);
    assert_eq!(scan.corrupt_rows[128].key, behind.request.to_string());
}

#[tokio::test]
async fn invalid_utf8_outbox_payload_is_a_row_fault_and_healthy_claims_progress() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let bad = item(450, "damaged bytes", MARK);
    let good = item(451, "healthy work", MARK);
    for write in [&bad, &good] {
        plant_row(
            &mut database,
            write,
            "inOutbox",
            serde_json::to_string(&write.payload).unwrap(),
            None,
            DEADLINE,
        )
        .await;
    }
    sqlx::query(
        "UPDATE outbox_items SET payload_json = CAST(x'80FF' AS TEXT) WHERE request_id = ?",
    )
    .bind(bad.request.as_str())
    .execute(&mut database.connection)
    .await
    .unwrap();

    let batch = MacOutbox::new(&mut database)
        .claim_next_batch(MARK, 1, MARK)
        .await
        .unwrap();
    assert_eq!(batch.healthy.writes[0].request, good.request);
    assert_eq!(
        batch.corrupt_rows,
        [RowCorruption {
            key: bad.request.to_string(),
            field: "payload_json",
            reason: RowCorruptionReason::InvalidJson,
        }]
    );
    let bytes: String =
        sqlx::query_scalar("SELECT hex(payload_json) FROM outbox_items WHERE request_id = ?")
            .bind(bad.request.as_str())
            .fetch_one(&mut database.connection)
            .await
            .unwrap();
    assert_eq!(bytes, "80FF");
}

#[tokio::test]
async fn non_integer_creation_stamp_does_not_turn_into_a_valid_expiry() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let bad = item(452, "wrong SQLite storage class", MARK);
    let good = item(453, "healthy work", MARK);
    for write in [&bad, &good] {
        plant_row(
            &mut database,
            write,
            "inOutbox",
            serde_json::to_string(&write.payload).unwrap(),
            None,
            DEADLINE,
        )
        .await;
    }
    // SQLite can read the text as zero. Give that coerced value an otherwise
    // self-consistent deadline to distinguish the physical validation.
    sqlx::query("UPDATE outbox_items SET created_at = 'invalid', expires_at_ms = 900000 WHERE request_id = ?")
        .bind(bad.request.as_str()).execute(&mut database.connection).await.unwrap();

    let batch = MacOutbox::new(&mut database)
        .claim_next_batch(MARK, 1, MARK)
        .await
        .unwrap();
    assert_eq!(batch.healthy.writes[0].request, good.request);
    assert!(batch.healthy.ended.is_empty());
    assert_eq!(
        batch.corrupt_rows,
        [RowCorruption {
            key: bad.request.to_string(),
            field: "created_at",
            reason: RowCorruptionReason::InvalidStorageType,
        }]
    );
    let retained: (String, String) = sqlx::query_as(
        "SELECT item_state, CAST(created_at AS TEXT) FROM outbox_items WHERE request_id = ?",
    )
    .bind(bad.request.as_str())
    .fetch_one(&mut database.connection)
    .await
    .unwrap();
    assert_eq!(retained, ("inOutbox".to_owned(), "invalid".to_owned()));
}
