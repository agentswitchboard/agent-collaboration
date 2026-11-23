use super::{MARK, RETENTION_MS, accepted_reply, caller, item, open};
use crate::{
    MacOutbox, MachineLocalStore, NewOutboxItem, OutboxRequestKey, OutboxRequestStatus,
    OutboxStoreError, StoredSenderOutcome,
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

async fn rows(database: &mut MachineLocalStore) -> Vec<(String, String, Option<String>, i64)> {
    sqlx::query_as(
        "SELECT item_state, payload_json, outcome_json, expires_at_ms
         FROM outbox_items ORDER BY request_id",
    )
    .fetch_all(&mut database.connection)
    .await
    .unwrap()
}

#[tokio::test]
async fn invalid_completed_row_cannot_be_pruned() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let valid = item(110, "first decision", MARK);
    let broken = item(111, "loss sentinel", MARK);
    plant_row(
        &mut database,
        &valid,
        "answered",
        body_free_payload(&valid),
        Some(serde_json::to_string(&accepted_reply()).unwrap()),
        DEADLINE,
    )
    .await;
    plant_row(
        &mut database,
        &broken,
        "answered",
        body_free_payload(&broken),
        None,
        DEADLINE,
    )
    .await;
    let before = rows(&mut database).await;

    assert!(matches!(
        MacOutbox::new(&mut database).delete_ended_before_retention(MARK + RETENTION_MS).await,
        Err(OutboxStoreError::CorruptItem { request_id, field: "outcome_json" })
            if request_id == broken.request.as_str()
    ));
    assert_eq!(rows(&mut database).await, before);
}

#[tokio::test]
async fn invalid_deadlines_prevent_claim_retry_or_expiry_without_rewriting_rows() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let valid = item(112, "first eligible", MARK);
    let waiting = item(113, "invalid waiting deadline", MARK);
    let sent = item(114, "invalid in-flight deadline", MARK);
    for (write, state, deadline) in [
        (&valid, "inOutbox", DEADLINE),
        (&waiting, "inOutbox", DEADLINE + 60_000),
        (&sent, "sent", DEADLINE + 60_000),
    ] {
        plant_row(
            &mut database,
            write,
            state,
            serde_json::to_string(&write.payload).unwrap(),
            None,
            deadline,
        )
        .await;
    }
    let before = rows(&mut database).await;

    let mut outbox = MacOutbox::new(&mut database);
    for result in [
        outbox.claim_next_batch(MARK, 1, DEADLINE).await.map(|_| ()),
        outbox.end_overdue(DEADLINE).await.map(|_| ()),
        outbox.earliest_deadline().await.map(|_| ()),
        outbox.sent_without_answer().await.map(|_| ()),
        outbox
            .return_unanswered(std::slice::from_ref(&sent.request), DEADLINE)
            .await
            .map(|_| ()),
        outbox.return_not_transmitted(&sent.request, DEADLINE).await,
    ] {
        assert!(matches!(
            result,
            Err(OutboxStoreError::CorruptItem {
                field: "expires_at_ms",
                ..
            })
        ));
    }
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
    use central_link_protocol::ReplyPayload;
    use workspace_model::{
        WorkspaceReply, WorkspaceResult,
        central_mail::{CentralMailAnswer, CentralMailResult},
    };

    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let transient = item(118, "transient", MARK);
    let wrong_operation = item(119, "wrong operation", MARK);
    for (write, reply) in [
        (
            &transient,
            ReplyPayload::Workspace(WorkspaceReply {
                result: WorkspaceResult::OutcomeUnknown,
                actors: vec![],
            }),
        ),
        (
            &wrong_operation,
            ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued {
                message_id: super::uuid_v7(120).parse().unwrap(),
            })),
        ),
    ] {
        plant_row(
            &mut database,
            write,
            "answered",
            body_free_payload(write),
            Some(serde_json::to_string(&reply).unwrap()),
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
