use super::{MacOutbox, NewOutboxItem, OutboxInsert, OutboxStoreError, StoredOutboxWrite};
use crate::{
    LocalOutboxRefusal, MachineLocalStore, OutboxCancellation, OutboxQueuedWrite, OutboxRefusal,
    OutboxRequestStatus, OutboxWriteOutcome, PendingWriteState,
};
use central_link_protocol::{ReplyPayload, RequestRefusal};
use serde_json::json;
use workspace_model::{Caller, RefusalReason, WorkspaceReply, WorkspaceResult};

#[path = "outbox_admission_test.rs"]
mod admission;
#[path = "outbox_store_model_test.rs"]
mod model;
#[path = "sender_outcome_store_test.rs"]
mod sender_outcomes;

fn central_refusal() -> RequestRefusal {
    RequestRefusal::Workspace(RefusalReason::RequestIdReused)
}

const MARK: i64 = 1_763_380_800_000;
const RETENTION_MS: i64 = 60 * 24 * 60 * 60 * 1_000;

fn uuid_v7(seed: u16) -> String {
    let mut random = [0_u8; 10];
    random[..2].copy_from_slice(&seed.to_be_bytes());
    uuid::Builder::from_unix_timestamp_millis(1_763_380_000_000 + u64::from(seed), &random)
        .into_uuid()
        .to_string()
}

fn caller(seed: u16) -> Caller {
    serde_json::from_value(json!({
        "kind": "session",
        "value": {"machineId": uuid_v7(60_000), "sessionId": format!("s{seed}")},
    }))
    .unwrap()
}

fn thread_post(request: &str, body: &str) -> OutboxQueuedWrite {
    serde_json::from_value(json!({
        "kind": "workspace",
        "request": {
            "requestId": request,
            "workspaceId": uuid_v7(60_001),
            "operation": {"kind": "threadPost", "threadId": uuid_v7(60_002), "body": body, "addressees": []},
        },
    }))
    .unwrap()
}

fn item(seed: u16, body: &str, flush_at: i64) -> NewOutboxItem {
    let request = uuid_v7(seed);
    NewOutboxItem {
        request: request.parse().unwrap(),
        caller: caller(1),
        payload: thread_post(&request, body),
        flush_at_ms: flush_at,
    }
}

fn accepted_reply() -> ReplyPayload {
    ReplyPayload::Workspace(WorkspaceReply {
        result: WorkspaceResult::Conflict,
        actors: Vec::new(),
    })
}

fn accepted() -> OutboxWriteOutcome {
    OutboxWriteOutcome::Answered(accepted_reply())
}

async fn open(root: &tempfile::TempDir) -> MachineLocalStore {
    MachineLocalStore::open(&root.path().join("machine_local.sqlite"))
        .await
        .unwrap()
}

#[tokio::test]
async fn a_write_moves_in_outbox_sent_accepted_and_survives_a_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let write = item(1, "hello", MARK);

    let inserted = MacOutbox::new(&mut store)
        .insert(&write, MARK - 20_000)
        .await
        .unwrap();
    let in_outbox = MacOutbox::new(&mut store)
        .request_status(&write.caller, &write.request)
        .await
        .unwrap();
    let sent = MacOutbox::new(&mut store)
        .mark_sent(std::slice::from_ref(&write.request), MARK)
        .await
        .unwrap();
    let after_send = MacOutbox::new(&mut store)
        .request_status(&write.caller, &write.request)
        .await
        .unwrap();
    MacOutbox::new(&mut store)
        .record_outcome(&write.request, &accepted(), MARK + 1_000)
        .await
        .unwrap();
    store.close().await.unwrap();
    let mut reopened = open(&root).await;
    let after_reopen = MacOutbox::new(&mut reopened)
        .request_status(&write.caller, &write.request)
        .await
        .unwrap();

    assert_eq!(inserted, OutboxInsert::Stored);
    assert_eq!(
        in_outbox,
        OutboxRequestStatus::InOutbox { flush_at_ms: MARK }
    );
    assert_eq!(
        sent,
        vec![StoredOutboxWrite {
            request: write.request.clone(),
            caller: write.caller.clone(),
            payload: write.payload.clone(),
            sent_at: chrono::DateTime::from_timestamp_millis(MARK - 20_000).unwrap(),
        }]
    );
    assert_eq!(after_send, OutboxRequestStatus::Sent);
    assert_eq!(
        after_reopen,
        OutboxRequestStatus::Answered(accepted_reply())
    );
}

#[tokio::test]
async fn an_answer_or_refusal_drops_the_text_from_the_row_and_every_view() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let answered = item(11, "first secret text", MARK);
    let refused = item(12, "second secret text", MARK);
    for write in [&answered, &refused] {
        MacOutbox::new(&mut store)
            .insert(write, MARK - 20_000)
            .await
            .unwrap();
    }
    let unsent = MacOutbox::new(&mut store)
        .pending_for(&answered.caller)
        .await
        .unwrap();
    MacOutbox::new(&mut store)
        .mark_sent(&[answered.request.clone(), refused.request.clone()], MARK)
        .await
        .unwrap();

    MacOutbox::new(&mut store)
        .record_outcome(&answered.request, &accepted(), MARK + 1_000)
        .await
        .unwrap();
    MacOutbox::new(&mut store)
        .record_outcome(
            &refused.request,
            &OutboxWriteOutcome::Refused(OutboxRefusal::Central(central_refusal())),
            MARK + 1_000,
        )
        .await
        .unwrap();
    let rows: Vec<String> = sqlx::query_scalar("SELECT payload_json FROM outbox_items")
        .fetch_all(&mut store.connection)
        .await
        .unwrap();
    let pending = MacOutbox::new(&mut store)
        .pending_for(&answered.caller)
        .await
        .unwrap();
    let outgoing = MacOutbox::new(&mut store)
        .outgoing_for(&answered.caller, 10)
        .await
        .unwrap();
    let resubmitted = MacOutbox::new(&mut store)
        .insert(&answered, MARK + 2_000)
        .await
        .unwrap();
    // The same id and target with a different body is another write.
    let mut different = answered.clone();
    different.payload = thread_post(answered.request.as_str(), "a different body");
    let conflicting = MacOutbox::new(&mut store)
        .insert(&different, MARK + 2_000)
        .await
        .unwrap();
    // The same id and body in another workspace is another write too.
    let mut moved = answered.clone();
    moved.payload = serde_json::from_value(json!({
        "kind": "workspace",
        "request": {
            "requestId": answered.request.as_str(),
            "workspaceId": uuid_v7(60_003),
            "operation": {"kind": "threadPost", "threadId": uuid_v7(60_002),
                "body": "first secret text", "addressees": []},
        },
    }))
    .unwrap();
    let moved_conflict = MacOutbox::new(&mut store)
        .insert(&moved, MARK + 2_000)
        .await
        .unwrap();

    assert!(matches!(
        &unsent[0].state,
        PendingWriteState::Unsent { body } if body.as_str() == "first secret text"
    ));
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(!row.contains("secret text"), "stored row keeps text: {row}");
    }
    let views = format!("{pending:?}{outgoing:?}");
    assert!(!views.contains("secret text"), "a view keeps text: {views}");
    assert_eq!(pending.len(), 1, "the refused write is not pending");
    assert_eq!(pending[0].target, outgoing[1].target);
    assert_eq!(outgoing.len(), 2);
    assert_eq!(
        resubmitted,
        OutboxInsert::AlreadyStored(OutboxRequestStatus::Answered(accepted_reply()))
    );
    assert_eq!(
        conflicting,
        OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict)
    );
    assert_eq!(
        moved_conflict,
        OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict)
    );
}

#[tokio::test]
async fn ended_rows_read_as_cancelled_hold_no_text_and_are_pruned() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let mut ids = Vec::new();
    for (seed, state) in [(21_u16, "notSent"), (24, "unconfirmed")] {
        let write = item(seed, "never sent", MARK);
        MacOutbox::new(&mut store)
            .insert(&write, MARK - 1_000)
            .await
            .unwrap();
        let body_free = json!({"requestId": write.request.as_str(), "target": {
            "kind": "thread", "workspaceId": uuid_v7(60_001), "threadId": uuid_v7(60_002)},
            "contentDigest": "0".repeat(64)});
        sqlx::query(
            "UPDATE outbox_items SET item_state = ?, payload_json = ? WHERE request_id = ?",
        )
        .bind(state)
        .bind(body_free.to_string())
        .bind(write.request.as_str())
        .execute(&mut store.connection)
        .await
        .unwrap();
        ids.push((write.request.clone(), write.caller.clone()));
    }
    let with_text = item(23, "still here", MARK);
    MacOutbox::new(&mut store)
        .insert(&with_text, MARK - 1_000)
        .await
        .unwrap();
    sqlx::query("UPDATE outbox_items SET item_state = 'notSent' WHERE request_id = ?")
        .bind(with_text.request.as_str())
        .execute(&mut store.connection)
        .await
        .unwrap();

    let mut statuses = Vec::new();
    for (request, caller) in &ids {
        statuses.push(
            MacOutbox::new(&mut store)
                .request_status(caller, request)
                .await
                .unwrap(),
        );
    }
    let cancelled_with_text = MacOutbox::new(&mut store)
        .request_status(&with_text.caller, &with_text.request)
        .await;
    sqlx::query("DELETE FROM outbox_items WHERE request_id = ?")
        .bind(with_text.request.as_str())
        .execute(&mut store.connection)
        .await
        .unwrap();
    let pending = MacOutbox::new(&mut store)
        .pending_for(&ids[0].1)
        .await
        .unwrap();
    let pruned = MacOutbox::new(&mut store)
        .delete_ended_before_retention(MARK + RETENTION_MS + 1_000)
        .await
        .unwrap();

    assert_eq!(
        statuses,
        vec![
            OutboxRequestStatus::Ended {
                reason: OutboxCancellation::NotSentWithin15Minutes
            },
            OutboxRequestStatus::Ended {
                reason: OutboxCancellation::NotConfirmedWithin15Minutes
            },
        ]
    );
    assert!(matches!(
        cancelled_with_text,
        Err(OutboxStoreError::CorruptItem {
            field: "payload_json",
            ..
        })
    ));
    assert!(
        pending
            .iter()
            .all(|write| matches!(write.state, PendingWriteState::Cancelled { .. }))
    );
    assert_eq!(pending.len(), 2);
    assert_eq!(pruned, 2);
}

#[tokio::test]
async fn another_caller_sees_not_found_and_resubmits_are_idempotent_or_conflicts() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let write = item(2, "same", MARK);
    MacOutbox::new(&mut store)
        .insert(&write, MARK - 20_000)
        .await
        .unwrap();

    let foreign = MacOutbox::new(&mut store)
        .request_status(&caller(2), &write.request)
        .await
        .unwrap();
    let again = MacOutbox::new(&mut store)
        .insert(&write, MARK - 10_000)
        .await
        .unwrap();
    let changed = MacOutbox::new(&mut store)
        .insert(&item(2, "different", MARK), MARK - 10_000)
        .await
        .unwrap();
    let other_caller = NewOutboxItem {
        caller: caller(2),
        ..write.clone()
    };
    let stolen = MacOutbox::new(&mut store)
        .insert(&other_caller, MARK - 10_000)
        .await
        .unwrap();

    assert_eq!(foreign, OutboxRequestStatus::NotFound);
    assert_eq!(
        again,
        OutboxInsert::AlreadyStored(OutboxRequestStatus::InOutbox { flush_at_ms: MARK })
    );
    assert_eq!(
        changed,
        OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict)
    );
    assert_eq!(
        stolen,
        OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict)
    );
}

#[tokio::test]
async fn unanswered_writes_return_with_their_receipt_mark_and_refusals_are_kept() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let first = item(3, "a", MARK);
    let second = item(4, "b", MARK + 60_000);
    MacOutbox::new(&mut store)
        .insert(&first, MARK - 1_000)
        .await
        .unwrap();
    MacOutbox::new(&mut store)
        .insert(&second, MARK + 1_000)
        .await
        .unwrap();
    MacOutbox::new(&mut store)
        .mark_sent(
            &[first.request.clone(), second.request.clone()],
            MARK + 60_000,
        )
        .await
        .unwrap();

    let unanswered = MacOutbox::new(&mut store)
        .sent_without_answer()
        .await
        .unwrap();
    MacOutbox::new(&mut store)
        .return_unanswered(std::slice::from_ref(&first.request), MARK + 70_000)
        .await
        .unwrap();
    let refusal = OutboxWriteOutcome::Refused(OutboxRefusal::Central(central_refusal()));
    MacOutbox::new(&mut store)
        .record_outcome(&second.request, &refusal, MARK + 70_000)
        .await
        .unwrap();

    let mut expected_unanswered = vec![first.request.clone(), second.request.clone()];
    expected_unanswered.sort();
    assert_eq!(unanswered, expected_unanswered);
    assert_eq!(
        MacOutbox::new(&mut store).waiting().await.unwrap(),
        vec![(MARK, first.request.clone())]
    );
    assert_eq!(
        MacOutbox::new(&mut store)
            .request_status(&second.caller, &second.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Refused(OutboxRefusal::Central(central_refusal()))
    );
}

#[tokio::test]
async fn out_of_order_transitions_and_mismatched_ids_are_refused() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let write = item(5, "x", MARK);
    MacOutbox::new(&mut store)
        .insert(&write, MARK - 1_000)
        .await
        .unwrap();
    let mismatched = NewOutboxItem {
        request: uuid_v7(6).parse().unwrap(),
        ..write.clone()
    };

    let answer_unsent = MacOutbox::new(&mut store)
        .record_outcome(&write.request, &accepted(), MARK)
        .await;
    let return_unsent = MacOutbox::new(&mut store)
        .return_unanswered(std::slice::from_ref(&write.request), MARK)
        .await;
    let mismatch = MacOutbox::new(&mut store).insert(&mismatched, MARK).await;
    MacOutbox::new(&mut store)
        .mark_sent(std::slice::from_ref(&write.request), MARK)
        .await
        .unwrap();
    let send_twice = MacOutbox::new(&mut store)
        .mark_sent(std::slice::from_ref(&write.request), MARK)
        .await;

    assert!(matches!(
        answer_unsent,
        Err(OutboxStoreError::UnexpectedState { .. })
    ));
    assert!(matches!(
        return_unsent,
        Err(OutboxStoreError::UnexpectedState { .. })
    ));
    assert!(matches!(
        mismatch,
        Err(OutboxStoreError::RequestIdMismatch { .. })
    ));
    assert!(matches!(
        send_twice,
        Err(OutboxStoreError::UnexpectedState { .. })
    ));
}

#[tokio::test]
async fn an_invalid_stored_row_is_refused_not_coerced() {
    let root = tempfile::tempdir().unwrap();
    let mut store = open(&root).await;
    let write = item(7, "x", MARK);
    MacOutbox::new(&mut store)
        .insert(&write, MARK - 1_000)
        .await
        .unwrap();
    sqlx::query("UPDATE outbox_items SET item_state = 'delivered'")
        .execute(&mut store.connection)
        .await
        .unwrap();
    let bad_state = MacOutbox::new(&mut store)
        .request_status(&write.caller, &write.request)
        .await;
    // An answered row that still holds the write's text is refused.
    let answer = serde_json::to_string(&accepted_reply()).unwrap();
    sqlx::query("UPDATE outbox_items SET item_state = 'answered', outcome_json = ?")
        .bind(&answer)
        .execute(&mut store.connection)
        .await
        .unwrap();
    let answered_with_text = MacOutbox::new(&mut store)
        .request_status(&write.caller, &write.request)
        .await;
    let body_free = json!({"requestId": write.request.as_str(), "target": {
        "kind": "thread", "workspaceId": uuid_v7(60_001), "threadId": uuid_v7(60_002)},
        "contentDigest": "0".repeat(64)});
    sqlx::query("UPDATE outbox_items SET payload_json = ?, outcome_json = NULL")
        .bind(body_free.to_string())
        .execute(&mut store.connection)
        .await
        .unwrap();
    let missing_outcome = MacOutbox::new(&mut store)
        .request_status(&write.caller, &write.request)
        .await;

    assert!(matches!(
        bad_state,
        Err(OutboxStoreError::CorruptItem {
            field: "item_state",
            ..
        })
    ));
    assert!(matches!(
        answered_with_text,
        Err(OutboxStoreError::CorruptItem {
            field: "payload_json",
            ..
        })
    ));
    assert!(matches!(
        missing_outcome,
        Err(OutboxStoreError::CorruptItem {
            field: "outcome_json",
            ..
        })
    ));
}
