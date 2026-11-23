use super::sender_outcomes::direct_write;
use super::{MARK, accepted_answer, caller, central_refusal, item, open, uuid_v7};
use crate::{
    MacOutbox, MachineLocalOutbox, MachineLocalStore, NewOutboxItem, OutboxAccepted,
    OutboxCancellation, OutboxCentralRefusal, OutboxQueuedWrite, OutboxRefusal, OutboxRequestKey,
    OutboxRequestStatus, OutboxStorage, OutboxStoreError, OutboxWriteOutcome, OutgoingOutcome,
    PendingWriteState, RowCorruptionReason, StoredSenderOutcome,
    outbox_receipt_profile::AnsweredWrite,
};
use router_clock::{ManualClock, RuntimeClock};
use std::{collections::BTreeSet, time::Duration};
use tempfile::TempDir;
use workspace_model::central_mail::{CentralMailRefusal, MessageOutcome};

const SEND_WINDOW: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Eq, PartialEq)]
struct CutoffRow {
    item_state: String,
    updated_at: i64,
    payload_json: String,
    outcome_json: Option<String>,
}

async fn cutoff_row(database: &mut MachineLocalStore, write: &NewOutboxItem) -> CutoffRow {
    let (item_state, updated_at, payload_json, outcome_json) = sqlx::query_as(
        "SELECT item_state, updated_at, payload_json, outcome_json
         FROM outbox_items WHERE request_id = ?",
    )
    .bind(write.request.as_str())
    .fetch_one(&mut database.connection)
    .await
    .unwrap();
    CutoffRow {
        item_state,
        updated_at,
        payload_json,
        outcome_json,
    }
}

async fn unconfirmed_cutoff(root: &TempDir, write: &NewOutboxItem) -> CutoffRow {
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let mut database = open(root).await;
    let mut store = MacOutbox::new(&mut database);
    let now = clock.utc_now().timestamp_millis();
    store.insert(write, now).await.unwrap();
    let batch = store
        .claim_next_batch(now, 1, now, now, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(&batch.writes[0].request, &write.request);
    clock.advance(SEND_WINDOW).unwrap();
    let now = clock.utc_now().timestamp_millis();
    let ended = store
        .end_overdue_orphans(now, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(ended.len(), 1);
    assert_eq!(&ended[0].request_id, &write.request);
    assert_eq!(
        ended[0].reason,
        OutboxCancellation::NotConfirmedWithin15Minutes
    );

    let cutoff = cutoff_row(&mut database, write).await;
    assert_eq!(cutoff.item_state, "unconfirmed");
    assert_eq!(cutoff.updated_at, now);
    assert_eq!(cutoff.outcome_json, None);
    database.close().await.unwrap();
    cutoff
}

async fn late_result_round_trip(
    write: NewOutboxItem,
    outcome: OutboxWriteOutcome,
    expected_status: OutboxRequestStatus,
    expected_pending: PendingWriteState,
) {
    let root = tempfile::tempdir().unwrap();
    let cutoff = unconfirmed_cutoff(&root, &write).await;
    let mut store = MachineLocalOutbox::new(open(&root).await);
    let key = OutboxRequestKey {
        request_id: write.request.clone(),
        scope: write.payload.request_scope(),
    };
    assert_eq!(
        store
            .scoped_request_status(&write.caller, &key)
            .await
            .unwrap(),
        OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes
        },
    );
    assert!(
        store
            .outgoing_for(&write.caller, 10)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .record_late_outcome(&write.request, &outcome)
        .await
        .unwrap();
    drop(store);

    let mut database = open(&root).await;
    let recorded = cutoff_row(&mut database, &write).await;
    assert!(recorded.outcome_json.is_some());
    assert_eq!(recorded.item_state, cutoff.item_state);
    assert_eq!(recorded.updated_at, cutoff.updated_at);
    assert_eq!(recorded.payload_json, cutoff.payload_json);
    let profile: AnsweredWrite = serde_json::from_str(&recorded.payload_json).unwrap();
    assert_eq!(profile.request_id, write.request);
    assert_eq!(profile.target, write.payload.content().unwrap().target);
    assert_eq!(
        profile.content_digest,
        write.payload.content_digest().unwrap()
    );

    let mut store = MachineLocalOutbox::new(database);
    assert_eq!(
        store
            .scoped_request_status(&write.caller, &key)
            .await
            .unwrap(),
        expected_status,
    );
    assert_eq!(
        store
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        expected_status,
    );
    let pending = store.pending_for(&write.caller).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].request_id, write.request);
    assert_eq!(pending[0].state, expected_pending);
    let outgoing = store.outgoing_for(&write.caller, 10).await.unwrap();
    assert_eq!(
        outgoing.len(),
        1,
        "a recorded cutoff outcome must appear in outgoing history"
    );
    let expected_outgoing = match expected_status {
        OutboxRequestStatus::Answered(answer) => OutgoingOutcome::Answered(answer),
        OutboxRequestStatus::Refused(refusal) => OutgoingOutcome::Refused(refusal),
        _ => panic!("the fixture must provide a settled answer or refusal"),
    };
    assert_eq!(outgoing[0].request_id, write.request);
    assert_eq!(outgoing[0].target, profile.target);
    assert_eq!(outgoing[0].outcome, expected_outgoing);
    assert_eq!(
        outgoing[0].answered_at_ms, None,
        "the cutoff is not an answer time"
    );
    assert_eq!(
        store.scoped_request_status(&caller(2), &key).await.unwrap(),
        OutboxRequestStatus::NotFound,
    );
    assert!(store.pending_for(&caller(2)).await.unwrap().is_empty());
    assert!(store.outgoing_for(&caller(2), 10).await.unwrap().is_empty());
    assert!(
        store
            .end_overdue(cutoff.updated_at)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    assert!(
        store
            .end_overdue_orphans(cutoff.updated_at, &BTreeSet::new())
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    let batch = store
        .claim_next_batch(
            cutoff.updated_at,
            1,
            cutoff.updated_at,
            cutoff.updated_at,
            &BTreeSet::new(),
        )
        .await
        .unwrap()
        .expect_clean();
    assert!(batch.ended.is_empty());
    assert!(batch.writes.is_empty());
}

#[tokio::test]
async fn late_post_acceptance_survives_reopen_after_the_cutoff() {
    let write = item(521, "accepted post", MARK);
    let reply = accepted_answer();
    let result = OutboxWriteOutcome::from_accepted(&write.payload, reply.clone()).unwrap();
    late_result_round_trip(
        write,
        result,
        OutboxRequestStatus::Answered(reply.clone()),
        PendingWriteState::Answered(reply),
    )
    .await;
}

#[tokio::test]
async fn late_post_refusal_survives_reopen_after_the_cutoff() {
    let write = item(522, "refused post", MARK);
    let refusal = OutboxRefusal::Central(central_refusal());
    let result = OutboxWriteOutcome::from_refusal(&write.payload, refusal.clone()).unwrap();
    late_result_round_trip(
        write,
        result,
        OutboxRequestStatus::Refused(refusal.clone()),
        PendingWriteState::Refused(refusal),
    )
    .await;
}

#[tokio::test]
async fn late_direct_message_acceptance_survives_reopen_after_the_cutoff() {
    let write = direct_write(523);
    let reply = OutboxAccepted::SendMessage {
        message_id: uuid_v7(524).parse().unwrap(),
    };
    let result = OutboxWriteOutcome::from_accepted(&write.payload, reply.clone()).unwrap();
    late_result_round_trip(
        write,
        result,
        OutboxRequestStatus::Answered(reply.clone()),
        PendingWriteState::Answered(reply),
    )
    .await;
}

#[tokio::test]
async fn late_direct_message_refusal_survives_reopen_after_the_cutoff() {
    let write = direct_write(525);
    let OutboxQueuedWrite::SendMessage { message, .. } = &write.payload else {
        panic!("expected a direct message fixture")
    };
    let refusal = OutboxRefusal::Central(OutboxCentralRefusal::Mail(
        CentralMailRefusal::UnknownSession {
            target: message.to.clone(),
        },
    ));
    let result = OutboxWriteOutcome::from_refusal(&write.payload, refusal.clone()).unwrap();
    late_result_round_trip(
        write,
        result,
        OutboxRequestStatus::Refused(refusal.clone()),
        PendingWriteState::Refused(refusal),
    )
    .await;
}

#[tokio::test]
async fn exact_late_replay_and_contradiction_keep_the_first_result() {
    let root = tempfile::tempdir().unwrap();
    let write = item(526, "write that may have landed", MARK);
    let result = OutboxWriteOutcome::from_accepted(&write.payload, accepted_answer()).unwrap();
    let other_result =
        OutboxWriteOutcome::from_refusal(&write.payload, OutboxRefusal::Central(central_refusal()))
            .unwrap();
    unconfirmed_cutoff(&root, &write).await;
    let mut database = open(&root).await;
    MacOutbox::new(&mut database)
        .record_late_outcome(&write.request, &result)
        .await
        .unwrap();
    let stored = cutoff_row(&mut database, &write).await;
    database.close().await.unwrap();
    let mut database = open(&root).await;

    MacOutbox::new(&mut database)
        .record_late_outcome(&write.request, &result)
        .await
        .unwrap();
    assert_eq!(cutoff_row(&mut database, &write).await, stored);
    assert!(matches!(
        MacOutbox::new(&mut database).record_late_outcome(&write.request, &other_result).await,
        Err(OutboxStoreError::ContradictoryFinalOutcome { request }) if request == write.request
    ));
    assert_eq!(cutoff_row(&mut database, &write).await, stored);
}

#[tokio::test]
async fn late_outcome_checks_request_target_and_original_content_independently() {
    let root = tempfile::tempdir().unwrap();
    let write = item(527, "original body", MARK);
    let wrong_request = item(528, "original body", MARK);
    let wrong_body = item(527, "changed body", MARK);
    let mut wrong_workspace = write.payload.clone();
    let OutboxQueuedWrite::Workspace(workspace) = &mut wrong_workspace else {
        panic!("expected a workspace post fixture")
    };
    workspace.workspace_id = Some(uuid_v7(60_003).parse().unwrap());
    let cutoff = unconfirmed_cutoff(&root, &write).await;
    let mut database = open(&root).await;

    for wrong_payload in [
        &wrong_request.payload,
        &wrong_workspace,
        &wrong_body.payload,
    ] {
        let result = OutboxWriteOutcome::from_accepted(wrong_payload, accepted_answer()).unwrap();
        assert!(matches!(
            MacOutbox::new(&mut database).record_late_outcome(&write.request, &result).await,
            Err(OutboxStoreError::InvalidOutcome { request }) if request == write.request
        ));
        assert_eq!(cutoff_row(&mut database, &write).await, cutoff);
    }
    let result = OutboxWriteOutcome::from_accepted(&write.payload, accepted_answer()).unwrap();
    MacOutbox::new(&mut database)
        .record_late_outcome(&write.request, &result)
        .await
        .unwrap();
}

#[tokio::test]
async fn never_sent_and_other_states_refuse_late_outcomes() {
    let root = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let never_sent = item(529, "never left", MARK);
    let active = item(530, "can still leave", MARK);
    let missing = item(531, "absent", MARK);
    let mut database = open(&root).await;
    let mut store = MacOutbox::new(&mut database);
    let now = clock.utc_now().timestamp_millis();
    store.insert(&never_sent, now).await.unwrap();
    clock.advance(SEND_WINDOW).unwrap();
    let now = clock.utc_now().timestamp_millis();
    let expired = store.end_overdue(now).await.unwrap().expect_clean();
    assert_eq!(
        expired[0].reason,
        OutboxCancellation::NotSentWithin15Minutes
    );
    store.insert(&active, now).await.unwrap();
    database.close().await.unwrap();
    let mut database = open(&root).await;
    let cutoff = cutoff_row(&mut database, &never_sent).await;
    let mut store = MacOutbox::new(&mut database);

    let result = OutboxWriteOutcome::from_accepted(&never_sent.payload, accepted_answer()).unwrap();
    assert!(matches!(
        store.record_late_outcome(&never_sent.request, &result).await,
        Err(OutboxStoreError::LateOutcomeForNotSent { request }) if request == never_sent.request
    ));
    for write in [&active, &missing] {
        let result = OutboxWriteOutcome::from_accepted(&write.payload, accepted_answer()).unwrap();
        assert!(matches!(
            store.record_late_outcome(&write.request, &result).await,
            Err(OutboxStoreError::UnexpectedState { request, .. }) if request == write.request
        ));
    }
    assert_eq!(cutoff_row(&mut database, &never_sent).await, cutoff);
}

#[tokio::test]
async fn unconfirmed_row_rejects_untagged_and_incompatible_stored_outcomes() {
    let root = tempfile::tempdir().unwrap();
    let write = item(532, "body dropped at cutoff", MARK);
    let cutoff = unconfirmed_cutoff(&root, &write).await;
    let mut database = open(&root).await;
    for (invalid, expected_fault) in [
        (
            serde_json::to_value(accepted_answer()).unwrap(),
            RowCorruptionReason::InvalidJson,
        ),
        (
            serde_json::json!({"kind": "answered", "value": OutboxAccepted::SendMessage {
                message_id: uuid_v7(533).parse().unwrap()
            }}),
            RowCorruptionReason::IncompatibleOutcome,
        ),
    ] {
        sqlx::query("UPDATE outbox_items SET outcome_json = ? WHERE request_id = ?")
            .bind(invalid.to_string())
            .bind(write.request.as_str())
            .execute(&mut database.connection)
            .await
            .unwrap();
        let scan = MacOutbox::new(&mut database)
            .delete_ended_before_retention(cutoff.updated_at + super::RETENTION_MS)
            .await
            .unwrap();
        assert_eq!(scan.healthy, 0);
        assert_eq!(scan.corrupt_rows.len(), 1);
        assert_eq!(scan.corrupt_rows[0].field, "outcome_json");
        assert_eq!(scan.corrupt_rows[0].reason, expected_fault);
    }
}

#[tokio::test]
async fn a_following_sender_notice_keeps_the_late_acceptance_and_cutoff() {
    let root = tempfile::tempdir().unwrap();
    let write = direct_write(534);
    let cutoff = unconfirmed_cutoff(&root, &write).await;
    let reported_at = chrono::DateTime::from_timestamp_millis(cutoff.updated_at + 1_000).unwrap();
    let notice = StoredSenderOutcome {
        inbox_message_id: uuid_v7(535).parse().unwrap(),
        message_id: uuid_v7(536).parse().unwrap(),
        request_id: write.request.clone(),
        outcome: MessageOutcome::DeliveryNotConfirmed,
        reported_at,
    };
    let reply = OutboxAccepted::SendMessage {
        message_id: notice.message_id.clone(),
    };
    let result = OutboxWriteOutcome::from_accepted(&write.payload, reply.clone()).unwrap();
    let mut database = open(&root).await;
    MacOutbox::new(&mut database)
        .record_late_outcome(&write.request, &result)
        .await
        .unwrap();
    let before_notice = cutoff_row(&mut database, &write).await;

    assert!(
        MacOutbox::new(&mut database)
            .record_sender_delivery(&write.caller, &notice, reported_at.timestamp_millis(),)
            .await
            .unwrap()
    );
    database.close().await.unwrap();
    let mut database = open(&root).await;
    let after_notice = cutoff_row(&mut database, &write).await;
    assert_eq!(after_notice.item_state, cutoff.item_state);
    assert_eq!(after_notice.updated_at, cutoff.updated_at);
    assert_eq!(after_notice.outcome_json, before_notice.outcome_json);
    let mut store = MacOutbox::new(&mut database);
    assert_eq!(
        store
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(reply)
    );
    assert_eq!(
        store
            .sender_outcome(&write.caller, &write.request)
            .await
            .unwrap(),
        Some(notice)
    );
}
