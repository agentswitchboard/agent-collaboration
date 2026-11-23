use super::{MARK, central_refusal, item, open};
use crate::{MacOutbox, OutboxRefusal, OutboxRequestStatus, OutboxStoreError, OutboxWriteOutcome};
use crate::{OutboxAccepted, OutboxCentralRefusal, OutboxRequestKey, OutboxScope};
use std::collections::BTreeSet;

#[tokio::test]
async fn insert_failure_distinguishes_before_begin_and_after_begin() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = item(96, "write awaiting storage", MARK);

    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut database.connection)
        .await
        .unwrap();
    let before_begin = MacOutbox::new(&mut database).insert(&write, MARK).await;
    sqlx::query("ROLLBACK")
        .execute(&mut database.connection)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TRIGGER refuse_outbox_insert BEFORE INSERT ON outbox_items
         BEGIN SELECT RAISE(ABORT, 'injected write failure'); END",
    )
    .execute(&mut database.connection)
    .await
    .unwrap();
    let after_begin = MacOutbox::new(&mut database).insert(&write, MARK).await;

    assert!(matches!(
        before_begin,
        Err(OutboxStoreError::InsertNotStarted(_))
    ));
    assert!(matches!(after_begin, Err(OutboxStoreError::Database(_))));
    assert_eq!(
        MacOutbox::new(&mut database)
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::NotFound,
    );
}

#[tokio::test]
async fn workspace_refusal_retains_the_original_json_bytes_and_caller_scope() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = item(97, "a refused post", MARK);
    let reason = central_refusal();
    let outcome =
        OutboxWriteOutcome::from_refusal(&write.payload, OutboxRefusal::Central(reason.clone()))
            .unwrap();
    let mut store = MacOutbox::new(&mut database);
    store.insert(&write, MARK - 1_000).await.unwrap();
    store
        .mark_sent(std::slice::from_ref(&write.request), MARK)
        .await
        .unwrap();
    store
        .record_outcome(&write.request, &outcome, MARK)
        .await
        .unwrap();

    let mut query = OutboxRequestKey {
        request_id: write.request.clone(),
        scope: write.payload.request_scope(),
    };
    assert_eq!(
        store
            .scoped_request_status(&write.caller, &query)
            .await
            .unwrap(),
        OutboxRequestStatus::Refused(OutboxRefusal::Central(reason.clone())),
    );
    assert_eq!(
        store
            .scoped_request_status(&super::caller(2), &query)
            .await
            .unwrap(),
        OutboxRequestStatus::NotFound,
    );
    query.scope = OutboxScope::Organization;
    assert_eq!(
        store
            .scoped_request_status(&write.caller, &query)
            .await
            .unwrap(),
        OutboxRequestStatus::NotFound,
    );

    let stored: String = sqlx::query_scalar("SELECT outcome_json FROM outbox_items")
        .fetch_one(&mut database.connection)
        .await
        .unwrap();
    assert_eq!(
        stored,
        r#"{"kind":"workspace","reason":{"kind":"requestIdReused"}}"#
    );
    sqlx::query("UPDATE outbox_items SET outcome_json = '{\"kind\":\"unrecognized\"}'")
        .execute(&mut database.connection)
        .await
        .unwrap();
    assert!(matches!(
        MacOutbox::new(&mut database)
            .request_status(&write.caller, &write.request)
            .await,
        Err(OutboxStoreError::CorruptItem {
            field: "outcome_json",
            ..
        }),
    ));
}

#[tokio::test]
async fn incompatible_results_and_authority_loss_leave_the_claim_recoverable() {
    use crate::{NewOutboxItem, OutboxQueuedWrite, PendingWriteState};
    use workspace_model::{RefusalReason, central_mail::CentralMailRefusal};

    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let request = super::uuid_v7(120).parse().unwrap();
    let write = NewOutboxItem {
        request,
        caller: super::caller(1),
        payload: OutboxQueuedWrite::SendMessage {
            request_id: super::uuid_v7(120).parse().unwrap(),
            message: serde_json::from_value(serde_json::json!({
                "to": {"machineId": super::uuid_v7(121), "sessionId": "recipient"},
                "body": "keep my body", "delivery": "auto", "replyTo": null
            }))
            .unwrap(),
        },
        flush_at_ms: MARK,
    };
    let mut outbox = MacOutbox::new(&mut database);
    outbox.insert(&write, MARK - 1000).await.unwrap();
    let claim = outbox
        .claim_next_batch(MARK, 1, MARK, MARK, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean()
        .writes
        .pop()
        .unwrap();
    for reason in [
        CentralMailRefusal::Workspace(RefusalReason::MachineRevoked),
        CentralMailRefusal::WakeNotFound {
            wake_id: super::uuid_v7(122).parse().unwrap(),
        },
        CentralMailRefusal::UnknownSession {
            target: serde_json::from_value(serde_json::json!({
                "machineId": super::uuid_v7(121), "sessionId": "another"
            }))
            .unwrap(),
        },
    ] {
        assert!(
            OutboxWriteOutcome::from_refusal(
                &claim.payload,
                OutboxRefusal::Central(OutboxCentralRefusal::Mail(reason))
            )
            .is_err()
        );
    }
    assert!(OutboxWriteOutcome::from_accepted(&claim.payload, super::accepted_answer()).is_err());
    let wrong_operation = super::accepted(&item(123, "post fixture", MARK).payload);
    assert!(matches!(
        outbox
            .record_outcome(&claim.request, &wrong_operation, MARK)
            .await,
        Err(OutboxStoreError::InvalidOutcome { .. })
    ));
    assert_eq!(
        outbox
            .sent_without_answer()
            .await
            .unwrap()
            .expect_clean()
            .into_iter()
            .map(|claim| claim.key.request_id)
            .collect::<Vec<_>>(),
        std::slice::from_ref(&claim.request)
    );
    let waiting = outbox.pending_for(&write.caller).await.unwrap();
    assert!(matches!(&waiting[0].state,
        PendingWriteState::SentAnswerLost { body } if body.as_str() == "keep my body"));
    assert!(
        outbox
            .return_unanswered(std::slice::from_ref(&claim.request), MARK + 1)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    let replay = outbox
        .claim_next_batch(MARK, 1, MARK + 1, MARK + 1, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean()
        .writes;
    assert_eq!(replay, [claim]);
}

#[tokio::test]
async fn accepted_post_uses_the_store_result_in_all_three_read_views() {
    use crate::{OutgoingOutcome, PendingWriteState};
    use workspace_model::ChangePosition;

    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = item(124, "post accepted with position", MARK);
    let post_id = super::uuid_v7(125).parse().unwrap();
    let position = ChangePosition::try_from(27).unwrap();
    let expected = OutboxAccepted::ThreadPost {
        post_id,
        position,
        actors: vec![],
    };
    let mut outbox = MacOutbox::new(&mut database);
    outbox.insert(&write, MARK).await.unwrap();
    let claimed = outbox
        .claim_next_batch(MARK, 1, MARK, MARK, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean();
    let claim = &claimed.writes[0];
    let accepted = OutboxWriteOutcome::from_accepted(&claim.payload, expected.clone()).unwrap();
    outbox
        .record_outcome(&claim.request, &accepted, MARK)
        .await
        .unwrap();

    database.close().await.unwrap();
    let mut reopened = open(&root).await;
    let mut outbox = MacOutbox::new(&mut reopened);
    assert_eq!(
        outbox
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(expected.clone())
    );
    assert_eq!(
        outbox.pending_for(&write.caller).await.unwrap()[0].state,
        PendingWriteState::Answered(expected.clone())
    );
    assert_eq!(
        outbox.outgoing_for(&write.caller, 1).await.unwrap()[0].outcome,
        OutgoingOutcome::Answered(expected)
    );
}

#[tokio::test]
async fn mail_refusal_retains_the_original_json_bytes_and_round_trips() {
    use super::sender_outcomes::direct_write;
    use workspace_model::central_mail::CentralMailRefusal;

    let directory = tempfile::tempdir().unwrap();
    let mut database = open(&directory).await;
    let write = direct_write(455);
    let refusal = OutboxRefusal::Central(OutboxCentralRefusal::Mail(
        CentralMailRefusal::TimeOutOfRange {
            field: "anchorAt".to_owned(),
        },
    ));
    let mut outbox = MacOutbox::new(&mut database);
    outbox.insert(&write, MARK).await.unwrap();
    let claimed = outbox
        .claim_next_batch(MARK, 1, MARK, MARK, &BTreeSet::new())
        .await
        .unwrap()
        .expect_clean();
    let claim = &claimed.writes[0];
    let decision = OutboxWriteOutcome::from_refusal(&claim.payload, refusal.clone()).unwrap();
    outbox
        .record_outcome(&claim.request, &decision, MARK)
        .await
        .unwrap();

    let stored: String = sqlx::query_scalar("SELECT outcome_json FROM outbox_items")
        .fetch_one(&mut database.connection)
        .await
        .unwrap();
    assert_eq!(
        stored,
        r#"{"kind":"mail","reason":{"kind":"timeOutOfRange","value":{"field":"anchorAt"}}}"#
    );
    database.close().await.unwrap();
    let mut reopened = open(&directory).await;
    assert_eq!(
        MacOutbox::new(&mut reopened)
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Refused(refusal)
    );
}
