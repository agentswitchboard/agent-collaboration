use super::{MARK, central_refusal, item, open};
use crate::{MacOutbox, OutboxRefusal, OutboxRequestStatus, OutboxStoreError, OutboxWriteOutcome};
use central_link_protocol::{RequestScope, RequestStatusQuery};

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
async fn central_refusal_uses_the_main_payload_and_preserves_caller_and_scope() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = item(97, "a refused post", MARK);
    let reason = central_refusal();
    let outcome = OutboxWriteOutcome::Refused(OutboxRefusal::Central(reason.clone()));
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

    let mut query = RequestStatusQuery {
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
    query.scope = RequestScope::Organization;
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
    assert_eq!(stored, serde_json::to_string(&reason).unwrap());
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
