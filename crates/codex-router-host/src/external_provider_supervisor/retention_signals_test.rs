use super::retention_fixture::{Operations, at};
use collaboration_protocol::{
    ProviderOperationEffect, ProviderOperationStage, ProviderReconciliationState,
};

#[tokio::test(flavor = "current_thread")]
async fn held_result_publication_and_store_commit_signal_distinct_sources() {
    let operations = Operations::open().await;
    let now = at("2026-06-15T12:00:00.000Z");
    let old = at("2026-05-01T00:00:00.000Z");
    operations.clock.set_utc(now);
    let held = operations.settled(old).await;
    let _free = operations.settled(old).await;
    let committed_later = operations.dispatched().await;
    operations.supervisor.hold_live_operation(held.clone());

    let mut commits = operations.store.lock().await.subscribe_commits();
    let mut completed = operations.supervisor.subscribe_completions();

    assert!(operations.supervisor.publish_held_result(&held).await);
    assert!(
        completed
            .has_changed()
            .expect("supervisor signal stays open")
    );
    assert!(!commits.has_changed().expect("store signal stays open"));
    let _completed_revision = *completed.borrow_and_update();
    let _commit_revision = *commits.borrow_and_update();

    let terminal = operations
        .store
        .lock()
        .await
        .record_terminal(
            &committed_later,
            ProviderOperationEffect::Applied,
            ProviderReconciliationState::Confirmed,
            None,
            old.timestamp_millis(),
        )
        .await
        .expect("commit a third eligible row");
    assert_eq!(terminal.operation_id, committed_later);
    assert_eq!(terminal.stage, ProviderOperationStage::Terminal);
    assert_eq!(terminal.terminal_at_ms, Some(old.timestamp_millis()));
    assert!(commits.has_changed().expect("store signal stays open"));
    assert!(
        !completed
            .has_changed()
            .expect("supervisor signal stays open")
    );
}
