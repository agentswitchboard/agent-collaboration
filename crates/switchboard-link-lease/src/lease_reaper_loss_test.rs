use super::*;

#[tokio::test]
async fn reaper_loss_cannot_confirm_join_while_a_real_child_holds_the_owner() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let acquire = start_acquire(&fixture.source, &key).await;
    let mut held_child = fixture.refreshes.recv().await.unwrap();
    let abort = match &*fixture.source.completion.lock().await {
        Completion::Running(reaper) => reaper.abort_handle(),
        Completion::Reaped(outcome) => panic!("unexpected reaped worker: {outcome:?}"),
        Completion::Finished(receipt) => panic!("unexpected completed receipt: {receipt:?}"),
    };

    abort.abort();
    let expected = Err(LeaseJoinUnconfirmed::ReaperCancelled);
    assert_eq!(fixture.source.join().await, expected);
    assert!(
        !held_child.is_closed(),
        "the detached worker still owns the held child"
    );
    assert_eq!(
        fixture.source.worker_exit().await,
        LeaseWorkerExit::Unconfirmed
    );
    assert_eq!(fixture.source.join().await, expected);

    fixture
        .source
        .request_stop_by(fixture.clock.monotonic_now());
    held_child.closed().await;
    assert_eq!(acquire.await, Err(LeaseFailure::Fault));
}

#[test]
fn a_handle_surviving_runtime_teardown_keeps_the_unconfirmed_join() {
    let started_runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (fixture, held_child) = started_runtime.block_on(async {
        let mut fixture = Fixture::new();
        let key = fixture.binding.key();
        let acquire = start_acquire(&fixture.source, &key).await;
        let held_child = fixture.refreshes.recv().await.unwrap();
        drop(acquire);
        (fixture, held_child)
    });
    drop(started_runtime);
    assert!(held_child.is_closed());

    let observer_runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    observer_runtime.block_on(async {
        let expected = Err(LeaseJoinUnconfirmed::ReaperCancelled);
        assert_eq!(fixture.source.join().await, expected);
        assert_eq!(
            fixture.source.worker_exit().await,
            LeaseWorkerExit::Unconfirmed
        );
        assert_eq!(fixture.source.join().await, expected);
    });
}
