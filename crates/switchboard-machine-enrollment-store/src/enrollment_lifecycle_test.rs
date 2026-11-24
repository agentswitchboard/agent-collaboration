use std::task::Poll;

use super::*;

async fn assert_connection_close_unconfirmed(
    store: &MachineEnrollmentStore,
    exit: EnrollmentWorkerExit,
) {
    let join = store.join().await;
    assert!(
        matches!(join, Err(EnrollmentObservationError::ConnectionCloseUnconfirmed { exit: observed }) if observed == exit),
        "expected unconfirmed SQLite close, got {join:?}"
    );
    assert_eq!(store.worker_exit().await, Ok(exit));
}

#[tokio::test]
async fn missing_sqlite_close_acknowledgement_preserves_connection_error_after_normal_exit() {
    let fixture = Fixture::open().await;
    fixture.faults.lose_close_acknowledgement();

    fixture.store.request_close();

    assert_connection_close_unconfirmed(&fixture.store, EnrollmentWorkerExit::Requested).await;
    let first = fixture.store.close().await;
    let second = fixture.store.close().await;
    let (
        Err(EnrollmentStoreCloseError::Connection(first)),
        Err(EnrollmentStoreCloseError::Connection(second)),
    ) = (first, second)
    else {
        panic!("both closes must return the retained SQLite error");
    };
    assert!(matches!(
        first.as_ref(),
        MachineLocalStoreError::Database(sqlx::Error::WorkerCrashed)
    ));
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn missing_sqlite_close_acknowledgement_preserves_worker_stopped_after_panic() {
    let fixture = Fixture::open().await;
    fixture.faults.lose_close_acknowledgement();
    fixture.faults.arm(TestWriteFault::PanicBeforeCommit);
    let initial = fixture.binding(1, ENROLLMENT, TOKEN);

    assert_eq!(
        fixture.store.replace(generation(0), &initial).await,
        Err(CredentialStoreError::Replace)
    );

    assert_connection_close_unconfirmed(&fixture.store, EnrollmentWorkerExit::Autonomous).await;
    for _ in 0..2 {
        assert!(matches!(
            fixture.store.close().await,
            Err(EnrollmentStoreCloseError::WorkerStopped)
        ));
    }
}

#[tokio::test]
async fn requested_close_finishes_started_write_after_first_join_waiter_is_dropped() {
    let fixture = Fixture::open().await;
    let original = fixture.install().await;
    let successor = fixture.binding(2, ALTERNATE_ENROLLMENT, TOKEN);
    let (started, release_write) = fixture.faults.hold_replacement_witness();
    let mut replacing = Box::pin(fixture.store.replace(original.generation, &successor));
    assert_queued(replacing.as_mut());
    started.await.unwrap();

    fixture.store.request_close();
    let mut first_waiter = Box::pin(fixture.store.join());
    assert_queued(first_waiter.as_mut());
    drop(first_waiter);
    release_write.send(()).unwrap();

    assert_eq!(replacing.await, Ok(ConditionalWrite::Applied));
    assert_eq!(
        fixture.store.worker_exit().await,
        Ok(EnrollmentWorkerExit::Requested)
    );
    let (first_receipt, second_receipt) = tokio::join!(fixture.store.join(), fixture.store.join());
    assert!(
        matches!(first_receipt, Ok(EnrollmentJoin { outcome: Ok(()) })),
        "{first_receipt:?}"
    );
    assert!(
        matches!(second_receipt, Ok(EnrollmentJoin { outcome: Ok(()) })),
        "{second_receipt:?}"
    );
    let reopened = fixture.another_worker().await;
    assert_eq!(
        reopened.read().await,
        Ok(StoredEnrollment::Active(successor))
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn autonomous_panic_is_visible_to_each_waiter_before_resources_are_reaped() {
    let fixture = Fixture::open().await;
    let mut first_observer = Box::pin(fixture.store.worker_exit());
    let mut second_observer = Box::pin(fixture.store.worker_exit());
    assert_queued(first_observer.as_mut());
    assert_queued(second_observer.as_mut());
    let (reaping, release_resources) = fixture.faults.hold_resource_cleanup();
    fixture.faults.arm(TestWriteFault::PanicBeforeCommit);
    let initial = fixture.binding(1, ENROLLMENT, TOKEN);

    let attempt = fixture.store.replace(generation(0), &initial).await;
    reaping.await.unwrap();

    assert_eq!(attempt, Err(CredentialStoreError::Replace));
    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        first_observer.as_mut().poll(&mut context),
        Poll::Ready(Ok(EnrollmentWorkerExit::Autonomous))
    );
    assert_eq!(
        second_observer.as_mut().poll(&mut context),
        Poll::Ready(Ok(EnrollmentWorkerExit::Autonomous))
    );
    let mut joining = Box::pin(fixture.store.join());
    assert_queued(joining.as_mut());

    fixture.store.request_close();
    release_resources.send(()).unwrap();
    assert!(matches!(
        joining.await,
        Ok(EnrollmentJoin {
            outcome: Err(EnrollmentStoreCloseError::WorkerStopped)
        })
    ));
    assert_eq!(
        fixture.store.worker_exit().await,
        Ok(EnrollmentWorkerExit::Autonomous)
    );
    assert!(matches!(
        fixture.store.join().await,
        Ok(EnrollmentJoin {
            outcome: Err(EnrollmentStoreCloseError::WorkerStopped)
        })
    ));
}

#[test]
fn reaper_lost_after_exit_leaves_exit_readable_but_ownership_unconfirmed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (fixture, cleanup_gate) = runtime.block_on(async {
        let fixture = Fixture::open().await;
        let (reaping, cleanup_gate) = fixture.faults.hold_resource_cleanup();
        fixture.store.request_close();
        reaping.await.unwrap();
        (fixture, cleanup_gate)
    });

    drop(runtime);
    drop(cleanup_gate);

    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        std::pin::pin!(fixture.store.worker_exit()).poll(&mut context),
        Poll::Ready(Ok(EnrollmentWorkerExit::Requested))
    );
    let join = std::pin::pin!(fixture.store.join()).poll(&mut context);
    assert!(
        matches!(
            join,
            Poll::Ready(Err(EnrollmentObservationError::ReaperLost))
        ),
        "{join:?}"
    );
}

#[tokio::test]
async fn aborted_worker_joins_only_after_its_real_blocking_keychain_read_ends() {
    let fixture = Fixture::open().await;
    fixture.install().await;
    let (keychain_started, release_keychain) = fixture.keychain.hold_next_read();
    let pending_key_reap = fixture.faults.watch_pending_key_reap();
    let mut reading = std::pin::pin!(fixture.store.read());
    assert_queued(reading.as_mut());
    keychain_started.await.unwrap();

    fixture.store.request_close();
    fixture.store.abort_handle().abort();
    assert_eq!(
        fixture.store.worker_exit().await,
        Ok(EnrollmentWorkerExit::Autonomous)
    );
    let mut joining = std::pin::pin!(fixture.store.join());
    let blocked = tokio::select! {
        biased;
        early_join = &mut joining => Err(early_join),
        pending = pending_key_reap => {
            pending.unwrap();
            Ok(())
        },
    };
    assert!(
        blocked.is_ok(),
        "joined while the Keychain read was held: {blocked:?}"
    );
    release_keychain.release();

    assert_eq!(reading.await, Err(CredentialStoreError::Read));
    assert!(matches!(
        joining.await,
        Ok(EnrollmentJoin {
            outcome: Err(EnrollmentStoreCloseError::WorkerStopped)
        })
    ));
    assert_eq!(
        fixture.store.worker_exit().await,
        Ok(EnrollmentWorkerExit::Autonomous)
    );
    assert!(matches!(
        fixture.store.join().await,
        Ok(EnrollmentJoin {
            outcome: Err(EnrollmentStoreCloseError::WorkerStopped)
        })
    ));
}
