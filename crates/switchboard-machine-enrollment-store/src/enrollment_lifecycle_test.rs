use std::task::Poll;

use super::*;

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
        EnrollmentWorkerExit::Requested
    );
    let (first_receipt, second_receipt) = tokio::join!(fixture.store.join(), fixture.store.join());
    assert!(first_receipt.outcome.is_ok(), "{first_receipt:?}");
    assert!(second_receipt.outcome.is_ok(), "{second_receipt:?}");
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
        Poll::Ready(EnrollmentWorkerExit::Autonomous)
    );
    assert_eq!(
        second_observer.as_mut().poll(&mut context),
        Poll::Ready(EnrollmentWorkerExit::Autonomous)
    );
    let mut joining = Box::pin(fixture.store.join());
    assert_queued(joining.as_mut());

    fixture.store.request_close();
    release_resources.send(()).unwrap();
    assert!(matches!(
        joining.await.outcome,
        Err(EnrollmentStoreCloseError::WorkerStopped)
    ));
    assert_eq!(
        fixture.store.worker_exit().await,
        EnrollmentWorkerExit::Autonomous
    );
    assert!(matches!(
        fixture.store.join().await.outcome,
        Err(EnrollmentStoreCloseError::WorkerStopped)
    ));
}
