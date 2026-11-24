use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use switchboard_machine_enrollment_store::{
    EnrollmentJoin, EnrollmentObservationError, EnrollmentStoreCloseError, EnrollmentWorkerExit,
};

use super::*;

#[tokio::test]
async fn requests_after_close_are_refused_before_the_worker_runs_again() {
    let fixture = Fixture::open().await;
    let binding = fixture.binding(1, ENROLLMENT, TOKEN);
    let observer = fixture.store.clone();

    fixture.store.request_close();
    observer.request_close();
    let mut context = Context::from_waker(Waker::noop());

    // Without an intervening yield, a queued request could only be pending.
    assert_eq!(
        pin!(observer.read()).poll(&mut context),
        Poll::Ready(Err(CredentialStoreError::Read))
    );
    assert_eq!(
        pin!(observer.read_for_key(&binding.key())).poll(&mut context),
        Poll::Ready(Err(CredentialStoreError::Read))
    );
    assert_eq!(
        pin!(observer.read_metadata()).poll(&mut context),
        Poll::Ready(Err(CredentialStoreError::Read))
    );
    assert_eq!(
        pin!(observer.replace(generation(0), &binding)).poll(&mut context),
        Poll::Ready(Err(CredentialStoreError::Replace))
    );
    assert_eq!(
        observer.worker_exit().await,
        Ok(EnrollmentWorkerExit::Requested)
    );
    observer.join().await.unwrap().outcome.unwrap();
}

#[test]
fn runtime_loss_before_any_witness_reports_reaper_lost() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(Fixture::open());

    drop(runtime);

    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        pin!(fixture.store.worker_exit()).poll(&mut context),
        Poll::Ready(Err(EnrollmentObservationError::ReaperLost))
    );
    let join = pin!(fixture.store.join()).poll(&mut context);
    assert!(
        matches!(
            join,
            Poll::Ready(Err(EnrollmentObservationError::ReaperLost))
        ),
        "{join:?}"
    );
    for _ in 0..2 {
        let close = pin!(fixture.store.close()).poll(&mut context);
        assert!(
            matches!(
                close,
                Poll::Ready(Err(EnrollmentStoreCloseError::WorkerStopped))
            ),
            "{close:?}"
        );
    }
}

#[test]
fn completed_join_and_exit_stay_readable_after_the_publisher_is_gone() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(async {
        let fixture = Fixture::open().await;
        fixture.store.close().await.unwrap();
        fixture
    });

    drop(runtime);

    let mut context = Context::from_waker(Waker::noop());
    assert_eq!(
        pin!(fixture.store.worker_exit()).poll(&mut context),
        Poll::Ready(Ok(EnrollmentWorkerExit::Requested))
    );
    let join = pin!(fixture.store.join()).poll(&mut context);
    assert!(
        matches!(join, Poll::Ready(Ok(EnrollmentJoin { outcome: Ok(()) }))),
        "{join:?}"
    );
}
