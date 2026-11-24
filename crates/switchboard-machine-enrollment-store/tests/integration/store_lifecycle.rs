use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use switchboard_machine_enrollment_store::EnrollmentWorkerExit;

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
        EnrollmentWorkerExit::Requested
    );
    observer.join().await.outcome.unwrap();
}
