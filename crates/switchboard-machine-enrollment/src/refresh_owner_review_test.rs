use std::{sync::atomic::Ordering, task::Poll};

use crate::{
    AccessRequest, ConditionalWrite, EnrollmentEnd, LeaseConfigurationFault, LeaseUnavailable,
    StoredEnrollment,
};

use super::{Fixture, binding, expect_lease, generation, poll_once, refresh_secret};

#[test]
fn lagged_waiter_rechecks_revocation_after_the_first_lease_is_delivered() {
    let fixture = Fixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut first = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    let mut lagged = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(lagged.as_mut()).is_pending());
    fixture.server.block.store(false, Ordering::SeqCst);
    expect_lease(poll_once(first.as_mut()), 2, 1);
    let key = fixture.store.snapshot().key().expect("current identity");
    assert_eq!(
        poll_once(std::pin::pin!(
            fixture.owner.end_enrollment(&key, EnrollmentEnd::Revoked)
        )),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );

    assert_eq!(
        poll_once(lagged.as_mut()),
        Poll::Ready(Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::Revoked
        }))
    );
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn lagged_waiter_uses_a_replacement_instead_of_the_predecessor_lease() {
    let fixture = Fixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut first = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    let mut lagged = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(lagged.as_mut()).is_pending());
    fixture.server.block.store(false, Ordering::SeqCst);
    expect_lease(poll_once(first.as_mut()), 2, 1);
    let mut successor = binding(3, 1);
    successor.context.organization_id = "019a0000-0000-7000-8000-000000000007"
        .parse()
        .expect("another context");
    assert_eq!(
        fixture
            .store
            .write(generation(2), StoredEnrollment::Active(successor)),
        ConditionalWrite::Applied
    );

    assert_eq!(
        poll_once(lagged.as_mut()),
        Poll::Ready(Err(LeaseConfigurationFault::BindingContext.into()))
    );
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn lagged_waiter_rechecks_elapsed_expiry() {
    let fixture = Fixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut first = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    let mut lagged = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(lagged.as_mut()).is_pending());
    fixture.server.block.store(false, Ordering::SeqCst);
    expect_lease(poll_once(first.as_mut()), 2, 1);

    fixture.advance(60);
    expect_lease(poll_once(lagged.as_mut()), 3, 2);
    assert_eq!(
        fixture.server.sent(),
        vec![refresh_secret(0), refresh_secret(1)]
    );
}

#[test]
fn a_future_generation_request_does_not_refuse_an_ordinary_caller() {
    let fixture = Fixture::new();
    fixture.store.0.hold_read.store(true, Ordering::SeqCst);
    let mut refused = std::pin::pin!(fixture.owner.lease(AccessRequest::NewerThan(generation(9))));
    let mut ordinary = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(refused.as_mut()).is_pending());
    assert!(poll_once(ordinary.as_mut()).is_pending());

    fixture.store.0.hold_read.store(false, Ordering::SeqCst);
    assert_eq!(
        poll_once(refused.as_mut()),
        Poll::Ready(Err(LeaseConfigurationFault::FutureGeneration.into()))
    );
    expect_lease(poll_once(ordinary.as_mut()), 2, 1);
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn failing_an_old_ending_does_not_fence_its_successor() {
    let fixture = Fixture::new();
    fixture.store.0.hold_remove.store(true, Ordering::SeqCst);
    fixture.store.0.fail_remove.store(true, Ordering::SeqCst);
    fixture.store.0.fail_marker.store(true, Ordering::SeqCst);
    let key = fixture.store.snapshot().key().expect("old key");
    let mut ending = std::pin::pin!(fixture.owner.end_enrollment(&key, EnrollmentEnd::Revoked));
    assert!(poll_once(ending.as_mut()).is_pending());
    let mut successor = binding(2, 0);
    successor.credential.enrollment_id = "019a0000-0000-7000-8000-000000000008"
        .parse()
        .expect("new enrollment");
    assert_eq!(
        fixture
            .store
            .write(generation(1), StoredEnrollment::Active(successor)),
        ConditionalWrite::Applied
    );
    fixture.store.0.hold_remove.store(false, Ordering::SeqCst);
    assert!(poll_once(ending.as_mut()).is_ready());
    expect_lease(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        3,
        1,
    );
}

#[test]
fn local_credential_loss_completes_when_both_cleanup_operations_stall() {
    let fixture = Fixture::new();
    fixture.store.0.fail_write.store(true, Ordering::SeqCst);
    fixture.store.0.hold_remove.store(true, Ordering::SeqCst);
    fixture.store.0.hold_marker.store(true, Ordering::SeqCst);
    let mut refresh = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(refresh.as_mut()).is_pending());
    fixture.advance(5);
    assert!(poll_once(refresh.as_mut()).is_pending());
    fixture.advance(1);
    assert!(poll_once(refresh.as_mut()).is_pending());
    fixture.advance(1);
    let lost = Poll::Ready(Err(LeaseUnavailable::Ended {
        why: EnrollmentEnd::LocalCredentialLost,
    }));
    assert_eq!(poll_once(refresh.as_mut()), lost);
    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        lost
    );
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn two_stale_access_grants_in_one_call_return_a_transient_result() {
    let fixture = Fixture::new();
    // Four stale grants cover two calls. A fifth, usable grant makes the unbounded
    // wrong build return so the call-count assertion fails without spinning.
    fixture.server.stale_answers.store(4, Ordering::SeqCst);
    for call in 1..=2 {
        let result = poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable)));
        assert_eq!(
            fixture.server.sent(),
            (0..call * 2).map(refresh_secret).collect::<Vec<_>>(),
            "one follow-up rotation per call"
        );
        assert_eq!(result, Poll::Ready(Err(LeaseUnavailable::StaleAccessGrant)));
        assert_eq!(fixture.store.token(), Some(refresh_secret(call * 2)));
    }
}
