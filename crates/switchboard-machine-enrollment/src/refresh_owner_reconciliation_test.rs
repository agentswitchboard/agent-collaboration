use std::{sync::atomic::Ordering::SeqCst, task::Poll};

use crate::{
    AccessRequest, ConditionalWrite, CredentialStoreError, EnrollmentEnd, LeaseConfigurationFault,
    LeaseUnavailable, StoredEnrollment,
};

use super::{Fixture, binding, expect_lease, generation, poll_once, refresh_secret};

#[test]
fn rotation_recovers_the_exact_binding_after_an_unknown_commit() {
    let fixture = Fixture::new();
    fixture
        .store
        .0
        .unknown_write_after_commit
        .store(true, SeqCst);
    let mut lease = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));

    expect_lease(poll_once(lease.as_mut()), 2, 1);
    assert_eq!(fixture.store.0.replace_calls.load(SeqCst), 1);
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Active(binding(2, 1))
    );
}

#[test]
fn rotation_reads_an_unchanged_generation_before_retrying_the_same_replace() {
    let fixture = Fixture::new();
    fixture
        .store
        .0
        .unknown_write_before_commit
        .store(true, SeqCst);
    let mut lease = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    let first = poll_once(lease.as_mut());

    assert!(fixture.store.0.read_calls.load(SeqCst) >= 2);
    if first.is_pending() {
        fixture.advance(1);
        expect_lease(poll_once(lease.as_mut()), 2, 1);
    } else {
        expect_lease(first, 2, 1);
    }
    assert_eq!(fixture.store.0.replace_calls.load(SeqCst), 2);
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn rotation_uses_the_reserved_second_to_read_before_reporting_unknown() {
    let fixture = Fixture::new();
    fixture.store.0.hold_write.store(true, SeqCst);
    let mut lease = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(lease.as_mut()).is_pending());
    fixture.store.0.hold_read.store(true, SeqCst);

    fixture.advance(4);
    assert!(poll_once(lease.as_mut()).is_pending());
    assert_eq!(fixture.store.0.read_calls.load(SeqCst), 2);

    fixture.advance(1);
    assert_eq!(
        poll_once(lease.as_mut()),
        Poll::Ready(Err(LeaseUnavailable::Store(
            CredentialStoreError::OutcomeNotKnown
        )))
    );
    assert_eq!(fixture.store.0.replace_calls.load(SeqCst), 1);
    assert_eq!(fixture.store.0.marker_calls.load(SeqCst), 0);
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Active(binding(1, 0))
    );
}

#[test]
fn rotation_does_not_claim_a_different_credential_at_the_attempted_generation() {
    let fixture = Fixture::new();
    fixture.store.0.hold_write.store(true, SeqCst);
    let mut lease = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(lease.as_mut()).is_pending());
    assert_eq!(
        fixture
            .store
            .write(generation(1), StoredEnrollment::Active(binding(2, 7))),
        ConditionalWrite::Applied
    );

    fixture.advance(4);
    assert_eq!(
        poll_once(lease.as_mut()),
        Poll::Ready(Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::RefreshRefused
        }))
    );
    assert_eq!(
        fixture.server.sent(),
        vec![refresh_secret(0), refresh_secret(7)]
    );
}

#[test]
fn ending_recovers_remove_before_trying_the_marker() {
    let fixture = Fixture::new();
    let key = fixture.store.snapshot().key().expect("enrolled identity");
    fixture.store.0.hold_remove_ack.store(true, SeqCst);
    let mut ending = std::pin::pin!(fixture.owner.end_enrollment(&key, EnrollmentEnd::Revoked));
    assert!(poll_once(ending.as_mut()).is_pending());

    fixture.advance(1);
    assert_eq!(
        poll_once(ending.as_mut()),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );
    assert_eq!(fixture.store.0.marker_calls.load(SeqCst), 0);
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Ended {
            generation: generation(2),
            key: key.clone(),
            why: EnrollmentEnd::Revoked,
        }
    );
}

#[test]
fn ending_does_not_submit_a_marker_while_the_remove_remains_unresolved() {
    let fixture = Fixture::new();
    let key = fixture.store.snapshot().key().expect("enrolled identity");
    fixture.store.0.hold_remove.store(true, SeqCst);
    let mut ending = std::pin::pin!(fixture.owner.end_enrollment(&key, EnrollmentEnd::Revoked));
    assert!(poll_once(ending.as_mut()).is_pending());
    fixture.store.0.hold_read.store(true, SeqCst);

    fixture.advance(1);
    assert!(poll_once(ending.as_mut()).is_pending());
    assert_eq!(fixture.store.0.read_calls.load(SeqCst), 2);
    assert_eq!(fixture.store.0.marker_calls.load(SeqCst), 0);
    fixture.advance(1);
    assert_eq!(
        poll_once(ending.as_mut()),
        Poll::Ready(Err(CredentialStoreError::CleanupTimedOut))
    );
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Active(binding(1, 0))
    );
}

#[test]
fn maximum_active_generation_can_end_without_spending_another_refresh() {
    let fixture = Fixture::new();
    let last_active = u64::try_from(i64::MAX).expect("SQLite maximum") - 1;
    let active = binding(last_active, 0);
    let key = active.key();
    *fixture.store.0.saved.lock().expect("fake store") = StoredEnrollment::Active(active);

    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        Poll::Ready(Err(LeaseConfigurationFault::GenerationExhausted.into()))
    );
    assert!(fixture.server.sent().is_empty());
    assert_eq!(
        poll_once(std::pin::pin!(
            fixture.owner.end_enrollment(&key, EnrollmentEnd::Revoked)
        )),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Ended {
            generation: generation(last_active + 1),
            key,
            why: EnrollmentEnd::Revoked,
        }
    );
}

#[test]
fn a_lease_after_enrollment_replacement_carries_the_new_key() {
    let fixture = Fixture::new();
    let original = fixture.store.snapshot().key().expect("original enrollment");
    let first = poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable)));
    assert!(matches!(first, Poll::Ready(Ok(lease)) if lease.key == original));

    let mut replacement = binding(3, 1);
    replacement.credential.enrollment_id = "019a0000-0000-7000-8000-000000000010"
        .parse()
        .expect("replacement enrollment");
    let replacement_key = replacement.key();
    assert_eq!(
        fixture
            .store
            .write(generation(2), StoredEnrollment::Active(replacement)),
        ConditionalWrite::Applied
    );

    let next = poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable)));
    assert!(matches!(
        next,
        Poll::Ready(Ok(lease)) if lease.key == replacement_key
            && lease.binding_generation == generation(4)
    ));
}

#[test]
fn later_durable_ending_is_confirmed_after_remove_timeout() {
    exercise_later_durable_ending(false);
}

#[test]
fn later_durable_ending_is_confirmed_after_unknown_remove_result() {
    exercise_later_durable_ending(true);
}

fn exercise_later_durable_ending(unknown: bool) {
    for (same_key, same_reason) in [(true, true), (false, true), (true, false)] {
        let fixture = Fixture::new();
        let original = fixture.store.snapshot().key().expect("enrolled key");
        fixture.store.0.hold_remove.store(true, SeqCst);
        let mut ending = std::pin::pin!(
            fixture
                .owner
                .end_enrollment(&original, EnrollmentEnd::Revoked)
        );
        assert!(poll_once(ending.as_mut()).is_pending());

        let mut successor = binding(2, 1);
        if !same_key {
            successor.credential.enrollment_id =
                "019a0000-0000-7000-8000-000000000008".parse().unwrap();
        }
        assert_eq!(
            fixture
                .store
                .write(generation(1), StoredEnrollment::Active(successor)),
            ConditionalWrite::Applied
        );
        let recorded = if same_reason {
            EnrollmentEnd::Revoked
        } else {
            EnrollmentEnd::Inactive
        };
        assert_eq!(
            fixture.store.end(generation(2), recorded),
            ConditionalWrite::Applied
        );
        let saved = fixture.store.snapshot();
        if unknown {
            fixture.store.0.unknown_remove.store(true, SeqCst);
            fixture.store.0.hold_remove.store(false, SeqCst);
        }
        fixture.advance(1);

        let expected = if same_key && same_reason {
            Ok(ConditionalWrite::Applied)
        } else {
            Err(CredentialStoreError::CleanupTimedOut)
        };
        assert_eq!(poll_once(ending.as_mut()), Poll::Ready(expected));
        assert_eq!(fixture.store.0.marker_calls.load(SeqCst), 0);
        assert_eq!(fixture.store.snapshot(), saved);
    }
}
