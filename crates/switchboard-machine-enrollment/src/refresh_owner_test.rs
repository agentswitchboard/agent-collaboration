use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use hegel::generators as gs;
use router_clock::{ManualClock, RuntimeClock};

use crate::{
    BindingGeneration, CentralEndpointIds, ConditionalWrite, CredentialStore, CredentialStoreError,
    EnrollmentBinding, EnrollmentContext, EnrollmentEnd, InvalidReason, RefreshAnswer,
    RefreshCredential, SecretValue, SignInDeployment, StoreDurability, StoredEnrollment,
    TokenGrant,
};

use super::{
    AccessRequest, LeaseConfigurationFault, LeaseUnavailable, RefreshOwner, RefreshSource,
};

struct StoreState {
    saved: Mutex<StoredEnrollment>,
    hold_write: AtomicBool,
    hold_read: AtomicBool,
    hold_remove: AtomicBool,
    hold_marker: AtomicBool,
    hold_marker_ack: AtomicBool,
    fail_write: AtomicBool,
    fail_remove: AtomicBool,
    fail_marker: AtomicBool,
}

#[derive(Clone)]
struct FakeStore(Arc<StoreState>);

impl FakeStore {
    fn snapshot(&self) -> StoredEnrollment {
        self.0.saved.lock().expect("fake store").clone()
    }

    fn token(&self) -> Option<SecretValue> {
        match self.snapshot() {
            StoredEnrollment::Active(binding) => Some(binding.credential.refresh_token),
            _ => None,
        }
    }

    fn write(
        &self,
        expected: BindingGeneration,
        replacement: StoredEnrollment,
    ) -> ConditionalWrite {
        let mut saved = self.0.saved.lock().expect("fake store");
        if saved.generation() != expected {
            return ConditionalWrite::Superseded;
        }
        assert_eq!(expected.successor(), Some(replacement.generation()));
        *saved = replacement;
        ConditionalWrite::Applied
    }

    fn end(&self, expected: BindingGeneration, why: EnrollmentEnd) -> ConditionalWrite {
        let snapshot = self.snapshot();
        if snapshot.generation() != expected {
            return ConditionalWrite::Superseded;
        }
        let key = snapshot.key().expect("ending requires an enrollment");
        let generation = expected.successor().expect("fixture generation");
        self.write(
            expected,
            StoredEnrollment::Ended {
                generation,
                why,
                key,
            },
        )
    }
}

impl CredentialStore for FakeStore {
    fn durability(&self) -> StoreDurability {
        StoreDurability::Durable
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        wait_while_held(&self.0.hold_read).await;
        Ok(self.snapshot())
    }

    async fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        poll_fn(|_| {
            if self.0.hold_write.load(Ordering::SeqCst) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        if self.0.fail_write.load(Ordering::SeqCst) {
            return Err(CredentialStoreError::Replace);
        }
        Ok(self.write(expected, StoredEnrollment::Active(binding.clone())))
    }

    async fn remove(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        wait_while_held(&self.0.hold_remove).await;
        if self.0.fail_remove.load(Ordering::SeqCst) {
            Err(CredentialStoreError::Remove)
        } else {
            Ok(self.end(expected, why))
        }
    }

    async fn mark_unusable(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        wait_while_held(&self.0.hold_marker).await;
        if self.0.fail_marker.load(Ordering::SeqCst) {
            Err(CredentialStoreError::Replace)
        } else {
            let committed = self.end(expected, why);
            wait_while_held(&self.0.hold_marker_ack).await;
            Ok(committed)
        }
    }
}

async fn wait_while_held(held: &AtomicBool) {
    poll_fn(|_| {
        if held.load(Ordering::SeqCst) {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
}

struct SignInState {
    sent: Mutex<Vec<SecretValue>>,
    block: AtomicBool,
    revision: AtomicUsize,
    lose_answer: AtomicBool,
    stale_answers: AtomicUsize,
}

impl SignInState {
    fn sent(&self) -> Vec<SecretValue> {
        self.sent.lock().expect("requests").clone()
    }

    fn record(&self, token: &SecretValue) {
        self.sent.lock().expect("requests").push(token.clone());
    }
}

#[derive(Clone)]
struct FakeSignIn(Arc<SignInState>);

impl RefreshSource for FakeSignIn {
    async fn refresh(&self, credential: &RefreshCredential) -> RefreshAnswer {
        self.0.record(&credential.refresh_token);
        poll_fn(|_| {
            if self.0.block.load(Ordering::SeqCst) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;

        let current = self.0.revision.load(Ordering::SeqCst);
        if credential.refresh_token != refresh_secret(current) {
            return RefreshAnswer::Refused {
                reason: InvalidReason::GrantRefused,
            };
        }
        let next = self.0.revision.fetch_add(1, Ordering::SeqCst) + 1;
        if self.0.lose_answer.load(Ordering::SeqCst) {
            RefreshAnswer::Unavailable { retry_after: None }
        } else {
            RefreshAnswer::Rotated(TokenGrant {
                credential: RefreshCredential {
                    enrollment_id: credential.enrollment_id,
                    ..wire_credential(next)
                },
                access_token: access_secret(next),
                expires_in: if self
                    .0
                    .stale_answers
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    Duration::ZERO
                } else {
                    Duration::from_secs(60)
                },
            })
        }
    }
}

fn secret(text: String) -> SecretValue {
    SecretValue::new(text).expect("fictional fixture value")
}

fn refresh_secret(revision: usize) -> SecretValue {
    secret(format!("refresh-{revision}"))
}

fn access_secret(revision: usize) -> SecretValue {
    secret(format!("access-{revision}"))
}

fn generation(value: u64) -> BindingGeneration {
    BindingGeneration::from_persisted(value)
}

fn context() -> EnrollmentContext {
    EnrollmentContext {
        organization_id: "019a0000-0000-7000-8000-000000000001"
            .parse()
            .expect("organization"),
        deployment: SignInDeployment::new("https://signin.example/".parse().expect("origin"))
            .expect("valid deployment"),
        machine_id: "019a0000-0000-7000-8000-000000000005"
            .parse()
            .expect("machine"),
    }
}

fn wire_credential(revision: usize) -> RefreshCredential {
    RefreshCredential {
        enrollment_id: "019a0000-0000-7000-8000-000000000002"
            .parse()
            .expect("enrollment"),
        central: CentralEndpointIds {
            receiver: "019a0000-0000-7000-8000-000000000003"
                .parse()
                .expect("receiver"),
            relay: "019a0000-0000-7000-8000-000000000004"
                .parse()
                .expect("relay"),
        },
        refresh_token: refresh_secret(revision),
    }
}

fn binding(version: u64, revision: usize) -> EnrollmentBinding {
    EnrollmentBinding {
        generation: generation(version),
        context: context(),
        credential: wire_credential(revision),
    }
}

struct Fixture {
    store: FakeStore,
    server: Arc<SignInState>,
    owner: RefreshOwner<FakeStore, FakeSignIn>,
    clock: Arc<ManualClock>,
}

impl Fixture {
    fn new() -> Self {
        let store = FakeStore(Arc::new(StoreState {
            saved: Mutex::new(StoredEnrollment::Active(binding(1, 0))),
            hold_write: AtomicBool::new(false),
            hold_read: AtomicBool::new(false),
            hold_remove: AtomicBool::new(false),
            hold_marker: AtomicBool::new(false),
            hold_marker_ack: AtomicBool::new(false),
            fail_write: AtomicBool::new(false),
            fail_remove: AtomicBool::new(false),
            fail_marker: AtomicBool::new(false),
        }));
        let server = Arc::new(SignInState {
            sent: Mutex::new(Vec::new()),
            block: AtomicBool::new(false),
            revision: AtomicUsize::new(0),
            lose_answer: AtomicBool::new(false),
            stale_answers: AtomicUsize::new(0),
        });
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let owner = RefreshOwner::new(
            store.clone(),
            FakeSignIn(Arc::clone(&server)),
            Arc::clone(&clock) as Arc<dyn RuntimeClock + Send + Sync>,
            context(),
        );
        Self {
            store,
            server,
            clock,
            owner,
        }
    }

    fn advance(&self, seconds: u64) {
        self.clock
            .advance(Duration::from_secs(seconds))
            .expect("injected clock advance");
    }
}

fn poll_once<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn expect_lease(
    status: Poll<Result<super::AccessLease, LeaseUnavailable>>,
    version: u64,
    revision: usize,
) {
    assert!(
        matches!(
            status,
            Poll::Ready(Ok(lease)) if lease.token == access_secret(revision)
                && lease.binding_generation == generation(version)
        ),
        "lease must match the committed generation"
    );
}

#[test]
fn concurrent_callers_join_one_owner_and_a_newer_request_advances_the_lease() {
    let fixture = Fixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut first = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    let mut second = std::pin::pin!(fixture.owner.lease(AccessRequest::NewerThan(generation(1))));
    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
    fixture.server.block.store(false, Ordering::SeqCst);

    expect_lease(poll_once(first.as_mut()), 2, 1);
    expect_lease(poll_once(second.as_mut()), 2, 1);
    expect_lease(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        2,
        1,
    );
    expect_lease(
        poll_once(std::pin::pin!(
            fixture.owner.lease(AccessRequest::NewerThan(generation(2)))
        )),
        3,
        2,
    );
    assert_eq!(
        fixture.server.sent(),
        vec![refresh_secret(0), refresh_secret(1)]
    );
}

#[test]
fn one_of_two_store_writers_from_the_same_generation_wins() {
    let fixture = Fixture::new();
    let other_process = fixture.store.clone();
    let candidate_a = binding(2, 1);
    let candidate_b = binding(2, 2);
    let replacement = fixture.store.replace(generation(1), &candidate_a);
    let competitor = other_process.replace(generation(1), &candidate_b);

    assert_eq!(
        poll_once(std::pin::pin!(replacement)),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );
    assert_eq!(
        poll_once(std::pin::pin!(competitor)),
        Poll::Ready(Ok(ConditionalWrite::Superseded))
    );
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Active(candidate_a)
    );
}

#[test]
fn a_late_refresh_cannot_restore_an_ended_binding() {
    let fixture = Fixture::new();
    let old = fixture.store.snapshot().key().expect("enrollment");
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut refresh = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(refresh.as_mut()).is_pending());
    assert_eq!(
        poll_once(std::pin::pin!(
            fixture.owner.end_enrollment(&old, EnrollmentEnd::Revoked)
        )),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );
    let ended = fixture.store.snapshot();
    fixture.server.block.store(false, Ordering::SeqCst);

    assert_eq!(
        poll_once(refresh.as_mut()),
        Poll::Ready(Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::Revoked
        }))
    );
    assert_eq!(fixture.store.snapshot(), ended);
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn an_old_ending_closes_the_latest_version_but_never_a_replacement() {
    let fixture = Fixture::new();
    let old = fixture.store.snapshot().key().expect("enrollment");
    expect_lease(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        2,
        1,
    );
    assert_eq!(
        poll_once(std::pin::pin!(
            fixture.owner.end_enrollment(&old, EnrollmentEnd::Revoked)
        )),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );
    assert!(matches!(
        fixture.store.snapshot(),
        StoredEnrollment::Ended { generation: version, why: EnrollmentEnd::Revoked, .. } if version == generation(3)
    ));

    let mut successor = binding(4, 2);
    successor.credential.enrollment_id = "019a0000-0000-7000-8000-000000000007"
        .parse()
        .expect("successor");
    assert_eq!(
        fixture
            .store
            .write(generation(3), StoredEnrollment::Active(successor.clone())),
        ConditionalWrite::Applied
    );
    assert_eq!(
        poll_once(std::pin::pin!(
            fixture.owner.end_enrollment(&old, EnrollmentEnd::Revoked)
        )),
        Poll::Ready(Ok(ConditionalWrite::Superseded))
    );
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Active(successor)
    );
}

#[test]
fn a_late_final_refusal_never_removes_the_successor() {
    let fixture = Fixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    fixture.server.revision.store(9, Ordering::SeqCst);
    let mut refresh = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(refresh.as_mut()).is_pending());

    let mut successor = binding(2, 17);
    successor.context.organization_id = "019a0000-0000-7000-8000-000000000007"
        .parse()
        .expect("new organization");
    assert_eq!(
        fixture
            .store
            .write(generation(1), StoredEnrollment::Active(successor.clone())),
        ConditionalWrite::Applied
    );
    fixture.server.block.store(false, Ordering::SeqCst);

    assert_eq!(
        poll_once(refresh.as_mut()),
        Poll::Ready(Err(LeaseConfigurationFault::BindingContext.into()))
    );
    assert_eq!(
        fixture.store.snapshot(),
        StoredEnrollment::Active(successor)
    );
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn rotated_pair_is_held_while_only_the_local_write_retries() {
    let fixture = Fixture::new();
    fixture.store.0.fail_write.store(true, Ordering::SeqCst);
    let mut first = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    let mut waiting = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));

    assert!(poll_once(first.as_mut()).is_pending());
    assert_eq!(fixture.store.token(), Some(refresh_secret(0)));
    assert!(poll_once(waiting.as_mut()).is_pending());
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
    fixture.store.0.fail_write.store(false, Ordering::SeqCst);
    fixture.advance(1);

    expect_lease(poll_once(first.as_mut()), 2, 1);
    expect_lease(poll_once(waiting.as_mut()), 2, 1);
    assert_eq!(fixture.store.token(), Some(refresh_secret(1)));
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn exhausted_write_retry_never_reuses_the_spent_token_even_if_marking_fails() {
    let fixture = Fixture::new();
    fixture.store.0.fail_write.store(true, Ordering::SeqCst);
    fixture.store.0.fail_remove.store(true, Ordering::SeqCst);
    fixture.store.0.fail_marker.store(true, Ordering::SeqCst);
    let mut refresh = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(refresh.as_mut()).is_pending());
    fixture.advance(5);

    let lost = Poll::Ready(Err(LeaseUnavailable::Ended {
        why: EnrollmentEnd::LocalCredentialLost,
    }));
    assert_eq!(poll_once(refresh.as_mut()), lost);
    assert_eq!(fixture.store.token(), Some(refresh_secret(0)));
    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        lost
    );
    assert_eq!(fixture.server.sent(), vec![refresh_secret(0)]);
}

#[test]
fn a_durable_unusable_marker_is_observed_by_a_new_owner() {
    let fixture = Fixture::new();
    fixture.store.0.fail_remove.store(true, Ordering::SeqCst);
    let old = fixture.store.snapshot().key().expect("enrollment");
    assert_eq!(
        poll_once(std::pin::pin!(
            fixture.owner.end_enrollment(&old, EnrollmentEnd::Inactive)
        )),
        Poll::Ready(Ok(ConditionalWrite::Applied))
    );

    let restarted = RefreshOwner::new(
        fixture.store.clone(),
        FakeSignIn(Arc::clone(&fixture.server)),
        Arc::clone(&fixture.clock) as Arc<dyn RuntimeClock + Send + Sync>,
        context(),
    );
    assert_eq!(
        poll_once(std::pin::pin!(restarted.lease(AccessRequest::Usable))),
        Poll::Ready(Err(LeaseUnavailable::Ended {
            why: EnrollmentEnd::Inactive
        }))
    );
    assert!(fixture.server.sent().is_empty());
}

#[test]
fn a_lost_answer_then_a_definitive_refusal_ends_the_binding() {
    let fixture = Fixture::new();
    fixture.server.lose_answer.store(true, Ordering::SeqCst);
    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        Poll::Ready(Err(LeaseUnavailable::SignInUnreachable {
            retry_after: None
        }))
    );
    assert_eq!(fixture.store.token(), Some(refresh_secret(0)));
    let ended = Poll::Ready(Err(LeaseUnavailable::Ended {
        why: EnrollmentEnd::RefreshRefused,
    }));
    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        ended
    );
    assert!(fixture.store.token().is_none());
    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.lease(AccessRequest::Usable))),
        ended
    );
    assert_eq!(
        fixture.server.sent(),
        vec![refresh_secret(0), refresh_secret(0)]
    );
}

#[test]
fn an_expired_access_answer_keeps_the_rotated_refresh_credential() {
    let fixture = Fixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut waiting = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
    assert!(poll_once(waiting.as_mut()).is_pending());
    fixture.advance(60);
    fixture.server.block.store(false, Ordering::SeqCst);

    expect_lease(poll_once(waiting.as_mut()), 3, 2);
    assert_eq!(fixture.store.token(), Some(refresh_secret(2)));
    assert_eq!(
        fixture.server.sent(),
        vec![refresh_secret(0), refresh_secret(1)]
    );
}

#[hegel::test(seed = Some(1351), test_cases = 100)]
fn generated_refresh_callers_match_the_rotation_model(case: hegel::TestCase) {
    let fixture = Fixture::new();
    let steps = case.draw(gs::integers::<u8>().min_value(1).max_value(20));
    let (mut remote, mut disk, mut version) = (0, Some(0), 1);
    let mut expected_calls = Vec::new();

    for _ in 0..steps {
        let lose = case.draw(gs::booleans());
        let fail_write = case.draw(gs::booleans());
        let reverse_order = case.draw(gs::booleans());
        fixture.server.lose_answer.store(lose, Ordering::SeqCst);
        fixture
            .store
            .0
            .fail_write
            .store(fail_write, Ordering::SeqCst);
        fixture.server.block.store(disk.is_some(), Ordering::SeqCst);
        let mut left = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
        let mut right = std::pin::pin!(fixture.owner.lease(AccessRequest::Usable));
        let (first, second) = if reverse_order {
            (&mut right, &mut left)
        } else {
            (&mut left, &mut right)
        };

        if let Some(old_revision) = disk {
            assert!(poll_once(first.as_mut()).is_pending());
            assert!(poll_once(second.as_mut()).is_pending());
            expected_calls.push(refresh_secret(old_revision));
            fixture.server.block.store(false, Ordering::SeqCst);
            if old_revision != remote {
                disk = None;
                version += 1;
                let ended = Poll::Ready(Err(LeaseUnavailable::Ended {
                    why: EnrollmentEnd::RefreshRefused,
                }));
                assert_eq!(poll_once(first.as_mut()), ended);
                assert_eq!(poll_once(second.as_mut()), ended);
            } else {
                remote += 1;
                if lose {
                    let lost = Poll::Ready(Err(LeaseUnavailable::SignInUnreachable {
                        retry_after: None,
                    }));
                    assert_eq!(poll_once(first.as_mut()), lost);
                    assert_eq!(poll_once(second.as_mut()), lost);
                } else {
                    if fail_write {
                        assert!(poll_once(first.as_mut()).is_pending());
                        assert!(poll_once(second.as_mut()).is_pending());
                        fixture.store.0.fail_write.store(false, Ordering::SeqCst);
                        fixture.advance(1);
                    }
                    disk = Some(remote);
                    version += 1;
                    expect_lease(poll_once(first.as_mut()), version, remote);
                    expect_lease(poll_once(second.as_mut()), version, remote);
                }
            }
        } else {
            let ended = Poll::Ready(Err(LeaseUnavailable::Ended {
                why: EnrollmentEnd::RefreshRefused,
            }));
            assert_eq!(poll_once(first.as_mut()), ended);
            assert_eq!(poll_once(second.as_mut()), ended);
        }
        assert_eq!(fixture.store.snapshot().generation(), generation(version));
        assert_eq!(fixture.store.token(), disk.map(refresh_secret));
        assert_eq!(fixture.server.sent(), expected_calls);
        fixture.advance(60);
    }
}

#[path = "refresh_owner_review_test.rs"]
mod review;
