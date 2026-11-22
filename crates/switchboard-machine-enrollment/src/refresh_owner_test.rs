use std::{
    future::{Future, poll_fn, ready},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use hegel::generators as gs;
use router_clock::ManualClock;

use crate::{
    CredentialStore, CredentialStoreError, EnrollmentEnd, InvalidReason, RefreshAnswer,
    RefreshCredential, SecretValue, TokenGrant,
};

use super::{BearerUnavailable, RefreshOwner, RefreshSource};

#[derive(Default)]
struct StoreState {
    credential: Mutex<Option<RefreshCredential>>,
    hold_writes: AtomicBool,
    fail_writes: AtomicBool,
}

impl StoreState {
    fn current(&self) -> Option<RefreshCredential> {
        self.credential.lock().expect("fake store").clone()
    }

    fn write(&self, credential: Option<RefreshCredential>) {
        *self.credential.lock().expect("fake store") = credential;
    }

    fn current_token(&self) -> Option<String> {
        self.current()
            .map(|c| c.refresh_token.expose_secret().to_owned())
    }
}

#[derive(Clone)]
struct FakeStore(Arc<StoreState>);

impl CredentialStore for FakeStore {
    fn read(
        &self,
    ) -> impl Future<Output = Result<Option<RefreshCredential>, CredentialStoreError>> + Send {
        ready(Ok(self.0.current()))
    }

    async fn replace(&self, credential: RefreshCredential) -> Result<(), CredentialStoreError> {
        poll_fn(|_| {
            if self.0.hold_writes.load(Ordering::SeqCst) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        if self.0.fail_writes.load(Ordering::SeqCst) {
            return Err(CredentialStoreError::Replace);
        }
        self.0.write(Some(credential));
        Ok(())
    }

    fn remove(&self) -> impl Future<Output = Result<(), CredentialStoreError>> + Send {
        self.0.write(None);
        ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
enum ServerStep {
    Rotate,
    Lost,
}

struct SignInState {
    calls: AtomicUsize,
    block: AtomicBool,
    version: AtomicUsize,
    step: Mutex<ServerStep>,
}

impl SignInState {
    fn expected_step(&self) -> ServerStep {
        *self.step.lock().expect("script step")
    }

    fn set_step(&self, step: ServerStep) {
        *self.step.lock().expect("script step") = step;
    }
}

#[derive(Clone)]
struct FakeSignIn(Arc<SignInState>);

impl RefreshSource for FakeSignIn {
    async fn refresh(&self, supplied: &RefreshCredential) -> RefreshAnswer {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        poll_fn(|_| {
            if self.0.block.load(Ordering::SeqCst) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
        let current = self.0.version.load(Ordering::SeqCst);
        if *supplied != credential(current) {
            return RefreshAnswer::Refused {
                reason: InvalidReason::GrantRefused,
            };
        }
        let next = self.0.version.fetch_add(1, Ordering::SeqCst) + 1;
        match self.0.expected_step() {
            ServerStep::Rotate => RefreshAnswer::Rotated(TokenGrant {
                credential: credential(next),
                access_token: secret(format!("access-{next}")),
                expires_in: Duration::from_secs(1),
            }),
            ServerStep::Lost => RefreshAnswer::Unavailable { retry_after: None },
        }
    }
}

fn secret(value: String) -> SecretValue {
    SecretValue::new(value).expect("fixture secret")
}

fn credential(revision: usize) -> RefreshCredential {
    RefreshCredential {
        enrollment_id: "019a0000-0000-7000-8000-000000000002"
            .parse()
            .expect("enrollment"),
        central: crate::CentralEndpointIds {
            receiver: "019a0000-0000-7000-8000-000000000003"
                .parse()
                .expect("receiver"),
            relay: "019a0000-0000-7000-8000-000000000004"
                .parse()
                .expect("relay"),
        },
        refresh_token: secret(format!("refresh-{revision}")),
    }
}

struct RefreshFixture {
    store: Arc<StoreState>,
    server: Arc<SignInState>,
    owner: RefreshOwner<FakeStore, FakeSignIn>,
    clock: Arc<ManualClock>,
}

impl RefreshFixture {
    fn new() -> Self {
        let store = Arc::new(StoreState::default());
        store.write(Some(credential(0)));
        let server = Arc::new(SignInState {
            calls: AtomicUsize::new(0),
            block: AtomicBool::new(false),
            version: AtomicUsize::new(0),
            step: Mutex::new(ServerStep::Rotate),
        });
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let owner = RefreshOwner::new(
            FakeStore(Arc::clone(&store)),
            FakeSignIn(Arc::clone(&server)),
            Arc::clone(&clock) as Arc<dyn router_clock::RuntimeClock + Send + Sync>,
        );
        Self {
            store,
            server,
            owner,
            clock,
        }
    }
}

fn poll_once<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn concurrent_bearer_requests_share_the_same_refresh() {
    let fixture = RefreshFixture::new();
    fixture.server.block.store(true, Ordering::SeqCst);
    let mut first = std::pin::pin!(fixture.owner.bearer());
    let mut second = std::pin::pin!(fixture.owner.bearer());

    assert!(poll_once(first.as_mut()).is_pending());
    assert!(poll_once(second.as_mut()).is_pending());
    assert_eq!(fixture.server.calls.load(Ordering::SeqCst), 1);
    fixture.server.block.store(false, Ordering::SeqCst);

    let first_result = poll_once(first.as_mut());
    let second_result = poll_once(second.as_mut());
    assert_ne!(first_result, Poll::Pending);
    assert_eq!(first_result, second_result);
    assert!(
        matches!(first_result, Poll::Ready(Ok(bearer)) if bearer.access_token.expose_secret() == "access-1")
    );
}

#[test]
fn a_new_bearer_is_still_pending_during_the_store_commit() {
    let fixture = RefreshFixture::new();
    fixture.store.hold_writes.store(true, Ordering::SeqCst);
    let mut waiting = std::pin::pin!(fixture.owner.bearer());

    assert!(poll_once(waiting.as_mut()).is_pending());
    assert_eq!(fixture.store.current_token().as_deref(), Some("refresh-0"));
    fixture.store.hold_writes.store(false, Ordering::SeqCst);

    assert!(
        matches!(poll_once(waiting.as_mut()), Poll::Ready(Ok(bearer)) if bearer.access_token.expose_secret() == "access-1")
    );
    assert_eq!(fixture.store.current_token().as_deref(), Some("refresh-1"));
}

#[test]
fn a_failed_write_never_releases_the_new_access_token() {
    let fixture = RefreshFixture::new();
    fixture.store.fail_writes.store(true, Ordering::SeqCst);
    let result = poll_once(std::pin::pin!(fixture.owner.bearer()));

    assert_eq!(
        result,
        Poll::Ready(Err(BearerUnavailable::Store(CredentialStoreError::Replace)))
    );
    assert_eq!(fixture.store.current_token().as_deref(), Some("refresh-0"));
}

#[test]
fn a_lost_reply_and_final_refusal_end_and_remove_the_credential() {
    let fixture = RefreshFixture::new();
    fixture.server.set_step(ServerStep::Lost);
    assert_eq!(
        poll_once(std::pin::pin!(fixture.owner.bearer())),
        Poll::Ready(Err(BearerUnavailable::SignInUnreachable {
            retry_after: None
        }))
    );
    assert_eq!(fixture.store.current_token().as_deref(), Some("refresh-0"));
    let ended = Poll::Ready(Err(BearerUnavailable::Ended {
        why: EnrollmentEnd::RefreshRefused,
        cleanup: Ok(()),
    }));

    assert_eq!(poll_once(std::pin::pin!(fixture.owner.bearer())), ended);
    assert_eq!(fixture.store.current_token(), None);
    assert_eq!(poll_once(std::pin::pin!(fixture.owner.bearer())), ended);
    assert_eq!(fixture.server.calls.load(Ordering::SeqCst), 2);
}

#[derive(Debug, Eq, PartialEq)]
enum ModelResult {
    Bearer(usize),
    Lost,
    WriteFailed,
    Ended,
}

#[hegel::test(seed = Some(1351), test_cases = 100)]
fn generated_refresh_callers_match_the_rotation_model(case: hegel::TestCase) {
    let fixture = RefreshFixture::new();
    let rounds = case.draw(gs::integers::<u8>().min_value(1).max_value(15));
    let (mut server_version, mut disk_version, mut calls) = (0, Some(0), 0);
    let mut ended = false;

    for _ in 0..rounds {
        let order = case.draw(gs::booleans());
        let lose = case.draw(gs::booleans());
        let fail_write = case.draw(gs::booleans());
        fixture.server.set_step(if lose {
            ServerStep::Lost
        } else {
            ServerStep::Rotate
        });
        fixture
            .store
            .fail_writes
            .store(fail_write, Ordering::SeqCst);
        fixture.server.block.store(!ended, Ordering::SeqCst);
        let mut left = std::pin::pin!(fixture.owner.bearer());
        let mut right = std::pin::pin!(fixture.owner.bearer());
        let (first, second) = if order {
            (&mut left, &mut right)
        } else {
            (&mut right, &mut left)
        };
        if !ended {
            assert!(poll_once(first.as_mut()).is_pending());
            assert!(poll_once(second.as_mut()).is_pending());
        }
        fixture.server.block.store(false, Ordering::SeqCst);

        let model = if ended {
            ModelResult::Ended
        } else {
            calls += 1;
            if disk_version != Some(server_version) {
                disk_version = None;
                ended = true;
                ModelResult::Ended
            } else {
                server_version += 1;
                if lose {
                    ModelResult::Lost
                } else if fail_write {
                    ModelResult::WriteFailed
                } else {
                    disk_version = Some(server_version);
                    ModelResult::Bearer(server_version)
                }
            }
        };
        let observe = |outcome| match outcome {
            Poll::Ready(Ok(bearer)) => {
                let super::AccessBearer { access_token, .. } = bearer;
                let revision = access_token
                    .expose_secret()
                    .strip_prefix("access-")
                    .expect("bearer marker")
                    .parse()
                    .expect("bearer version");
                ModelResult::Bearer(revision)
            }
            Poll::Ready(Err(BearerUnavailable::SignInUnreachable { .. })) => ModelResult::Lost,
            Poll::Ready(Err(BearerUnavailable::Store(CredentialStoreError::Replace))) => {
                ModelResult::WriteFailed
            }
            Poll::Ready(Err(BearerUnavailable::Ended {
                cleanup: Ok(()), ..
            })) => ModelResult::Ended,
            other => panic!("unexpected refresh result {other:?}"),
        };
        assert_eq!(observe(poll_once(first.as_mut())), model);
        assert_eq!(observe(poll_once(second.as_mut())), model);
        let expected_disk_token = disk_version.map(|revision| format!("refresh-{revision}"));
        assert_eq!(fixture.store.current_token(), expected_disk_token);
        assert_eq!(fixture.server.calls.load(Ordering::SeqCst), calls);
        fixture
            .clock
            .advance(Duration::from_secs(1))
            .expect("advance fixture clock");
    }
}
