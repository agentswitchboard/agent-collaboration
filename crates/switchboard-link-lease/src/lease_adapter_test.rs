use std::{future::poll_fn, task::Poll, time::Duration};

use router_clock::{ClockInstant, ManualClock};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStoreError, EnrollmentBinding,
    EnrollmentContext, EnrollmentEnd as StoredEnding, EnrollmentMetadata, InvalidReason,
    RefreshAnswer, RefreshCredential, SecretValue, SignInDeployment, StoreDurability,
    StoredEnrollment, TokenGrant,
};

use super::*;

#[derive(Clone)]
pub(crate) struct RowStore {
    pub(crate) row: watch::Sender<StoredEnrollment>,
    pub(crate) fail_end: watch::Sender<bool>,
    pub(crate) fail_metadata: watch::Sender<bool>,
}

impl CredentialStore for RowStore {
    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        Ok(self.row.borrow().clone())
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        if *self.fail_metadata.borrow() {
            return Err(CredentialStoreError::Read);
        }
        Ok(self.row.borrow().clone().into())
    }

    fn durability(&self) -> StoreDurability {
        StoreDurability::Durable
    }

    async fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let accepted = self.row.send_if_modified(|saved| {
            if saved.generation() != expected {
                return false;
            }
            *saved = StoredEnrollment::Active(binding.clone());
            true
        });
        Ok(if accepted {
            ConditionalWrite::Applied
        } else {
            ConditionalWrite::Superseded
        })
    }

    async fn remove(
        &self,
        expected: BindingGeneration,
        why: StoredEnding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        if *self.fail_end.borrow() {
            return Err(CredentialStoreError::Remove);
        }
        let accepted = self.row.send_if_modified(|saved| {
            if saved.generation() != expected {
                return false;
            }
            let (StoredEnrollment::Active(binding), Some(generation)) =
                (&*saved, expected.successor())
            else {
                return false;
            };
            *saved = StoredEnrollment::Ended {
                key: binding.key(),
                generation,
                why,
            };
            true
        });
        Ok(if accepted {
            ConditionalWrite::Applied
        } else {
            ConditionalWrite::Superseded
        })
    }

    async fn mark_unusable(
        &self,
        expected: BindingGeneration,
        why: StoredEnding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        self.remove(expected, why).await
    }
}

struct SignInScript {
    reached: mpsc::UnboundedSender<oneshot::Sender<RefreshAnswer>>,
}

impl RefreshSource for SignInScript {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        let (reply, answer) = oneshot::channel();
        self.reached.send(reply).expect("test owns sign-in script");
        answer.await.expect("test supplies sign-in response")
    }
}

pub(crate) struct Fixture {
    pub(crate) source: EnrollmentLeaseSource,
    pub(crate) store: RowStore,
    pub(crate) clock: Arc<ManualClock>,
    refreshes: mpsc::UnboundedReceiver<oneshot::Sender<RefreshAnswer>>,
    pub(crate) binding: EnrollmentBinding,
}

impl Fixture {
    pub(crate) fn new() -> Self {
        let deployment =
            SignInDeployment::new("https://sign-in.example/".parse().unwrap()).unwrap();
        let context = EnrollmentContext {
            deployment,
            organization_id: "019a0000-0000-7000-8000-000000000001".parse().unwrap(),
            machine_id: "019a0000-0000-7000-8000-000000000002".parse().unwrap(),
        };
        let binding = EnrollmentBinding {
            generation: BindingGeneration::from_persisted(1).unwrap(),
            context: context.clone(),
            credential: RefreshCredential {
                enrollment_id: "019a0000-0000-7000-8000-000000000003".parse().unwrap(),
                refresh_token: SecretValue::new("initial-fictional-refresh".into()).unwrap(),
            },
        };
        let store = RowStore {
            row: watch::channel(StoredEnrollment::Active(binding.clone())).0,
            fail_end: watch::channel(false).0,
            fail_metadata: watch::channel(false).0,
        };
        let clock = Arc::new(ManualClock::new(Default::default()));
        let (reached, refreshes) = mpsc::unbounded_channel();
        let owner = Arc::new(RefreshOwner::new(
            store.clone(),
            SignInScript { reached },
            clock.clone(),
            context,
        ));
        let source = EnrollmentLeaseSource::start(owner, clock.clone());
        Self {
            source,
            store,
            clock,
            refreshes,
            binding,
        }
    }

    fn rotated(&self) -> RefreshAnswer {
        RefreshAnswer::Rotated(TokenGrant {
            credential: RefreshCredential {
                enrollment_id: self.binding.credential.enrollment_id,
                refresh_token: SecretValue::new("rotated-fictional-refresh".into()).unwrap(),
            },
            access_token: SecretValue::new("fictional-access-token".into()).unwrap(),
            expires_in: Duration::from_secs(60),
        })
    }
}

async fn start_acquire<'a>(
    source: &'a EnrollmentLeaseSource,
    key: &'a EnrollmentKey,
) -> LeaseFuture<'a, EnrollmentKey> {
    let mut acquire = source.acquire(key, None);
    poll_fn(|context| {
        assert!(acquire.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    acquire
}

#[tokio::test]
async fn dropping_the_caller_drains_the_started_refresh_into_storage() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let acquire = start_acquire(&fixture.source, &key).await;
    let reply = fixture.refreshes.recv().await.unwrap();
    drop(acquire);
    reply.send(fixture.rotated()).unwrap();

    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert_eq!(fixture.store.row.borrow().generation().get(), 2);
}

#[tokio::test]
async fn close_bounds_a_held_refresh_and_lists_its_key() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let acquire = start_acquire(&fixture.source, &key).await;
    let held_sign_in = fixture.refreshes.recv().await.unwrap();
    let mut close = std::pin::pin!(fixture.source.close());
    tokio::select! {
        result = &mut close => panic!("close returned before its deadline: {result:?}"),
        () = fixture.clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5))) => {}
    }
    fixture.clock.advance(Duration::from_secs(5)).unwrap();

    assert_eq!(close.await.unwrap().acquisitions, vec![key.clone()]);
    assert_eq!(acquire.await, Err(LeaseFailure::Fault));
    assert!(held_sign_in.is_closed());
}

#[tokio::test]
async fn end_fences_immediately_retries_and_keeps_the_first_cause() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    let mut row = fixture.store.row.subscribe();
    fixture.store.fail_end.send_replace(true);
    fixture.source.end(&key, EnrollmentEnd::Inactive);
    fixture.source.end(&key, EnrollmentEnd::Revoked);

    assert_eq!(
        fixture.source.acquire(&key, None).await,
        Err(LeaseFailure::EnrollmentEnded)
    );
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5)))
        .await;
    fixture.store.fail_end.send_replace(false);
    fixture.clock.advance(Duration::from_secs(5)).unwrap();

    let terminal = row
        .wait_for(|saved| matches!(saved, StoredEnrollment::Ended { .. }))
        .await
        .unwrap()
        .clone();
    assert!(matches!(
        terminal,
        StoredEnrollment::Ended {
            why: StoredEnding::Inactive,
            ..
        }
    ));
    assert_eq!(
        fixture.source.acquire(&key, None).await,
        Err(LeaseFailure::EnrollmentEnded)
    );
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
}

#[tokio::test]
async fn closing_reports_retirements_whose_final_attempt_cannot_write() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    fixture.store.fail_end.send_replace(true);
    fixture.source.end(&key, EnrollmentEnd::Revoked);
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5)))
        .await;

    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork {
            acquisitions: vec![],
            retirements: vec![key],
        }
    );
}

#[tokio::test]
async fn a_stale_refusal_releases_the_fence_for_the_same_keys_newer_generation() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let mut retirement = fixture.source.state.subscribe();
    fixture.store.fail_end.send_replace(true);

    let first = start_acquire(&fixture.source, &key).await;
    fixture
        .refreshes
        .recv()
        .await
        .unwrap()
        .send(RefreshAnswer::Refused {
            reason: InvalidReason::GrantRefused,
        })
        .unwrap();
    assert_eq!(first.await, Err(LeaseFailure::EnrollmentEnded));

    let mut successor = fixture.binding.clone();
    successor.generation = successor.generation.successor().unwrap();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(successor));
    fixture.store.fail_end.send_replace(false);
    fixture.source.end(&key, EnrollmentEnd::LeaseRefused);
    let mut row = fixture.store.row.subscribe();
    tokio::select! {
        biased;
        changed = retirement.wait_for(|state| state.retirements.is_empty()) => {
            changed.expect("adapter drops the stale fence");
        }
        _ = row.wait_for(|saved| matches!(saved, StoredEnrollment::Ended { .. })) => {
            panic!("the stale refusal retired a newer active generation");
        }
    }

    let second = start_acquire(&fixture.source, &key).await;
    fixture
        .refreshes
        .recv()
        .await
        .unwrap()
        .send(fixture.rotated())
        .unwrap();
    assert_eq!(second.await.unwrap().window.version, LeaseVersion(3));
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
}

#[tokio::test]
async fn central_ended_cause_preserves_the_distinct_stored_reason() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    fixture
        .source
        .end(&key, EnrollmentEnd::ConnectionEnrollmentEnded);
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert_eq!(
        fixture.store.row.borrow().clone(),
        StoredEnrollment::Ended {
            key,
            generation: BindingGeneration::from_persisted(2).unwrap(),
            why: StoredEnding::EndedByCentral,
        }
    );
}

#[tokio::test]
async fn a_queued_acquire_does_not_start_refresh_after_the_key_is_fenced() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let (reply, result) = oneshot::channel();
    fixture
        .source
        .commands
        .send(LeaseCommand::Acquire {
            key: key.clone(),
            newer_than: None,
            reply,
        })
        .unwrap();
    fixture
        .source
        .state
        .send_modify(|state| state.retirements.push(key));

    tokio::select! {
        result = result => assert_eq!(result.unwrap(), Err(LeaseFailure::EnrollmentEnded)),
        request = fixture.refreshes.recv() => panic!("the queued acquire reached sign-in: {}", request.is_some()),
    }
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
}
