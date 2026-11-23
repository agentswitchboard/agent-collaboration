use std::{future::poll_fn, sync::Mutex, task::Poll, time::Duration};
use switchboard_machine_enrollment::{EnrollmentKey, KeyedEnrollment};

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
    end_attempts: watch::Sender<u64>,
    pub(crate) fail_metadata: watch::Sender<bool>,
    credential_read: OperationCheckpoint,
    pub(crate) metadata_read: OperationCheckpoint,
    credential_replace: OperationCheckpoint,
}

pub(crate) type ResumeOperation = Box<dyn FnOnce() + Send>;

struct HeldOperation {
    entered: oneshot::Sender<()>,
    resume: oneshot::Receiver<ResumeOperation>,
}

#[derive(Clone, Default)]
pub(crate) struct OperationCheckpoint(Arc<Mutex<Option<HeldOperation>>>);

impl OperationCheckpoint {
    pub(crate) fn hold_next(&self) -> (oneshot::Receiver<()>, oneshot::Sender<ResumeOperation>) {
        let (entered, observed) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        let previous = self
            .0
            .lock()
            .unwrap()
            .replace(HeldOperation { entered, resume });
        assert!(previous.is_none());
        (observed, release)
    }

    async fn pause_if_held(&self) {
        let held = self.0.lock().unwrap().take();
        if let Some(held) = held {
            held.entered.send(()).unwrap();
            held.resume.await.expect("test resumes the operation")();
        }
    }
}

impl CredentialStore for RowStore {
    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        let snapshot = self.row.borrow().clone();
        self.credential_read.pause_if_held().await;
        Ok(snapshot)
    }

    async fn read_for_key(
        &self,
        requested: &EnrollmentKey,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        self.read()
            .await
            .map(|snapshot| KeyedEnrollment::from_snapshot(snapshot, requested))
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        if *self.fail_metadata.borrow() {
            return Err(CredentialStoreError::Read);
        }
        let snapshot = self.row.borrow().clone().into();
        self.metadata_read.pause_if_held().await;
        Ok(snapshot)
    }

    fn durability(&self) -> StoreDurability {
        StoreDurability::Durable
    }

    async fn replace(
        &self,
        expected: BindingGeneration,
        binding: &EnrollmentBinding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        self.credential_replace.pause_if_held().await;
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
        self.end_attempts.send_modify(|attempts| *attempts += 1);
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
    retirements: RetirementFence,
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
            end_attempts: watch::channel(0).0,
            fail_metadata: watch::channel(false).0,
            credential_read: OperationCheckpoint::default(),
            metadata_read: OperationCheckpoint::default(),
            credential_replace: OperationCheckpoint::default(),
        };
        let clock = Arc::new(ManualClock::new(Default::default()));
        let (reached, refreshes) = mpsc::unbounded_channel();
        let owner = RefreshOwner::new(
            store.clone(),
            SignInScript { reached },
            clock.clone(),
            context,
        );
        let retirements = owner.retirement_fence();
        let source = EnrollmentLeaseSource::start(owner, clock.clone());
        Self {
            source,
            store,
            clock,
            refreshes,
            binding,
            retirements,
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

    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5)))
        .await;
    let mut successor = fixture.binding.clone();
    successor.generation = successor.generation.successor().unwrap();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(successor));
    fixture.store.fail_end.send_replace(false);
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    let mut row = fixture.store.row.subscribe();
    tokio::select! {
        biased;
        _ = row.wait_for(|saved| matches!(saved, StoredEnrollment::Ended { .. })) => {
            panic!("the stale refusal retired a newer active generation");
        }
        () = async {
            while fixture.source.retirements.is_fenced(&key) {
                tokio::task::yield_now().await;
            }
        } => {}
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
        .send(LeaseCommand {
            key: key.clone(),
            newer_than: None,
            reply,
        })
        .unwrap();
    fixture.source.end(&key, EnrollmentEnd::Revoked);

    tokio::select! {
        result = result => assert_eq!(result.unwrap(), Err(LeaseFailure::EnrollmentEnded)),
        request = fixture.refreshes.recv() => panic!("the queued acquire reached sign-in: {}", request.is_some()),
    }
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
}

#[tokio::test]
async fn a_dispatched_acquire_cannot_start_sign_in_when_its_held_read_returns_after_end() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let source = Arc::new(fixture.source);
    let (reading, release_read) = fixture.store.credential_read.hold_next();
    let acquire = start_acquire(&source, &key).await;
    reading.await.unwrap();

    let source_at_read_return = source.clone();
    let ending_key = key.clone();
    assert!(
        release_read
            .send(Box::new(move || {
                source_at_read_return.end(&ending_key, EnrollmentEnd::Revoked);
            }))
            .is_ok()
    );

    tokio::select! {
        result = acquire => assert_eq!(result, Err(LeaseFailure::EnrollmentEnded)),
        request = fixture.refreshes.recv() => panic!("sign-in started after end: {}", request.is_some()),
    }
    assert_eq!(
        source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
}

#[tokio::test]
async fn a_definitive_end_waiting_behind_a_stale_refusal_keeps_the_key_fenced() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
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

    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5)))
        .await;
    let mut successor = fixture.binding.clone();
    successor.generation = successor.generation.successor().unwrap();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(successor));
    fixture.store.fail_end.send_replace(false);

    let (checking_staleness, release_read) = fixture.store.metadata_read.hold_next();
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    checking_staleness.await.unwrap();
    fixture.source.end(&key, EnrollmentEnd::Revoked);
    assert_eq!(
        fixture.source.acquire(&key, None).await,
        Err(LeaseFailure::EnrollmentEnded)
    );
    assert!(release_read.send(Box::new(|| {})).is_ok());

    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert_eq!(
        fixture.store.row.borrow().clone(),
        StoredEnrollment::Ended {
            key: key.clone(),
            generation: BindingGeneration::from_persisted(3).unwrap(),
            why: StoredEnding::Revoked,
        }
    );
    assert_eq!(
        fixture.source.acquire(&key, None).await,
        Err(LeaseFailure::EnrollmentEnded)
    );
}

#[tokio::test]
async fn forced_close_abandons_a_rotated_credential_held_before_storage() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let (writing, release_write) = fixture.store.credential_replace.hold_next();
    let acquire = start_acquire(&fixture.source, &key).await;
    let held_sign_in = fixture.refreshes.recv().await.unwrap();

    let mut close = std::pin::pin!(fixture.source.close());
    tokio::select! {
        result = &mut close => panic!("close returned before its deadline: {result:?}"),
        () = fixture.clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5))) => {}
    }
    fixture.clock.advance(Duration::from_secs(4)).unwrap();
    held_sign_in.send(fixture.rotated()).unwrap();
    writing.await.unwrap();
    fixture.clock.advance(Duration::from_secs(1)).unwrap();
    assert_eq!(close.await.unwrap().acquisitions, vec![key.clone()]);
    assert_eq!(acquire.await, Err(LeaseFailure::Fault));

    assert!(release_write.send(Box::new(|| {})).is_err());
    assert_eq!(fixture.store.row.borrow().generation().get(), 1);
}

#[tokio::test]
async fn close_uses_the_first_call_deadline_even_if_the_supervisor_polls_late() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    let acquire = start_acquire(&fixture.source, &key).await;
    let held_sign_in = fixture.refreshes.recv().await.unwrap();

    let mut first_close = std::pin::pin!(fixture.source.close());
    poll_fn(|context| {
        assert!(first_close.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    fixture.clock.advance(Duration::from_secs(6)).unwrap();
    let repeat_close = fixture.source.close();

    let (first, repeated) = tokio::select! {
        reports = async { tokio::join!(&mut first_close, repeat_close) } => reports,
        () = fixture.clock.not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(11))) => {
            panic!("the supervisor gave close another five seconds");
        }
    };
    let expected = UnfinishedLeaseWork {
        acquisitions: vec![key.clone()],
        retirements: vec![],
    };
    assert_eq!(first, Ok(expected.clone()));
    assert_eq!(repeated, Ok(expected));
    assert_eq!(acquire.await, Err(LeaseFailure::Fault));
    assert!(held_sign_in.is_closed());
}

fn refresh_refused() -> RefreshAnswer {
    RefreshAnswer::Refused {
        reason: InvalidReason::GrantRefused,
    }
}

#[tokio::test]
async fn an_owner_refusal_survives_a_dropped_acquire_and_appears_in_the_close_report() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    fixture.store.fail_end.send_replace(true);
    let mut attempts = fixture.store.end_attempts.subscribe();
    let acquired = start_acquire(&fixture.source, &key).await;
    let reply = fixture.refreshes.recv().await.unwrap();
    drop(acquired);
    reply.send(refresh_refused()).unwrap();
    attempts.wait_for(|count| *count >= 2).await.unwrap();

    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork {
            acquisitions: vec![],
            retirements: vec![key],
        }
    );
}

#[tokio::test]
async fn a_delayed_lease_refused_callback_cannot_erase_a_new_owner_refusal() {
    let mut fixture = Fixture::new();
    let key = fixture.binding.key();
    fixture.store.fail_end.send_replace(true);
    let original = start_acquire(&fixture.source, &key).await;
    fixture
        .refreshes
        .recv()
        .await
        .unwrap()
        .send(refresh_refused())
        .unwrap();
    assert_eq!(original.await, Err(LeaseFailure::EnrollmentEnded));
    fixture.source.end(&key, EnrollmentEnd::LeaseRefused);
    fixture
        .clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(5)))
        .await;

    let mut successor = fixture.binding.clone();
    successor.generation = successor.generation.successor().unwrap();
    fixture
        .store
        .row
        .send_replace(StoredEnrollment::Active(successor));
    fixture.store.fail_end.send_replace(false);
    fixture.clock.advance(Duration::from_secs(5)).unwrap();
    while fixture.retirements.is_fenced(&key) {
        tokio::task::yield_now().await;
    }
    let second = start_acquire(&fixture.source, &key).await;
    let second_answer = fixture.refreshes.recv().await.unwrap();

    fixture.source.end(&key, EnrollmentEnd::LeaseRefused);
    assert!(!fixture.retirements.is_fenced(&key));
    second_answer.send(refresh_refused()).unwrap();
    assert_eq!(second.await, Err(LeaseFailure::EnrollmentEnded));
    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork::default()
    );
    assert_eq!(
        fixture.store.row.borrow().clone(),
        StoredEnrollment::Ended {
            key,
            generation: BindingGeneration::from_persisted(3).unwrap(),
            why: StoredEnding::RefreshRefused,
        }
    );
    assert!(fixture.refreshes.try_recv().is_err());
}

#[tokio::test]
async fn close_finds_pending_retirements_admitted_by_a_retained_owner_handle() {
    let fixture = Fixture::new();
    let key = fixture.binding.key();
    fixture.store.fail_metadata.send_replace(true);
    fixture
        .retirements
        .request_definitive(&key, StoredEnding::Revoked);
    fixture.source.end(&key, EnrollmentEnd::Revoked);

    assert_eq!(
        fixture.source.close().await.unwrap(),
        UnfinishedLeaseWork {
            acquisitions: vec![],
            retirements: vec![key],
        }
    );
}
