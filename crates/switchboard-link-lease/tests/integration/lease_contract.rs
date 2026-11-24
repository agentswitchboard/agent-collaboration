use std::{
    collections::BTreeSet,
    future::{Future, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use router_clock::{ManualClock, RuntimeClock};
use switchboard_central_link::lease_source::{LeaseFailure, LeaseSource};
use switchboard_link_lease::{DiscoverySource, EnrollmentLeaseSource, LinkSettingsSource};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, DiscoveryAnswer,
    EnrollmentBinding, EnrollmentContext, EnrollmentEnd, EnrollmentKey, EnrollmentMetadata,
    KeyedEnrollment, RefreshAnswer, RefreshCredential, RefreshOwner, RefreshSource,
    SignInDeployment, SignInFailure, StoreDurability, StoredEnrollment, decode_discovery,
};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone)]
struct MetadataStore {
    row: watch::Sender<EnrollmentMetadata>,
    endings: Option<mpsc::UnboundedSender<AdmittedEnding>>,
}

struct AdmittedEnding {
    expected: BindingGeneration,
    why: EnrollmentEnd,
    reply: oneshot::Sender<ConditionalWrite>,
}

impl AdmittedEnding {
    fn commit(self, store: &MetadataStore) {
        let applied = store.row.send_if_modified(|row| {
            let EnrollmentMetadata::Active {
                key, generation, ..
            } = row
            else {
                return false;
            };
            if *generation != self.expected {
                return false;
            }
            *row = EnrollmentMetadata::Ended {
                key: key.clone(),
                generation: self.expected.successor().unwrap(),
                why: self.why,
            };
            true
        });
        assert!(applied);
        let _ = self.reply.send(ConditionalWrite::Applied);
    }
}

impl MetadataStore {
    fn new(metadata: EnrollmentMetadata) -> Self {
        Self {
            row: watch::channel(metadata).0,
            endings: None,
        }
    }
}

impl CredentialStore for MetadataStore {
    fn durability(&self) -> StoreDurability {
        StoreDurability::MemoryOnly
    }

    async fn read(&self) -> Result<StoredEnrollment, CredentialStoreError> {
        panic!("excluded lease read reached the store");
    }

    async fn read_for_key(
        &self,
        _: &EnrollmentKey,
    ) -> Result<KeyedEnrollment, CredentialStoreError> {
        panic!("excluded keyed lease read reached the store");
    }

    async fn read_metadata(&self) -> Result<EnrollmentMetadata, CredentialStoreError> {
        Ok(self.row.borrow().clone())
    }

    async fn replace(
        &self,
        _: BindingGeneration,
        _: &EnrollmentBinding,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        panic!("excluded enrollment was rotated");
    }

    async fn remove(
        &self,
        expected: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        let endings = self
            .endings
            .as_ref()
            .expect("excluded enrollment was retired again");
        let (reply, result) = oneshot::channel();
        assert!(
            endings
                .send(AdmittedEnding {
                    expected,
                    why,
                    reply
                })
                .is_ok()
        );
        Ok(result.await.unwrap())
    }

    async fn mark_unusable(
        &self,
        _: BindingGeneration,
        _: EnrollmentEnd,
    ) -> Result<ConditionalWrite, CredentialStoreError> {
        panic!("excluded enrollment was marked unusable again");
    }
}

struct NoRefresh;

impl RefreshSource for NoRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        panic!("excluded enrollment reached Sign-in");
    }
}

struct CountDiscovery(Arc<AtomicUsize>);

impl DiscoverySource for CountDiscovery {
    async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(decode_discovery(
            br#"{"client_id":"019a0000-0000-7000-8000-000000000011","link_url":"https://central.example/{organization_id}/link"}"#,
        ).unwrap())
    }
}

fn context() -> EnrollmentContext {
    EnrollmentContext {
        machine_id: "019a0000-0000-7000-8000-000000000001".parse().unwrap(),
        organization_id: "019a0000-0000-7000-8000-000000000002".parse().unwrap(),
        deployment: SignInDeployment::new("https://sign-in.example/".parse().unwrap()).unwrap(),
    }
}

fn key(context: &EnrollmentContext, id: &str) -> EnrollmentKey {
    EnrollmentKey {
        machine_id: context.machine_id.clone(),
        organization_id: context.organization_id,
        enrollment_id: id.parse().unwrap(),
    }
}

fn active(context: &EnrollmentContext, key: &EnrollmentKey, generation: u64) -> EnrollmentMetadata {
    EnrollmentMetadata::Active {
        key: key.clone(),
        deployment: context.deployment.clone(),
        generation: BindingGeneration::from_persisted(generation).unwrap(),
    }
}

#[tokio::test]
async fn excluded_settings_never_discover_or_publish_after_a_store_change() {
    let context = context();
    let allowed = key(&context, "019a0000-0000-7000-8000-000000000003");
    let excluded = key(&context, "019a0000-0000-7000-8000-000000000004");
    let store = MetadataStore::new(active(&context, &excluded, 1));
    let clock = Arc::new(ManualClock::new(Default::default()));
    let calls = Arc::new(AtomicUsize::new(0));
    let (changes, revisions) = watch::channel(0);
    let (stop, shutdown) = watch::channel(false);
    let (settings, mut published) = LinkSettingsSource::with_discovery(
        store.clone(),
        CountDiscovery(calls.clone()),
        context.clone(),
        clock.clone(),
        revisions,
        Arc::new(BTreeSet::from([excluded.enrollment_id])),
    );
    let worker = tokio::spawn(settings.run(shutdown));
    let detector = || clock.monotonic_now().after(Duration::from_secs(15));
    clock.not_due_check_now(detector()).await;
    assert!(published.borrow().binding.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    store.row.send_replace(active(&context, &allowed, 2));
    changes.send_replace(1);
    published
        .wait_for(|settings| {
            settings.binding.as_ref() == Some(&allowed) && settings.address.is_ok()
        })
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    clock.advance(Duration::from_secs(1)).unwrap();
    store.row.send_replace(active(&context, &excluded, 3));
    changes.send_replace(2);
    clock.not_due_check_now(detector()).await;
    assert!(published.borrow().binding.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    stop.send_replace(true);
    worker.await.unwrap();
}

#[tokio::test]
async fn excluded_lease_is_refused_before_acquisition_or_retirement() {
    let context = context();
    let excluded = key(&context, "019a0000-0000-7000-8000-000000000004");
    let store = MetadataStore::new(active(&context, &excluded, 1));
    let clock = Arc::new(ManualClock::new(Default::default()));
    let owner = RefreshOwner::new(store, NoRefresh, clock.clone(), context);
    let fence = owner.retirement_fence();
    let lease = EnrollmentLeaseSource::start(
        owner,
        clock.clone(),
        Arc::new(BTreeSet::from([excluded.enrollment_id])),
    );

    assert_eq!(
        lease.acquire(&excluded, None).await,
        Err(LeaseFailure::EnrollmentEnded)
    );
    lease.end(&excluded, central_link_client::EnrollmentEnd::Revoked);
    assert!(!fence.is_fenced(&excluded));
    lease.request_stop_by(clock.monotonic_now());
    let joined = lease.join().await.unwrap();
    assert_eq!(joined.worker, Ok(()));
    assert!(joined.unsettled_retirements.is_empty());
}

struct HeldRetirement {
    source: EnrollmentLeaseSource,
    store: MetadataStore,
    key: EnrollmentKey,
    clock: Arc<ManualClock>,
    fence: switchboard_machine_enrollment::RetirementFence,
    admitted: AdmittedEnding,
}

async fn admit_retirement() -> HeldRetirement {
    let context = context();
    let key = key(&context, "019a0000-0000-7000-8000-000000000004");
    let mut store = MetadataStore::new(active(&context, &key, 1));
    let (endings, mut admitted) = mpsc::unbounded_channel();
    store.endings = Some(endings);
    let clock = Arc::new(ManualClock::new(Default::default()));
    let owner = RefreshOwner::new(store.clone(), NoRefresh, clock.clone(), context);
    let fence = owner.retirement_fence();
    let source = EnrollmentLeaseSource::start(owner, clock.clone(), Arc::new(BTreeSet::new()));
    source.end(&key, central_link_client::EnrollmentEnd::Revoked);
    let admitted = admitted.recv().await.unwrap();
    HeldRetirement {
        source,
        store,
        key,
        clock,
        fence,
        admitted,
    }
}

#[tokio::test]
async fn join_replaces_the_pending_timeout_sample_with_the_settled_fence() {
    let HeldRetirement {
        source,
        store,
        clock,
        key,
        fence,
        admitted,
    } = admit_retirement().await;
    let now = clock.monotonic_now();
    source.request_stop_by(now.after(Duration::from_secs(5)));
    let mut early_wait = std::pin::pin!(router_clock::timeout_at(
        clock.as_ref(),
        now.after(Duration::from_secs(1)),
        source.join(),
    ));
    poll_fn(|cx| {
        assert!(early_wait.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    clock.advance(Duration::from_secs(1)).unwrap();
    assert!(early_wait.await.is_err());
    assert_eq!(fence.pending_keys(), vec![key]);

    let mut late_join = std::pin::pin!(source.join());
    poll_fn(|cx| {
        assert!(late_join.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    admitted.commit(&store);
    let joined = late_join.await.unwrap();
    assert_eq!(joined.worker, Ok(()));
    assert!(joined.unsettled_retirements.is_empty());
    assert_eq!(source.join().await.unwrap(), joined);
}

#[tokio::test]
async fn an_admitted_retirement_can_finish_in_the_store_after_the_child_is_joined() {
    let HeldRetirement {
        source,
        store,
        clock,
        key,
        admitted,
        ..
    } = admit_retirement().await;
    source.request_stop_by(clock.monotonic_now());

    let joined = source.join().await.unwrap();
    assert_eq!(joined.worker, Ok(()));
    assert_eq!(joined.unsettled_retirements, vec![key.clone()]);
    assert!(admitted.reply.is_closed());
    assert!(matches!(
        &*store.row.borrow(),
        EnrollmentMetadata::Active { .. }
    ));

    admitted.commit(&store);
    assert!(matches!(
        &*store.row.borrow(),
        EnrollmentMetadata::Ended { key: ended, why: EnrollmentEnd::Revoked, .. } if ended == &key
    ));
    assert_eq!(source.join().await.unwrap(), joined);
}
