use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use switchboard_link_lease::{DiscoverySource, LinkSettingsSource};
use switchboard_machine_enrollment::{
    DiscoveryAnswer, EnrollmentKey, InvalidReason, SignInFailure, StoredEnrollment,
};

#[derive(Default)]
struct SwitchableKeychain {
    unavailable: AtomicBool,
    reads: AtomicUsize,
}

impl KeychainAccess for SwitchableKeychain {
    fn read_secret(&self, _: &str, _: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if self.unavailable.load(Ordering::Acquire) {
            return Err(KeychainAccessError::Unavailable);
        }
        Ok(Some(vec![0x6b; 32]))
    }

    fn add_secret(&self, _: &str, _: &str, _: &[u8]) -> Result<(), KeychainAccessError> {
        Err(KeychainAccessError::Unavailable)
    }
}

enum RefreshScript {
    NoRequest,
    Grant {
        lifetime: Duration,
        access: &'static str,
    },
    Refused(InvalidReason),
    Oversized,
    Unavailable(Option<Duration>),
}

impl RefreshSource for RefreshScript {
    async fn refresh(&self, credential: &RefreshCredential) -> RefreshAnswer {
        match *self {
            Self::NoRequest => RefreshAnswer::Unavailable { retry_after: None },
            Self::Grant { lifetime, access } => RefreshAnswer::Rotated(TokenGrant {
                access_token: SecretValue::new(access.to_owned()).unwrap(),
                credential: credential.clone(),
                expires_in: lifetime,
            }),
            Self::Refused(reason) => RefreshAnswer::Refused { reason },
            Self::Oversized => RefreshAnswer::OversizedReply,
            Self::Unavailable(retry_after) => RefreshAnswer::Unavailable { retry_after },
        }
    }
}

struct LocalEnrollment {
    store: MachineEnrollmentStore,
    keychain: Arc<SwitchableKeychain>,
    binding: EnrollmentBinding,
    clock: Arc<ManualClock>,
}

impl LocalEnrollment {
    async fn open(root: &std::path::Path, clock: Arc<ManualClock>) -> Self {
        std::fs::DirBuilder::new().mode(0o700).create(root).unwrap();
        let machine_id = load_or_create_machine_id(root, clock.utc_now().timestamp_millis())
            .await
            .unwrap();
        let keychain = Arc::new(SwitchableKeychain::default());
        let store = MachineEnrollmentStore::open_with_test_keychain(
            &root.join(MACHINE_LOCAL_DATABASE),
            root.join("secrets"),
            machine_id.clone(),
            clock.clone(),
            keychain.clone(),
        )
        .await
        .unwrap();
        let binding = EnrollmentBinding {
            generation: BindingGeneration::from_persisted(1).unwrap(),
            context: EnrollmentContext {
                machine_id,
                organization_id: signed::ORGANIZATION.parse().unwrap(),
                deployment: SignInDeployment::new("https://sign-in.example/".parse().unwrap())
                    .unwrap(),
            },
            credential: RefreshCredential {
                enrollment_id: "019f0000-0000-7000-8000-00000000cf51".parse().unwrap(),
                refresh_token: SecretValue::new("fictional-enrolled-case-refresh".into()).unwrap(),
            },
        };
        Self {
            store,
            keychain,
            binding,
            clock,
        }
    }

    async fn install(&self) {
        assert_eq!(
            self.store
                .replace(BindingGeneration::EMPTY, &self.binding)
                .await,
            Ok(ConditionalWrite::Applied)
        );
    }

    fn source(&self, script: RefreshScript) -> EnrollmentLeaseSource {
        let owner = RefreshOwner::new(
            self.store.clone(),
            script,
            self.clock.clone(),
            self.binding.context.clone(),
        );
        EnrollmentLeaseSource::start(owner, self.clock.clone(), Arc::new(BTreeSet::new()))
    }
}

fn unavailable(seconds: u64) -> Result<LinkLease<EnrollmentKey>, LeaseFailure> {
    Err(LeaseFailure::Unavailable {
        retry_after: Duration::from_secs(seconds),
    })
}

#[tokio::test]
async fn the_real_enrollment_store_obeys_each_acquire_mapping_and_keeps_other_keys_out() {
    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
    let allocation = start_machine_link_fixture(clock.clone()).await.unwrap();
    let local = LocalEnrollment::open(&allocation.root_path().join("acquire-matrix"), clock).await;
    let key = local.binding.key();
    let empty = local.source(RefreshScript::NoRequest);
    assert_eq!(empty.acquire(&key, None).await, unavailable(1));
    stop_lease(&empty, local.clock.as_ref()).await;
    local.install().await;

    let mut different = key.clone();
    different.enrollment_id = "019f0000-0000-7000-8000-00000000cf52".parse().unwrap();
    let fresh = local.source(RefreshScript::NoRequest);
    assert_eq!(fresh.acquire(&different, None).await, unavailable(1));
    assert_eq!(
        fresh.acquire(&key, Some(LeaseVersion(u64::MAX))).await,
        Err(LeaseFailure::Fault)
    );
    assert_eq!(
        fresh.acquire(&key, Some(LeaseVersion(42))).await,
        Err(LeaseFailure::Fault)
    );
    local.keychain.unavailable.store(true, Ordering::Release);
    assert_eq!(fresh.acquire(&key, None).await, unavailable(5));
    let reads_before_different_key = local.keychain.reads.load(Ordering::Relaxed);
    assert_eq!(fresh.acquire(&different, None).await, unavailable(1));
    assert_eq!(
        local.keychain.reads.load(Ordering::Relaxed),
        reads_before_different_key,
        "classifying a different key must not try to open its credential"
    );
    local.keychain.unavailable.store(false, Ordering::Release);
    stop_lease(&fresh, local.clock.as_ref()).await;

    for (script, expected) in [
        (RefreshScript::Unavailable(None), unavailable(30)),
        (
            RefreshScript::Unavailable(Some(Duration::from_secs(19))),
            unavailable(19),
        ),
        (RefreshScript::Oversized, Err(LeaseFailure::Fault)),
        (
            RefreshScript::Refused(InvalidReason::InvalidRequest),
            Err(LeaseFailure::Fault),
        ),
        (
            RefreshScript::Grant {
                lifetime: Duration::ZERO,
                access: "expired-fixture",
            },
            unavailable(1),
        ),
        (
            RefreshScript::Grant {
                lifetime: Duration::from_secs(90),
                access: "invalid wire token",
            },
            Err(LeaseFailure::Fault),
        ),
    ] {
        let source = local.source(script);
        assert_eq!(source.acquire(&key, None).await, expected);
        stop_lease(&source, local.clock.as_ref()).await;
    }
    let active = local.store.read().await.unwrap().generation();
    let usable = local.source(RefreshScript::Grant {
        lifetime: Duration::from_secs(90),
        access: "fixture-wire-token",
    });
    let acquired = usable
        .acquire(&key, Some(LeaseVersion(active.get())))
        .await
        .unwrap();
    assert_eq!(acquired.window.binding, key);
    assert_eq!(acquired.window.version.0, active.get() + 1);
    assert_eq!(acquired.token.expose(), "fixture-wire-token");
    stop_lease(&usable, local.clock.as_ref()).await;

    let refused = local.source(RefreshScript::Refused(InvalidReason::GrantRefused));
    let mut enrollment_changes = local.store.subscribe_enrollment_changes();
    assert_eq!(
        refused.acquire(&key, None).await,
        Err(LeaseFailure::EnrollmentEnded)
    );
    enrollment_changes.changed().await.unwrap();
    assert!(matches!(
        local.store.read().await.unwrap(),
        StoredEnrollment::Ended {
            why: StoredEnding::RefreshRefused,
            ..
        }
    ));
    stop_lease(&refused, local.clock.as_ref()).await;
    local.store.close().await.unwrap();
    allocation.close().await.unwrap();
}

type DiscoveryReply = tokio::sync::oneshot::Sender<Result<DiscoveryAnswer, SignInFailure>>;

struct DiscoveryScript(mpsc::UnboundedSender<DiscoveryReply>);

impl DiscoverySource for DiscoveryScript {
    async fn discover(&self) -> Result<DiscoveryAnswer, SignInFailure> {
        let (sender, reply) = tokio::sync::oneshot::channel();
        self.0.send(sender).unwrap();
        reply.await.unwrap()
    }
}

#[tokio::test]
async fn real_store_settings_see_same_scope_replacement_and_ending_without_the_keychain() {
    let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
    let allocation = start_machine_link_fixture(clock.clone()).await.unwrap();
    let path = allocation.root_path().join("settings-matrix");
    let local = LocalEnrollment::open(&path, clock.clone()).await;
    let (send, mut discovery) = mpsc::unbounded_channel();
    let (settings, mut current) = LinkSettingsSource::with_discovery(
        local.store.clone(),
        DiscoveryScript(send),
        local.binding.context.clone(),
        clock.clone(),
        local.store.subscribe_enrollment_changes(),
        Arc::new(BTreeSet::new()),
    );
    let (stop, shutdown) = watch::channel(false);
    let settings = tokio::spawn(settings.run(shutdown));
    assert!(current.borrow().binding.is_none());
    local.install().await;
    let held = discovery.recv().await.unwrap();

    let other = MachineEnrollmentStore::open_with_test_keychain(
        &path.join(MACHINE_LOCAL_DATABASE),
        path.join("secrets"),
        local.binding.context.machine_id.clone(),
        clock.clone(),
        local.keychain.clone(),
    )
    .await
    .unwrap();
    let mut replacement = local.binding.clone();
    replacement.generation = replacement.generation.successor().unwrap();
    replacement.credential.enrollment_id = "019f0000-0000-7000-8000-00000000cf54".parse().unwrap();
    let replacement_key = replacement.key();
    assert_eq!(
        other.replace(local.binding.generation, &replacement).await,
        Ok(ConditionalWrite::Applied)
    );
    local.keychain.unavailable.store(true, Ordering::Release);
    clock.advance(Duration::from_secs(15)).unwrap();

    let published = current
        .wait_for(|settings| settings.binding.as_ref() == Some(&replacement_key))
        .await
        .unwrap()
        .clone();
    assert_eq!(
        published.address,
        Err(switchboard_central_link::AddressError::DiscoveryPending)
    );
    let _held_replacement = discovery.recv().await.unwrap();
    assert!(held.is_closed());
    assert_eq!(
        local.store.read().await,
        Err(switchboard_machine_enrollment::CredentialStoreError::Read)
    );

    assert_eq!(
        local
            .store
            .remove(replacement.generation, StoredEnding::Revoked)
            .await,
        Ok(ConditionalWrite::Applied)
    );
    current
        .wait_for(|settings| settings.binding.is_none())
        .await
        .unwrap();
    stop.send_replace(true);
    settings.await.unwrap();
    local.store.close().await.unwrap();
    other.close().await.unwrap();
    allocation.close().await.unwrap();
}
