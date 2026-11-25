use super::*;
use central_link_client::{AdmittedLifecycleLink, LifecycleLinkReadiness, SessionLifecycleScope};
use std::collections::BTreeSet;
use switchboard_link_lease::EnrollmentLeaseSource;
use switchboard_machine_core_store::{MACHINE_LOCAL_DATABASE, load_or_create_machine_id};
use switchboard_machine_enrollment::{
    BindingGeneration, ConditionalWrite, CredentialStore, EnrollmentBinding, EnrollmentContext,
    EnrollmentEnd as StoredEnding, EnrollmentKey, EnrollmentMetadata, RefreshAnswer,
    RefreshCredential, RefreshOwner, RefreshSource, SecretValue, SignInDeployment, TokenGrant,
};
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStore,
    test_support::{KeychainAccess, KeychainAccessError},
};

async fn stop_lease(lease: &EnrollmentLeaseSource, clock: &dyn RuntimeClock) {
    lease.seal_ending_reports();
    lease.request_stop_by(clock.monotonic_now().after(Duration::from_secs(5)));
    let joined = lease.join().await.unwrap();
    assert_eq!(joined.worker, Ok(()));
    assert!(joined.unsettled_retirements.is_empty());
}

struct FixtureKeychain;

impl KeychainAccess for FixtureKeychain {
    fn read_secret(&self, _: &str, _: &str) -> Result<Option<Vec<u8>>, KeychainAccessError> {
        Ok(Some(vec![0x71; 32]))
    }

    fn add_secret(&self, _: &str, _: &str, _: &[u8]) -> Result<(), KeychainAccessError> {
        Err(KeychainAccessError::Unavailable)
    }
}

struct SignedRefresh {
    access: SecretValue,
    enrollment: switchboard_machine_enrollment::EnrollmentId,
}

impl RefreshSource for SignedRefresh {
    async fn refresh(&self, _: &RefreshCredential) -> RefreshAnswer {
        RefreshAnswer::Rotated(TokenGrant {
            access_token: self.access.clone(),
            credential: RefreshCredential {
                enrollment_id: self.enrollment,
                refresh_token: SecretValue::new("fictional-rotated-refresh".to_owned()).unwrap(),
            },
            expires_in: Duration::from_secs(600),
        })
    }
}

struct ObservedLeases {
    source: Arc<EnrollmentLeaseSource>,
    acquisitions: mpsc::UnboundedSender<EnrollmentKey>,
}

impl LeaseSource<EnrollmentKey> for ObservedLeases {
    fn lifecycle_scope(&self, key: &EnrollmentKey) -> Option<SessionLifecycleScope> {
        self.source.lifecycle_scope(key)
    }

    fn acquire<'a>(
        &'a self,
        key: &'a EnrollmentKey,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'a, EnrollmentKey> {
        self.acquisitions.send(key.clone()).unwrap();
        self.source.acquire(key, newer_than)
    }

    fn end(&self, key: &EnrollmentKey, cause: EnrollmentEnd) {
        self.source.end(key, cause);
    }
}

async fn wait_for_driver_poll(barrier: &mpsc::UnboundedSender<tokio::sync::oneshot::Sender<()>>) {
    let (ack, observed) = tokio::sync::oneshot::channel();
    barrier.send(ack).unwrap();
    observed.await.unwrap();
}

#[tokio::test]
async fn real_central_revocation_ends_the_sqlite_enrollment_and_never_redials_its_key() {
    let epoch = "2030-03-17T17:46:40Z".parse().unwrap();
    let clock = Arc::new(ManualClock::new(epoch));
    let central = start_machine_link_fixture(clock.clone()).await.unwrap();
    let directory = central.root_path().join("mac-core");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .unwrap();
    let machine_id = load_or_create_machine_id(&directory, clock.utc_now().timestamp_millis())
        .await
        .unwrap();
    let store = MachineEnrollmentStore::open_with_test_keychain(
        &directory.join(MACHINE_LOCAL_DATABASE),
        directory.join("secrets"),
        machine_id.clone(),
        clock.clone(),
        Arc::new(FixtureKeychain),
    )
    .await
    .unwrap();
    let organization = central.organization_id.as_str();
    let now = clock.utc_now().timestamp();
    let central_owner = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    let approval = central_owner.decide_device_login(&serde_json::from_value(json!({
        "organizationId": organization, "approver": signed::OWNER,
        "permissionChecked":"machine:enroll", "requestId":"019f0000-0000-7000-8000-00000000cf11",
        "machineId": machine_id.as_str(), "label":"W2 fixture", "allowedWorkspaces":[],
        "approverSecondFactorAt":now-60, "codeExpiresAt":now+600,
    })).unwrap()).await.unwrap();
    let approval = serde_json::to_value(approval).unwrap();
    assert_eq!(approval["result"], json!("approved"));
    let enrollment = approval["enrollmentId"].as_str().unwrap();
    let grant = central_owner
        .decide_machine_token(
            &serde_json::from_value(json!({
                "organizationId": organization, "enrollmentId":enrollment, "clientId":enrollment,
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let grant = serde_json::to_value(grant).unwrap();
    assert_eq!(grant["result"], json!("grant"));
    let token = signed::machine_token(
        organization,
        machine_id.as_str(),
        enrollment,
        now,
        grant["grantExpiresAt"].as_i64().unwrap(),
    )
    .unwrap();
    let deployment = SignInDeployment::new("https://sign-in.example/".parse().unwrap()).unwrap();
    let context = EnrollmentContext {
        machine_id: machine_id.clone(),
        organization_id: organization.parse().unwrap(),
        deployment,
    };
    let enrollment_id = enrollment.parse().unwrap();
    let binding = EnrollmentBinding {
        context: context.clone(),
        generation: BindingGeneration::from_persisted(1).unwrap(),
        credential: RefreshCredential {
            enrollment_id,
            refresh_token: SecretValue::new("fictional-initial-refresh".to_owned()).unwrap(),
        },
    };
    let key = binding.key();
    assert_eq!(
        store
            .replace(BindingGeneration::EMPTY, &binding)
            .await
            .unwrap(),
        ConditionalWrite::Applied
    );
    let owner = RefreshOwner::new(
        store.clone(),
        SignedRefresh {
            access: SecretValue::new(token).unwrap(),
            enrollment: enrollment_id,
        },
        clock.clone(),
        context.clone(),
    );
    let lease = Arc::new(EnrollmentLeaseSource::start(
        owner,
        clock.clone(),
        Arc::new(BTreeSet::new()),
    ));
    // This harness supplies the address at the settings boundary. Its real
    // Central listens on loopback HTTP, so the test dialer uses the fixture URI.
    let fixture_uri = format!(
        "ws://{}/v1/organizations/{organization}/link",
        central.origin().strip_prefix("http://").unwrap()
    );
    let (settings, settings_rx) = watch::channel(DriverSettings {
        binding: Some(key.clone()),
        address: DialAddress::test_loopback(&fixture_uri),
    });
    let (stop, shutdown) = watch::channel(false);
    let mut changes = store.subscribe_enrollment_changes();
    let (status_tx, mut status) = watch::channel(LinkStatus::NotEnrolled);
    let (lane_handle, lane) = link_lane_channel();
    let lifecycle = lane_handle.lifecycle_calls();
    let scope = SessionLifecycleScope {
        organization: central.organization_id.clone(),
        machine: machine_id.clone(),
        enrollment: enrollment.to_owned().try_into().unwrap(),
    };
    let address = central
        .origin()
        .strip_prefix("http://")
        .unwrap()
        .parse::<std::net::SocketAddr>()
        .unwrap();
    let (dials, mut dialed) = mpsc::unbounded_channel();
    let (acquisitions, mut acquired) = mpsc::unbounded_channel();
    let (barrier, mut barrier_requests) =
        mpsc::unbounded_channel::<tokio::sync::oneshot::Sender<()>>();
    let link = LinkDriver {
        machine_id,
        clock: clock.clone(),
        lease_source: Arc::new(ObservedLeases {
            source: lease.clone(),
            acquisitions,
        }),
        settings: settings_rx,
        shutdown,
        status: status_tx,
        lane,
    };
    let driver = tokio::spawn(async move {
        let run = link.run_with_test_transport(move || {
            let _ = dials.send(());
            tokio::net::TcpStream::connect(address)
        });
        tokio::pin!(run);
        loop {
            tokio::select! {
                biased;
                () = &mut run => break,
                request = barrier_requests.recv() => {
                    let Some(ack) = request else { break };
                    // The driver's real future was polled before this acknowledgement.
                    let _ = ack.send(());
                }
            }
        }
    });
    assert_eq!(acquired.recv().await, Some(key.clone()));
    assert_eq!(dialed.recv().await, Some(()));
    let seen = status
        .wait_for(|status| {
            matches!(
                status,
                LinkStatus::Welcomed { .. }
                    | LinkStatus::Fault { .. }
                    | LinkStatus::EnrollmentEnded { .. }
            )
        })
        .await
        .unwrap()
        .clone();
    assert!(matches!(seen, LinkStatus::Welcomed { .. }));
    let admitted = lane_handle
        .watch_admitted()
        .wait_for(Option::is_some)
        .await
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(
        lifecycle.lifecycle_readiness(&scope),
        LifecycleLinkReadiness::Ready(admitted)
    );

    changes.borrow_and_update();
    let private = reqwest::Client::builder()
        .no_proxy()
        .unix_socket(central.signin_socket.clone())
        .build()
        .unwrap();
    let revoke = signed::RevocationJourney {
        client: &private,
        origin: "http://localhost",
        organization,
        now,
    };
    revoke
        .expect(
            &revoke.prepare().await.unwrap(),
            enrollment,
            json!({"result":"revoked"}),
        )
        .await
        .unwrap();
    status
        .wait_for(|status| matches!(status, LinkStatus::EnrollmentEnded { .. }))
        .await
        .unwrap();
    changes.changed().await.unwrap();
    assert_eq!(
        lifecycle.lifecycle_readiness(&scope),
        LifecycleLinkReadiness::EnrollmentEnded
    );
    assert_eq!(
        store.read_metadata().await.unwrap(),
        EnrollmentMetadata::Ended {
            key: key.clone(),
            why: StoredEnding::Revoked,
            generation: BindingGeneration::from_persisted(3).unwrap(),
        }
    );
    wait_for_driver_poll(&barrier).await;
    clock.advance(Duration::from_secs(30)).unwrap();
    wait_for_driver_poll(&barrier).await;
    assert_eq!(settings.borrow().binding.as_ref(), Some(&key));
    assert!(
        acquired.try_recv().is_err(),
        "the driver tried to acquire another lease for the retired binding"
    );
    assert!(
        dialed.try_recv().is_err(),
        "the retired binding was dialed again"
    );
    stop.send_replace(true);
    driver.await.unwrap();
    stop_lease(&lease, clock.as_ref()).await;
    store.close().await.unwrap();
    drop(central_owner);
    central.close().await.unwrap();
}

use std::os::unix::fs::DirBuilderExt;

#[path = "enrolled_lease_cases.rs"]
mod cases;
