//! Builds the real message system and controls the publication boundaries used by joined work tests.

use std::{net::SocketAddr, os::unix::fs::DirBuilderExt as _, sync::Arc};

use central_link_client::EnrollmentEnd;
use collaboration_central_tokio::test_support::MachineLinkFixture;
use router_clock::RuntimeClock;
use switchboard_central_link::{
    DialAddress,
    lease_source::{ApprovalServiceTarget, LeaseFailure, LeaseFuture, LeaseSource},
    link_driver::{DriverSettings, LinkDriver},
    link_policy::{LeaseVersion, LinkStatus},
};
use switchboard_link_lease::EnrollmentLeaseSource;
use switchboard_machine_core_store::load_or_create_machine_id;
use switchboard_machine_enrollment::EnrollmentKey;
use switchboard_message_system::{
    MessageRoots, MessageSystem, MessageSystemEvents,
    test_support::{LinkFixture, StatusPublicationHold, start_with_link_fixture},
};
use tokio::sync::{mpsc, watch};

use super::{
    message_unit_fixture::{
        FixtureDiscovery, FixtureRefresh, KnownRecipient, RecordCustody, enroll, grant_at_central,
        open_store,
    },
    native_work_carrier::NativeWorkCarrier,
};

pub(super) struct DriverStatusControl {
    pub(super) actual: watch::Receiver<LinkStatus>,
    pub(super) publish: watch::Sender<bool>,
    pub(super) ending_reports: mpsc::UnboundedReceiver<DeferredEnrollmentEnding>,
    pub(super) operator_status_hold: StatusPublicationHold,
}

pub(super) struct DeferredEnrollmentEnding {
    lease_source: Arc<EnrollmentLeaseSource>,
    key: EnrollmentKey,
    pub(super) cause: EnrollmentEnd,
}

impl DeferredEnrollmentEnding {
    pub(super) fn persist(self) {
        self.lease_source.end(&self.key, self.cause);
    }
}

struct ObserveTerminalGate {
    lease_source: Arc<EnrollmentLeaseSource>,
    ending_reports: mpsc::UnboundedSender<DeferredEnrollmentEnding>,
}

impl LeaseSource<EnrollmentKey> for ObserveTerminalGate {
    fn registration_target(
        &self,
        key: &EnrollmentKey,
    ) -> Result<ApprovalServiceTarget, LeaseFailure> {
        self.lease_source.registration_target(key)
    }

    fn acquire<'source>(
        &'source self,
        key: &'source EnrollmentKey,
        newer_than: Option<LeaseVersion>,
    ) -> LeaseFuture<'source, EnrollmentKey> {
        self.lease_source.acquire(key, newer_than)
    }

    fn end(&self, key: &EnrollmentKey, cause: EnrollmentEnd) {
        // The driver reports this only after the owning socket withdraws its gate.
        // Hold persistence so the test can request a refusal in that exact turn.
        assert!(
            self.ending_reports
                .send(DeferredEnrollmentEnding {
                    lease_source: self.lease_source.clone(),
                    key: key.clone(),
                    cause,
                })
                .is_ok()
        );
    }
}

async fn forward_link_status(
    mut actual: watch::Receiver<LinkStatus>,
    published: watch::Sender<LinkStatus>,
    mut publish: watch::Receiver<bool>,
) {
    let mut controls_open = true;
    loop {
        let latest = actual.borrow_and_update().clone();
        if *publish.borrow_and_update() {
            published.send_if_modified(|status| {
                if *status == latest {
                    return false;
                }
                *status = latest;
                true
            });
        }
        tokio::select! {
            update = actual.changed() => {
                if update.is_err() {
                    return;
                }
            }
            update = publish.changed(), if controls_open => controls_open = update.is_ok(),
        }
    }
}

pub(super) async fn start_mac(
    central: &MachineLinkFixture,
    carrier: NativeWorkCarrier,
) -> (MessageSystem, DriverStatusControl) {
    let clock = central.clock.clone();
    let root = central.root_path().join("typed-work");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    let machine = load_or_create_machine_id(&root, clock.utc_now().timestamp_millis())
        .await
        .unwrap();
    let store = open_store(&root, machine.clone(), clock.clone()).await;
    let (enrollment_id, bearer) = grant_at_central(
        central,
        &machine,
        "019f0000-0000-7000-8000-00000000cd15".parse().unwrap(),
    )
    .await;
    let organization = central.organization_id.as_str().parse().unwrap();
    enroll(&store, machine.clone(), organization, enrollment_id).await;
    let (events, _observed_events) = mpsc::unbounded_channel();
    let (discovery, _observed_discovery) = mpsc::unbounded_channel();
    let address = central
        .url(central.organization_id.as_str())
        .replacen("http://", "ws://", 1);
    let physical_address: SocketAddr = central
        .origin()
        .strip_prefix("http://")
        .unwrap()
        .parse()
        .unwrap();
    let driver_clock = clock.clone();
    let (driver_status, actual) = watch::channel(LinkStatus::NotEnrolled);
    let (publish, relay) = watch::channel(true);
    let (ending_reports, endings) = mpsc::unbounded_channel();
    let operator_status_hold = StatusPublicationHold::default();
    let system = start_with_link_fixture(
        MessageRoots::new(root.clone(), root.join("secrets")),
        machine,
        store.clone(),
        clock.clone(),
        Arc::new(KnownRecipient("typed-work".parse().unwrap())),
        MessageSystemEvents::new(Arc::new(RecordCustody(events)))
            .with_status_publication_hold(operator_status_hold.clone()),
        LinkFixture {
            discovery: FixtureDiscovery(discovery),
            refresh: FixtureRefresh {
                enrollment_id,
                bearer,
            },
            run_driver: move |mut driver: LinkDriver<_, _>| {
                let address = address.clone();
                let carrier = carrier.clone();
                let clock = driver_clock.clone();
                let relay = relay.clone();
                let published = std::mem::replace(&mut driver.status, driver_status.clone());
                let actual = driver.status.subscribe();
                let ending_reports = ending_reports.clone();
                async move {
                    let settings = {
                        let discovered = driver
                            .settings
                            .wait_for(|found| found.address.is_ok())
                            .await
                            .unwrap();
                        DriverSettings {
                            binding: discovered.binding.clone(),
                            address: DialAddress::test_loopback(&address),
                        }
                    };
                    let (_settings, updates) = watch::channel(settings);
                    let driver = LinkDriver {
                        machine_id: driver.machine_id,
                        clock: driver.clock,
                        lease_source: Arc::new(ObserveTerminalGate {
                            lease_source: driver.lease_source,
                            ending_reports,
                        }),
                        settings: updates,
                        shutdown: driver.shutdown,
                        status: driver.status,
                        lane: driver.lane,
                    };
                    let run = driver.run_with_test_transport(move || {
                        let carrier = carrier.clone();
                        let clock = clock.clone();
                        async move { carrier.connect(physical_address, clock).await }
                    });
                    tokio::select! {
                        () = Box::pin(run) => {},
                        () = forward_link_status(actual, published, relay) => {},
                    }
                }
            },
        },
    );
    (
        system,
        DriverStatusControl {
            actual,
            publish,
            ending_reports: endings,
            operator_status_hold,
        },
    )
}
