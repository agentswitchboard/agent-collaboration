use std::{os::unix::fs::DirBuilderExt as _, sync::Arc, time::Duration};

use central_storage::{SqlValue, Statement};
use collaboration_central_tokio::test_support::{MachineLinkFixture, start_machine_link_fixture};
use router_clock::{ManualClock, RuntimeClock};
use switchboard_central_link::{
    DialAddress,
    link_driver::{DriverSettings, LinkDriver},
    link_policy::LinkStatus,
};
use switchboard_link_lease::EnrollmentLeaseSource;
use switchboard_machine_core_store::load_or_create_machine_id;
use switchboard_machine_enrollment::{
    ConditionalWrite, CredentialStore as _, EnrollmentBinding, EnrollmentId, EnrollmentKey,
    EnrollmentMetadata, RefreshAnswer, RefreshCredential, RefreshSource, TokenGrant,
};
use switchboard_message_system::{
    CloseReport, MessageRoots, MessageSystemEvents,
    test_support::{LinkFixture, start_with_link_fixture},
};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use workspace_ids::{MachineId, RouterRunId};

use super::{
    EPOCH, FixtureDiscovery, FixtureRefresh, KnownRecipient, RecordCustody, enroll, open_store,
};
use crate::message_unit_fixture::{approve_at_central, grant_for_enrollment};

#[tokio::test]
async fn durable_binding_replacement_registers_a_new_run_with_the_same_live_message_system() {
    let clock = Arc::new(ManualClock::new(EPOCH.parse().unwrap()));
    let central = Arc::new(start_machine_link_fixture(clock.clone()).await.unwrap());
    let root = central.root_path().join("binding-replacement");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .unwrap();
    let machine = load_or_create_machine_id(&root, clock.utc_now().timestamp_millis())
        .await
        .unwrap();
    let store = open_store(&root, machine.clone(), clock.clone()).await;
    let first = approve_at_central(
        &central,
        &machine,
        "019f0000-0000-7000-8000-00000000cd61".parse().unwrap(),
    )
    .await;
    let first_bearer = grant_for_enrollment(&central, &machine, first).await;
    let context = enroll(
        &store,
        machine.clone(),
        central.organization_id.as_str().parse().unwrap(),
        first,
    )
    .await;
    let replacement = approve_at_central(
        &central,
        &machine,
        "019f0000-0000-7000-8000-00000000cd62".parse().unwrap(),
    )
    .await;
    assert_ne!(first, replacement);

    let (refresh_entered, mut entered) = mpsc::unbounded_channel();
    let (release, hold) = oneshot::channel();
    let (drivers, mut started) = mpsc::unbounded_channel();
    let (discovery, _) = mpsc::unbounded_channel();
    let (custody, _) = mpsc::unbounded_channel();
    let address = DialAddress::test_loopback(
        &central
            .url(central.organization_id.as_str())
            .replacen("http://", "ws://", 1),
    )
    .unwrap();
    let system = start_with_link_fixture(
        MessageRoots::new(root.clone(), root.join("secrets")),
        machine.clone(),
        store.clone(),
        clock,
        Arc::new(KnownRecipient(
            "replacement-fixture-recipient".parse().unwrap(),
        )),
        MessageSystemEvents::new(Arc::new(RecordCustody(custody))),
        LinkFixture {
            discovery: FixtureDiscovery(discovery),
            refresh: ReplacementRefresh {
                central: central.clone(),
                machine: machine.clone(),
                first: FixtureRefresh {
                    enrollment_id: first,
                    bearer: first_bearer,
                },
                replacement,
                entered: refresh_entered,
                hold: Arc::new(Mutex::new(Some(hold))),
            },
            run_driver: move |driver: LinkDriver<EnrollmentKey, EnrollmentLeaseSource>| {
                assert!(drivers.send(driver.status.subscribe()).is_ok());
                follow_live_settings(driver, address.clone())
            },
        },
    );
    let mut driver = started.recv().await.unwrap();
    driver
        .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
        .await
        .unwrap();
    let first_run = stored_run(&central, &machine, first).await;

    let EnrollmentMetadata::Active { generation, .. } = store.read_metadata().await.unwrap() else {
        panic!("the first enrollment must still be active before replacement");
    };
    let binding = EnrollmentBinding {
        generation: generation.binding_successor().unwrap(),
        context,
        credential: RefreshCredential {
            enrollment_id: replacement,
            refresh_token: switchboard_machine_enrollment::SecretValue::new(
                "fixture-new-binding".to_owned(),
            )
            .unwrap(),
        },
    };
    assert_eq!(
        store.replace(generation, &binding).await.unwrap(),
        ConditionalWrite::Applied
    );
    assert_eq!(entered.recv().await, Some(replacement));
    // Discovery can briefly publish its missing address before the new
    // lease arrives. Neither state can be Connected for the withdrawn run.
    assert!(!matches!(*driver.borrow(), LinkStatus::Connected { .. }));
    assert!(release.send(()).is_ok());
    driver
        .wait_for(|status| matches!(status, LinkStatus::Connected { .. }))
        .await
        .unwrap();

    assert_ne!(stored_run(&central, &machine, replacement).await, first_run);
    assert!(matches!(
        store.read_metadata().await.unwrap(),
        EnrollmentMetadata::Active { key, .. } if key == binding.key()
    ));
    assert!(
        started.try_recv().is_err(),
        "the original message unit owns both registrations"
    );
    assert_eq!(system.close().await, CloseReport::Closed);
    Arc::into_inner(central).unwrap().close().await.unwrap();
}

#[derive(Clone)]
struct ReplacementRefresh {
    central: Arc<MachineLinkFixture>,
    machine: MachineId,
    first: FixtureRefresh,
    replacement: EnrollmentId,
    entered: mpsc::UnboundedSender<EnrollmentId>,
    hold: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

impl RefreshSource for ReplacementRefresh {
    async fn refresh(&self, credential: &RefreshCredential) -> RefreshAnswer {
        if credential.enrollment_id == self.first.enrollment_id {
            return self.first.refresh(credential).await;
        }
        assert_eq!(credential.enrollment_id, self.replacement);
        let hold = self.hold.lock().await.take();
        if let Some(hold) = hold {
            self.entered.send(self.replacement).unwrap();
            hold.await.unwrap();
        }
        RefreshAnswer::Rotated(TokenGrant {
            access_token: grant_for_enrollment(&self.central, &self.machine, self.replacement)
                .await,
            credential: credential.clone(),
            expires_in: Duration::from_secs(600),
        })
    }
}

async fn follow_live_settings(
    driver: LinkDriver<EnrollmentKey, EnrollmentLeaseSource>,
    address: DialAddress,
) {
    let mut upstream = driver.settings.clone();
    let project = |settings: &DriverSettings<EnrollmentKey>| DriverSettings {
        binding: settings.binding.clone(),
        address: settings
            .address
            .as_ref()
            .map(|_| address.clone())
            .map_err(Clone::clone),
    };
    let (publish, settings) = watch::channel(project(&upstream.borrow_and_update()));
    let running = LinkDriver { settings, ..driver }.run();
    tokio::pin!(running);
    loop {
        tokio::select! {
            biased;
            () = &mut running => return,
            update = upstream.changed() => {
                if update.is_err() {
                    drop(publish);
                    running.await;
                    return;
                }
                publish.send_replace(project(&upstream.borrow_and_update()));
            }
        }
    }
}

#[expect(
    clippy::panic,
    reason = "The native store proof requires exactly one typed run row."
)]
async fn stored_run(
    central: &MachineLinkFixture,
    machine: &MachineId,
    enrollment: EnrollmentId,
) -> RouterRunId {
    let runtime = central
        .host
        .central(&central.organization_id)
        .await
        .unwrap();
    let rows = runtime.runtime().statement_host().read(Statement {
        sql: "SELECT router_run_id FROM approval_router_runs WHERE machine_id = ?1 AND enrollment_id = ?2",
        args: vec![SqlValue::Text(machine.to_string()), SqlValue::Text(enrollment.to_string())],
    }).await.unwrap().rows;
    let [row] = rows.as_slice() else {
        panic!("expected exactly one run for this enrollment");
    };
    let [SqlValue::Text(id)] = row.as_slice() else {
        panic!("expected a typed run id");
    };
    id.parse().unwrap()
}
