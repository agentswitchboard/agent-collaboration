use std::{future, os::unix::fs::PermissionsExt as _, path::Path, sync::Arc};

use router_clock::{ManualClock, RuntimeClock as _};
use switchboard_machine_core_store::MACHINE_LOCAL_DATABASE;
use switchboard_machine_enrollment::{
    BindingGeneration, CredentialStoreError, EnrollmentContext, EnrollmentEnd, EnrollmentKey,
    SignInClient, SignInDeployment,
};
use switchboard_message_custody::{
    custody_task::{CustodyEventSink, CustodyTaskEvent},
    recipient_sessions::RecipientSessionAnswer,
};
use tokio::sync::oneshot;
use workspace_ids::SessionId;

use switchboard_machine_enrollment::CredentialStore as _;
use switchboard_machine_enrollment_store::MachineEnrollmentStore;
use switchboard_message_custody::recipient_sessions::RecipientSessionDirectory;

use super::MessageRuntime;
use crate::{
    ActiveEnrollment, FrontRefusal, MessageUnit, MessageUnitBuildError, MessageUnitEvents,
    message_unit::UnitLinkInputs,
};
use switchboard_machine_core_store::load_or_create_machine_id;
use switchboard_machine_enrollment::EnrollmentMetadata;

fn fixture_clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(
        "2030-03-17T17:46:40Z".parse().expect("fixed UTC"),
    ))
}

fn private_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("temporary unit root");
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
        .expect("private unit root");
    root
}

fn enrollment() -> ActiveEnrollment {
    ActiveEnrollment {
        key: EnrollmentKey {
            machine_id: "019f0000-0000-7000-8000-000000000060"
                .parse()
                .expect("fixture machine"),
            organization_id: "019f0000-0000-7000-8000-000000000061"
                .parse()
                .expect("fixture organization"),
            enrollment_id: "019f0000-0000-7000-8000-000000000062"
                .parse()
                .expect("fixture enrollment"),
        },
        deployment: SignInDeployment::new(
            "https://127.0.0.1:1/".parse().expect("fixture issuer URL"),
        )
        .expect("fixture issuer"),
    }
}

async fn empty_store(root: &Path, clock: Arc<ManualClock>) -> MachineEnrollmentStore {
    let machine_id = load_or_create_machine_id(root, clock.utc_now().timestamp_millis())
        .await
        .expect("create fixture machine id");
    MachineEnrollmentStore::open(
        &root.join(MACHINE_LOCAL_DATABASE),
        root.join("secrets"),
        machine_id,
        clock,
    )
    .await
    .expect("open fixture store")
}

#[tokio::test]
async fn empty_metadata_refuses_without_calling_the_builder() {
    let root = private_root();
    let store = empty_store(root.path(), fixture_clock()).await;
    let (entered, mut observed_entry) = oneshot::channel();
    let mut runtime = MessageRuntime::start_for_enrollment(store.clone(), |_| {
        let _ = entered.send(());
        future::ready(Err(MessageUnitBuildError::Outbox))
    })
    .await;

    assert!(matches!(
        runtime.front().custody(),
        Err(FrontRefusal::NotEnrolled)
    ));
    assert_eq!(
        observed_entry.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    );
    let stopped = runtime.stop().await;
    assert!(stopped.unit.is_none());
    assert!(
        stopped
            .enrollment_store
            .expect("startup opened store")
            .expect("store joined")
            .outcome
            .is_ok()
    );
    assert!(store.read_metadata().await.is_err());
}

#[tokio::test]
async fn ended_and_unreadable_metadata_refuse_without_calling_the_builder() {
    let active = enrollment();
    for (metadata, expected) in [
        (
            Ok(EnrollmentMetadata::Ended {
                key: active.key,
                why: EnrollmentEnd::Revoked,
                generation: BindingGeneration::from_persisted(2).expect("ending generation"),
            }),
            FrontRefusal::EnrollmentEnded,
        ),
        (
            Err(CredentialStoreError::Read),
            FrontRefusal::MetadataUnreadable,
        ),
    ] {
        let root = private_root();
        let store = empty_store(root.path(), fixture_clock()).await;
        let (entered, observed_entry) = oneshot::channel();
        let mut runtime = MessageRuntime::start_from_metadata(metadata, store, |_| {
            let _ = entered.send(());
            future::ready(Err(MessageUnitBuildError::Outbox))
        })
        .await;

        assert_eq!(runtime.front().custody().err(), Some(expected));
        assert!(observed_entry.await.is_err());
        assert!(runtime.stop().await.unit.is_none());
    }
}

struct EmptyDirectory;

impl RecipientSessionDirectory for EmptyDirectory {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> std::pin::Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        Box::pin(future::ready(RecipientSessionAnswer::UnknownSession))
    }
}

struct IgnoreEvents;

impl CustodyEventSink for IgnoreEvents {
    fn record_event(&self, _: CustodyTaskEvent) {}
}

#[tokio::test]
async fn failed_outbox_open_refuses_startup_and_drops_the_unstarted_driver() {
    let root = private_root();
    let unusable_root = private_root();
    std::fs::create_dir(unusable_root.path().join(MACHINE_LOCAL_DATABASE))
        .expect("block custody database");
    let clock = fixture_clock();
    let store = empty_store(root.path(), clock.clone()).await;
    let unit_store = store.clone();
    let active = enrollment();
    let metadata = EnrollmentMetadata::Active {
        key: active.key,
        deployment: active.deployment,
        generation: BindingGeneration::from_persisted(1).expect("active generation"),
    };
    let (driver_entered, observed_driver) = oneshot::channel();
    let (release_driver, hold_driver) = oneshot::channel::<()>();
    let (build_outcome, observed_build) = oneshot::channel();

    let mut runtime =
        MessageRuntime::start_from_metadata(Ok(metadata), store, |enrollment| async move {
            let context = EnrollmentContext {
                machine_id: enrollment.key.machine_id,
                organization_id: enrollment.key.organization_id,
                deployment: enrollment.deployment,
            };
            let discovery = SignInClient::new(context.deployment.origin().clone(), clock.clone())
                .expect("configure test sign-in client");
            let refresh_owner = discovery.refresh_owner(
                unit_store.clone(),
                context.machine_id.clone(),
                context.organization_id,
            );
            let events = MessageUnitEvents::new(Arc::new(IgnoreEvents));
            let result = MessageUnit::assemble(
                unusable_root.path(),
                &unit_store,
                clock,
                Arc::new(EmptyDirectory),
                events,
                UnitLinkInputs {
                    context,
                    discovery,
                    refresh_owner,
                    run_driver: move |_| async {
                        let _ = driver_entered.send(());
                        let _ = hold_driver.await;
                    },
                },
            )
            .await;
            let _ = build_outcome.send(result.as_ref().err().copied());
            result
        })
        .await;

    assert_eq!(
        observed_build.await.expect("builder reported result"),
        Some(MessageUnitBuildError::Outbox)
    );
    assert!(
        release_driver.send(()).is_err(),
        "the held driver was dropped without starting"
    );
    assert!(observed_driver.await.is_err());
    assert!(matches!(
        runtime.front().custody(),
        Err(FrontRefusal::CouldNotStart)
    ));
    assert!(runtime.stop().await.unit.is_none());
}
