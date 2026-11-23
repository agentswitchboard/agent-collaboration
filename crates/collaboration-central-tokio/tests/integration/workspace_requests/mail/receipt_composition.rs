use super::super::support::{Fixture, id, settings};
use collaboration_central_core::WorkspaceRuntime;
use collaboration_central_tokio::{
    NativeDirectMessageStore, NativeSignInTrust, ObjectStorage, TokioWorkspaceStores,
};
use fleet_contracts::mail_link::MailReceiptSettlement;
use fleet_runtime::{ClockIds, FleetRuntime};
use fleet_runtime_ports::{Clock, SyncTime};
use router_clock::RuntimeClock;
use std::sync::Arc;
use workspace_model::central_mail::{ReceiptKind, ReceiptResult};

#[derive(Clone)]
struct FixtureClock(Arc<dyn RuntimeClock>);

impl Clock for FixtureClock {
    fn now(&self) -> SyncTime {
        SyncTime::from_unix_millis(runtime_primitives::Clock::now(self.0.as_ref()).unix_millis())
    }
}

#[tokio::test]
async fn native_receipt_composition_without_alarms_reports_no_such_message() {
    let fixture = Fixture::new().await;
    let organization = fixture.caller.organization_id();
    let attachment = fixture.caller.connection().attachment();
    fixture.host.close().await;

    let storage = Arc::new(ObjectStorage::new(
        fixture
            .root
            .path()
            .join("central")
            .join(format!("{}.sqlite", organization.as_str())),
        Arc::new(fleet_storage_schema::workspace_migrations::central_migrator()),
    ));
    let workspace_root = fixture.root.path().join("workspace-stores");
    let workspace = Arc::new(ObjectStorage::new(
        workspace_root.join(format!("{}.sqlite", fixture.workspace)),
        Arc::new(fleet_storage_schema::workspace_migrations::workspace_migrator()),
    ));
    let stores = TokioWorkspaceStores::with_fixture_store(
        workspace_root,
        fixture.workspace.clone(),
        workspace,
    );
    let clock_handle = FixtureClock(fixture.shared_for_runtime());
    let clock: Arc<dyn Clock> = Arc::new(clock_handle.clone());
    // This composition installs the receipt clock independently of alarm publication.
    let runtime = WorkspaceRuntime::new(
        FleetRuntime::open(
            organization.clone(),
            storage.clone(),
            stores,
            clock.clone(),
            Arc::new(ClockIds::new(clock_handle)),
            Default::default(),
            Arc::new(|_| {}),
        ),
        storage.clone(),
        Arc::new(NativeDirectMessageStore::new(
            storage.clone(),
            clock.clone(),
        )),
    )
    .with_sign_in_verification(
        NativeSignInTrust::new(settings(&fixture.keys.url))
            .unwrap()
            .for_organization(organization.clone(), clock)
            .unwrap(),
    );

    assert_eq!(
        runtime
            .apply_delivery_receipt(
                &attachment,
                &id(299_101),
                ReceiptKind::Seen {
                    completed_at: fixture.clock.utc_now(),
                },
            )
            .await
            .unwrap(),
        MailReceiptSettlement::Stop(ReceiptResult::NoSuchMessage)
    );
    assert_eq!(storage.alarm(), None);
    storage.close().await;
}
