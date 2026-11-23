use super::{ROUTER, SAVED, complete, managed_inventory, provider_record, session_id};
use crate::provider_test_support::{bind_scripted_provider, launch_fixture_agent};
use collaboration_protocol::{
    CodexGeneration, ConversationBindingIdentity, GenerationNumber, NonEmptyText, OperationId,
    ProviderBindingId, ProviderBindingIdentity, ProviderCapabilities, ProviderCapability,
    ProviderCapabilityEvidence, ProviderCapabilityName, ProviderCapabilityStatus, ProviderKind,
    ProviderOperationKind, ProviderRuntimeIdentity, ProviderTransport,
};
use provider_session_machines::session_lifecycle_machine::{RestoreVia, SessionLifecycle};
use router_clock::RuntimeClock;
use sqlx::{Connection as _, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{sync::Arc, time::Duration};
use switchboard_message_custody::recipient_sessions::{
    CombinedRecipientSessionDirectory, InventoryRead, RecipientSessionAnswer,
    RecipientSessionDirectory, RecipientSessionSources, SessionId, SessionInventory,
};
use switchboard_session_store::{ProviderOperationAdmission, ProviderOperationStore};
use tokio::sync::Mutex;

#[tokio::test]
async fn managed_loading_owner_counts_with_or_without_a_saved_record() {
    let root = tempfile::tempdir().unwrap();
    let database_path = root.path().join("operations.sqlite");
    let store = Arc::new(Mutex::new(
        ProviderOperationStore::open(&database_path).await.unwrap(),
    ));
    let mut inventory = managed_inventory(Some(store.clone()));
    let record = provider_record(&inventory, 1, root.path());
    let target = record.target.clone();
    let binding = ProviderBindingIdentity {
        endpoint: target.endpoint.clone(),
        binding_id: ProviderBindingId::try_from("cursor-inventory-fixture".to_owned()).unwrap(),
        runtime: ProviderRuntimeIdentity {
            provider: ProviderKind::Cursor,
            runtime_name: NonEmptyText::try_from("cursor".to_owned()).unwrap(),
            runtime_version: None,
        },
        transport: ProviderTransport::StdioAcp,
        generation: CodexGeneration {
            service_epoch: ROUTER.to_owned().try_into().unwrap(),
            generation: GenerationNumber::try_from(1).unwrap(),
        },
        capabilities: ProviderCapabilities::try_from(vec![ProviderCapability {
            name: ProviderCapabilityName::Load,
            status: ProviderCapabilityStatus::Supported,
            evidence: ProviderCapabilityEvidence::Advertised,
        }])
        .unwrap(),
    };
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let launch = launch_fixture_agent(
        "permission-mode-agent.ts",
        vec![
            root.path().join("wire.json").display().to_string(),
            "cursor".to_owned(),
            "no-directories".to_owned(),
            "advertise-restore".to_owned(),
            "restored-in-auto".to_owned(),
        ],
    );
    let (supervisor, _events) = bind_scripted_provider(
        binding.clone(),
        launch,
        store.clone(),
        Arc::new(acp_provider_adapters::StandardAcpBehavior),
        clock.clone(),
    )
    .await
    .unwrap();
    let supervisor = Arc::new(supervisor);
    inventory.supervisor = Some(supervisor.clone());
    let session = session_id(SAVED);
    assert_eq!(inventory.lookup_session(&session).await, complete(0));

    let operation =
        OperationId::try_from("019f0000-0000-7000-8000-000000000035".to_owned()).unwrap();
    let correlation = operation.as_str().parse().unwrap();
    {
        let mut store = store.lock().await;
        store
            .admit(ProviderOperationAdmission {
                operation_id: operation.clone(),
                operation_kind: ProviderOperationKind::ConversationLoad,
                binding: ConversationBindingIdentity::ExternalProvider { binding },
                admitted_at_ms: 0,
            })
            .await
            .unwrap();
        store.record_target(&operation, &target, 0).await.unwrap();
        store.mark_may_have_dispatched(&operation, 0).await.unwrap();
    }
    let owner = supervisor.session_owner_for(&target).unwrap();
    let (reached, release) = supervisor
        .runtime_for(&target.endpoint)
        .unwrap()
        .pause_restore_preparation();
    let restore = owner.restore_explicit(correlation, RestoreVia::Load, Box::new(record.clone()));
    let check = async {
        reached.await.unwrap();
        let lifecycle = owner.snapshots().borrow().lifecycle;
        let saved_before = store
            .lock()
            .await
            .session_record(&target)
            .await
            .unwrap()
            .is_some();
        let owner_only = inventory.lookup_session(&session).await;
        store.lock().await.record_session(&record).await.unwrap();
        let owner_and_saved = inventory.lookup_session(&session).await;
        release.send(()).unwrap();
        (lifecycle, saved_before, owner_only, owner_and_saved)
    };
    let (restored, (lifecycle, saved_before, owner_only, owner_and_saved)) =
        tokio::join!(restore, check);
    let final_lifecycle = owner.snapshots().borrow().lifecycle;
    let live_and_saved = inventory.lookup_session(&session).await;
    assert!(matches!(final_lifecycle, SessionLifecycle::Live { .. }));
    let mut writer =
        SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&database_path))
            .await
            .unwrap();
    let removed = sqlx::query("DELETE FROM provider_session_records")
        .execute(&mut writer)
        .await
        .unwrap();
    assert_eq!(removed.rows_affected(), 1);
    writer.close().await.unwrap();
    assert!(
        store
            .lock()
            .await
            .session_record(&target)
            .await
            .unwrap()
            .is_none()
    );
    let live_without_saved = inventory.lookup_session(&session).await;
    assert!(matches!(
        owner.snapshots().borrow().lifecycle,
        SessionLifecycle::Live { .. }
    ));

    let directory = CombinedRecipientSessionDirectory::new(RecipientSessionSources {
        codex: Arc::new(FixedInventory(1)),
        managed_providers: Arc::new(inventory),
        claude_terminal: Arc::new(FixedInventory(0)),
    });
    let store_guard = store.lock().await;
    let deadline = clock.monotonic_now().after(Duration::from_secs(30));
    tokio::select! {
        biased;
        answer = directory.query_until(&session, clock.as_ref(), deadline) => {
            assert_eq!(answer, RecipientSessionAnswer::AmbiguousSession);
        }
        () = clock.not_due_check_now(deadline) => {
            panic!("a live managed owner must prove its claim while both saved reads are blocked");
        }
    }
    drop(store_guard);
    supervisor.shutdown().await.unwrap();

    let _settlement = restored.unwrap();
    assert!(matches!(lifecycle, SessionLifecycle::Loading { .. }));
    assert!(!saved_before);
    assert_eq!(owner_only, complete(1));
    assert_eq!(owner_and_saved, complete(1));
    assert_eq!(live_and_saved, complete(1));
    assert_eq!(live_without_saved, complete(1));
}

struct FixedInventory(usize);

impl SessionInventory for FixedInventory {
    fn lookup_session<'lookup>(&'lookup self, _: &'lookup SessionId) -> InventoryRead<'lookup> {
        Box::pin(std::future::ready(complete(self.0)))
    }
}
