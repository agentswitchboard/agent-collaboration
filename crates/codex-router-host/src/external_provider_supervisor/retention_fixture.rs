//! Operation store and supervisor shared by the retention tests.

use super::ExternalProviderSupervisor;
use chrono::{DateTime, Utc};
use collaboration_protocol::{
    CodexGeneration, ConversationBindingIdentity, EndpointId, EndpointRef, GenerationNumber,
    NonEmptyText, OperationId, ProviderBindingId, ProviderBindingIdentity, ProviderCapabilities,
    ProviderCapability, ProviderCapabilityEvidence, ProviderCapabilityName,
    ProviderCapabilityStatus, ProviderKind, ProviderOperationEffect, ProviderOperationKind,
    ProviderReconciliationState, ProviderRuntimeIdentity, ProviderTransport, UuidIdentity,
};
use router_clock::ManualClock;
use std::{path::PathBuf, sync::Arc};
use switchboard_session_store::{ProviderOperationAdmission, ProviderOperationStore};
use tokio::sync::Mutex;

pub(crate) fn at(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap()
        .with_timezone(&Utc)
}

fn binding() -> ProviderBindingIdentity {
    let endpoint = EndpointRef {
        service_id: UuidIdentity::try_from("0ff962c5-7fa3-4c18-a5ca-1bbe8db09e89".to_owned())
            .unwrap(),
        endpoint_id: EndpointId::try_from("cursor-local".to_owned()).unwrap(),
    };
    ProviderBindingIdentity {
        endpoint,
        binding_id: ProviderBindingId::try_from("cursor-binding".to_owned()).unwrap(),
        runtime: ProviderRuntimeIdentity {
            provider: ProviderKind::Cursor,
            runtime_name: NonEmptyText::try_from("cursor".to_owned()).unwrap(),
            runtime_version: None,
        },
        transport: ProviderTransport::StdioAcp,
        generation: CodexGeneration {
            service_epoch: UuidIdentity::try_from(
                "1ff962c5-7fa3-4c18-a5ca-1bbe8db09e80".to_owned(),
            )
            .unwrap(),
            generation: GenerationNumber::try_from(1).unwrap(),
        },
        capabilities: ProviderCapabilities::try_from(vec![ProviderCapability {
            name: ProviderCapabilityName::Create,
            status: ProviderCapabilityStatus::Supported,
            evidence: ProviderCapabilityEvidence::Advertised,
        }])
        .unwrap(),
    }
}

pub(crate) struct Operations {
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) _directory: tempfile::TempDir,
    pub(crate) path: PathBuf,
    pub(crate) store: Arc<Mutex<ProviderOperationStore>>,
    pub(crate) supervisor: Arc<ExternalProviderSupervisor>,
}

impl Operations {
    pub(crate) async fn open() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("provider-operations.sqlite");
        let store = Arc::new(Mutex::new(
            ProviderOperationStore::open(&path).await.unwrap(),
        ));
        let clock = Arc::new(ManualClock::new(
            DateTime::from_timestamp(1_700_012_345, 0).expect("fixture UTC time"),
        ));
        let supervisor = Arc::new(
            ExternalProviderSupervisor::new(Vec::new(), Arc::clone(&store), clock.clone()).unwrap(),
        );
        Self {
            clock,
            _directory: directory,
            path,
            store,
            supervisor,
        }
    }

    /// Admits an operation and marks it dispatched, as running it does.
    pub(crate) async fn dispatched(&self) -> OperationId {
        let mut store = self.store.lock().await;
        let operation_id = OperationId::generate();
        store
            .admit(ProviderOperationAdmission {
                operation_id: operation_id.clone(),
                operation_kind: ProviderOperationKind::ConversationPrompt,
                binding: ConversationBindingIdentity::ExternalProvider { binding: binding() },
                admitted_at_ms: 1,
            })
            .await
            .unwrap();
        store
            .mark_may_have_dispatched(&operation_id, 2)
            .await
            .unwrap();
        operation_id
    }

    /// Records the operation's settlement, as the supervisor does before it publishes.
    pub(crate) async fn settle(&self, operation_id: &OperationId, settled: DateTime<Utc>) {
        self.store
            .lock()
            .await
            .record_terminal(
                operation_id,
                ProviderOperationEffect::Applied,
                ProviderReconciliationState::Confirmed,
                None,
                settled.timestamp_millis(),
            )
            .await
            .unwrap();
    }

    pub(crate) async fn settled(&self, settled: DateTime<Utc>) -> OperationId {
        let operation_id = self.dispatched().await;
        self.settle(&operation_id, settled).await;
        operation_id
    }

    pub(crate) async fn holds(&self, operation_id: &OperationId) -> bool {
        self.store
            .lock()
            .await
            .inspect(operation_id)
            .await
            .unwrap()
            .is_some()
    }
}
