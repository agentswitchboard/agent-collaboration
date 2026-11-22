//! Shared Host, provider and store fixture for restore recovery tests.

use super::super::*;
use collaboration_protocol::{
    ConversationLoadRequest, ConversationPromptRequest, ConversationResumeRequest, MessageContent,
    MessageDelivery,
};
use router_clock::ManualClock;
use switchboard_session_events::ProviderSessionEventHub;
use switchboard_session_store::ProviderSessionRecord;

pub(super) fn admission_pause() -> (
    AdmissionTestPause,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (resume, release) = tokio::sync::oneshot::channel();
    (AdmissionTestPause { entered, release }, reached, resume)
}

pub(super) async fn wait_for_queued_input(
    host: &ExternalProviderSupervisor,
    target: &SessionRef,
    operation: &OperationId,
) {
    let operation = uuid::Uuid::parse_str(operation.as_str()).expect("operation identity");
    host.session_owner_for(target)
        .expect("registered owner")
        .snapshots()
        .wait_for(|snapshot| {
            snapshot
                .queue
                .iter()
                .any(|entry| entry.input.operation == operation)
        })
        .await
        .expect("prompt is waiting on that owner");
}

pub(crate) struct RestoreRig {
    pub(crate) root: tempfile::TempDir,
    pub(crate) target: SessionRef,
    pub(crate) clock: Arc<ManualClock>,
}

impl RestoreRig {
    pub(crate) fn new() -> Self {
        Self {
            root: tempfile::tempdir().expect("restore root"),
            target: SessionRef {
                endpoint: endpoint(),
                session_id: SessionId::try_from("saved-session".to_owned()).expect("session"),
            },
            clock: Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        }
    }

    pub(super) fn working_directory(&self) -> ProviderWorkingDirectory {
        ProviderWorkingDirectory::try_from(self.root.path().display().to_string())
            .expect("working directory")
    }

    pub(super) async fn store(&self) -> ProviderOperationStore {
        ProviderOperationStore::open(&self.root.path().join("operations.sqlite"))
            .await
            .expect("store")
    }

    pub(crate) async fn seed_session(&self) {
        self.store()
            .await
            .record_session(&ProviderSessionRecord {
                target: self.target.clone(),
                working_directory: self.working_directory(),
                requested_policy: policy(),
                created_by: self.target.clone().into(),
                approver: self.target.clone().into(),
                updated_at_ms: 1,
                resume: Default::default(),
            })
            .await
            .expect("saved session");
    }

    pub(super) async fn open_host(&self, behavior: &str) -> ExternalProviderSupervisor {
        self.open_host_with(&[behavior]).await
    }

    pub(super) async fn open_host_with(&self, behaviors: &[&str]) -> ExternalProviderSupervisor {
        self.open_host_with_adapter(
            behaviors,
            Arc::new(acp_provider_adapters::StandardAcpBehavior),
        )
        .await
    }

    pub(super) async fn open_host_with_adapter(
        &self,
        behaviors: &[&str],
        adapter: Arc<dyn acp_client_runtime::ProviderBehavior>,
    ) -> ExternalProviderSupervisor {
        let arguments = std::iter::once("plain-prompts")
            .chain(behaviors.iter().copied())
            .collect::<Vec<_>>();
        self.open_host_with_fixture_arguments(&arguments, adapter)
            .await
            .0
    }

    pub(crate) async fn open_with_store(
        &self,
        behavior: &str,
    ) -> (
        ExternalProviderSupervisor,
        Arc<Mutex<ProviderOperationStore>>,
    ) {
        let (supervisor, _, store) = self
            .open_fixture_parts(
                &["plain-prompts", behavior],
                Arc::new(acp_provider_adapters::StandardAcpBehavior),
            )
            .await;
        (supervisor, store)
    }

    pub(super) async fn open_host_with_fixture_arguments(
        &self,
        behaviors: &[&str],
        adapter: Arc<dyn acp_client_runtime::ProviderBehavior>,
    ) -> (ExternalProviderSupervisor, Arc<ProviderSessionEventHub>) {
        let (supervisor, hub, _) = self.open_fixture_parts(behaviors, adapter).await;
        (supervisor, hub)
    }

    async fn open_fixture_parts(
        &self,
        behaviors: &[&str],
        adapter: Arc<dyn acp_client_runtime::ProviderBehavior>,
    ) -> (
        ExternalProviderSupervisor,
        Arc<ProviderSessionEventHub>,
        Arc<Mutex<ProviderOperationStore>>,
    ) {
        let store = Arc::new(Mutex::new(self.store().await));
        // Reuse the binding generation across fixture reseeding.
        let (host, hub) = crate::provider_test_support::bind_scripted_provider(
            binding(),
            crate::provider_test_support::launch_fixture_agent(
                "permission-mode-agent.ts",
                vec![
                    self.root.path().join("wire.json").display().to_string(),
                    "cursor".into(),
                    "no-directories".into(),
                    "advertise-restore".into(),
                ]
                .into_iter()
                .chain(behaviors.iter().map(|behavior| (*behavior).to_owned()))
                .collect(),
            ),
            store.clone(),
            adapter,
            self.clock.clone(),
        )
        .await
        .expect("scripted binding and supervisor");
        (host, hub, store)
    }

    pub(super) fn load_request(&self, operation_id: OperationId) -> ConversationLoadRequest {
        ConversationLoadRequest {
            operation_id,
            target: self.target.clone(),
            generation: Some(generation()),
            working_directory: self.working_directory(),
            requested_by: self.target.clone().into(),
            approver: self.target.clone().into(),
            requested_policy: policy(),
        }
    }

    pub(crate) fn resume_request(&self, operation_id: OperationId) -> ConversationResumeRequest {
        ConversationResumeRequest {
            operation_id,
            target: self.target.clone(),
            generation: Some(generation()),
            working_directory: self.working_directory(),
            requested_by: self.target.clone().into(),
            approver: self.target.clone().into(),
            requested_policy: policy(),
        }
    }

    pub(crate) fn wire_receipt(&self) -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(self.root.path().join("wire.json")).expect("wire receipt"),
        )
        .expect("wire JSON")
    }

    pub(super) async fn until_wire_request(&self, method: &str) {
        while !std::fs::read(self.root.path().join("wire.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .and_then(|wire| wire[method].as_array().map(|rows| !rows.is_empty()))
            .unwrap_or(false)
        {
            tokio::task::yield_now().await;
        }
    }

    pub(super) fn prompt_request(&self, id: OperationId, text: &str) -> ConversationPromptRequest {
        ConversationPromptRequest {
            delivery: MessageDelivery::Auto,
            input_id: None,
            operation_id: id,
            target: self.target.clone(),
            generation: Some(generation()),
            requested_by: self.target.clone().into(),
            approver: self.target.clone().into(),
            prompt: MessageContent::HumanUser {
                text: MessageText::try_from(text.to_owned()).expect("prompt text"),
            },
        }
    }

    pub(crate) async fn assert_loads(&self, host: &ExternalProviderSupervisor) {
        let id = OperationId::generate();
        host.load(self.load_request(id.clone()))
            .await
            .expect("restore admitted");
        let settled = host.wait(wait_request(id)).await.expect("restore settled");
        assert!(
            matches!(settled.output, ConversationOperationWaitOutput::Available {
            settlement: ConversationOperationSettlement::Loaded { ref target, .. }
        } if target == &self.target),
            "{settled:?}"
        );
    }
}

pub(super) fn policy() -> ProviderRequestedPolicy {
    ProviderRequestedPolicy {
        access: RouterAccess::WriteRestricted,
    }
}

pub(crate) fn wait_request(operation_id: OperationId) -> ConversationOperationWaitRequest {
    ConversationOperationWaitRequest {
        operation_id,
        timeout_seconds: PositiveSeconds::try_from(5).expect("wait budget"),
    }
}
