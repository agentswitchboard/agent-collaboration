//! The typed collaboration application service.
//!
//! One public async method per collaboration operation, grouped by family. Each family takes
//! the existing typed `collaboration-protocol` and `message-board` requests and returns their
//! typed results or the family's typed failure. Transport adapters only decode requests, call
//! these methods and encode what comes back; no operation logic lives in a transport.
//!
//! Each family owns only the capabilities its constructor receives.
//! [`CollaborationApplication`] shares conversation, session, message and
//! provider observation operations with its callers.
//! (`provider_observation_operations`). Heavier paging logic stays in the crate's domain modules
//! (`codex_session_inventory`, `provider_session_inventory`).
mod conversation_operations;
pub mod message_operations;
mod observation_call_driver;
mod observation_call_machine;
mod observation_failure;
#[cfg(any(test, feature = "test-support"))]
pub use observation_call_driver::test_support as observation_test_support;
mod provider_observation_operations;
mod session_native_control;
mod session_operations;
mod workspace_operations;
pub use crate::inventory_failures::{
    NativeSessionFailure, NativeSessionFailureKind, NativeSessionStage, ProviderInventoryFailure,
    ProviderInventoryFailureKind,
};
pub use crate::response_budget::{API_RESULT_BUDGET, RESULT_LIMIT_BYTES, ResultByteBudget};
pub use session_native_control::NativeSessionControl;

pub use conversation_operations::{ConversationFailure, ConversationOperations};
pub use observation_call_driver::ObservedEventSink;
pub use observation_failure::ObservationFailure;
pub use provider_observation_operations::ObservationOperations;
pub use session_operations::{
    EndpointDirectoryUnavailable, InstallationIdentityUnavailable, SessionOperations,
};
#[cfg(test)]
pub(crate) use session_operations::{
    classify_native_call_failure, rename_method_unsupported, valid_session_rename_name,
};
pub use workspace_operations::WorkspaceOperations;

use collaboration_contracts::RouterIdentity;
use message_operations::MessageOperations;

/// Constructed operation families serving one Router installation and run.
#[derive(Clone)]
pub struct CollaborationApplication {
    identity: RouterIdentity,
    conversations: std::sync::Arc<ConversationOperations>,
    observation: std::sync::Arc<ObservationOperations>,
    sessions: std::sync::Arc<SessionOperations>,
    messages: MessageOperations,
}
impl CollaborationApplication {
    #[must_use]
    pub fn new(
        identity: RouterIdentity,
        conversations: ConversationOperations,
        observation: ObservationOperations,
        sessions: SessionOperations,
    ) -> Self {
        Self {
            identity,
            conversations: std::sync::Arc::new(conversations),
            observation: std::sync::Arc::new(observation),
            sessions: std::sync::Arc::new(sessions),
            messages: MessageOperations::default(),
        }
    }
    #[must_use]
    pub fn with_messages(mut self, messages: MessageOperations) -> Self {
        self.messages = messages;
        self
    }

    #[must_use]
    pub fn messages(&self) -> &MessageOperations {
        &self.messages
    }
    #[must_use]
    pub fn workspace(&self) -> WorkspaceOperations<'_> {
        WorkspaceOperations::new(&self.messages)
    }
    #[must_use]
    pub fn service_id(&self) -> &collaboration_protocol::UuidIdentity {
        self.identity.service_id()
    }
    #[must_use]
    pub fn service_epoch(&self) -> &collaboration_protocol::UuidIdentity {
        self.identity.service_epoch()
    }
    #[must_use]
    pub fn installation_peer(&self) -> Option<collaboration_protocol::InstallationPeerId> {
        self.identity.installation_peer()
    }
    #[must_use]
    pub fn machine_label(&self) -> &collaboration_protocol::MachineLabel {
        self.identity.machine_identity().machine_label()
    }
    #[must_use]
    pub fn conversations(&self) -> &ConversationOperations {
        &self.conversations
    }
    #[must_use]
    pub fn observation(&self) -> &ObservationOperations {
        &self.observation
    }
    #[must_use]
    pub fn sessions(&self) -> &SessionOperations {
        &self.sessions
    }
}

// Rejection tests compose existing failure builders without exposing application internals.
#[cfg(test)]
pub(crate) use session_native_control::INVALID_NATIVE_PARAMETERS;
