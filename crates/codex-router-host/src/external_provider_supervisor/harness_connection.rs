//! The session supervisor's port to a harness connection.

use collaboration_protocol::{OperationId, ProviderBindingIdentity};
use provider_session_machines::{ProviderSettingKind, session_owner::RequestFuture};
use session_event_model::{
    CapabilityReport, ExternalProviderApprovalRefusalReason, PreparedPrompt, ProviderTurnInput,
};
use std::sync::Arc;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    session_owner_ingress::SessionOwnerIngress,
    session_owner_lifecycle_reports::SessionHarnessProvider,
};

/// One harness connection, as the supervisor uses it.
pub trait HarnessConnection: Send + Sync {
    /// The provider the supervisor's session door binds each session through.
    fn session_provider(&self, tasks: TaskTracker) -> Arc<SessionHarnessProvider>;

    /// The connection advertisement overlaid with the session's negotiated values.
    fn capabilities(
        &self,
        observed: Option<CapabilityReport>,
    ) -> RequestFuture<'_, CapabilityReport>;

    /// Shape and check a caller's input for this harness.
    fn prepare_prompt(
        &self,
        input: ProviderTurnInput,
        observed: Option<CapabilityReport>,
    ) -> RequestFuture<'_, Result<PreparedPrompt, PromptRefusal>>;

    fn take_permission_refusal(
        &self,
        operation: &OperationId,
    ) -> Option<ExternalProviderApprovalRefusalReason>;
    fn reuses_setting_confirmation(&self, kind: ProviderSettingKind) -> bool;
    fn ended_on_oversized_message(&self) -> bool;
    fn shutdown(&self) -> RequestFuture<'_, ConnectionShutdown>;
}

/// Why prompt preparation refused before the harness received a request.
#[derive(Debug, Eq, PartialEq)]
pub enum PromptRefusal {
    Selection {
        kind: PromptSelectionRefusal,
        message: String,
    },
    UnsupportedContent {
        content_type: &'static str,
    },
    Unclassified,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptSelectionRefusal {
    InvalidSelection,
    MalformedMetadata,
    Unsupported,
    SourceUnavailable,
}

pub enum ConnectionShutdown {
    Stopped,
    OwnerFailedToJoin,
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) type FixtureOwnerDoorSlot =
    Arc<std::sync::OnceLock<Arc<crate::session_owner_door::SessionOwnerDoor>>>;

#[cfg(any(test, feature = "test-support"))]
pub(crate) struct HarnessConnectionTestSupport {
    pub(crate) hold_writer_capacity:
        Option<Arc<crate::session_owner_door::HoldProviderWriterCapacity>>,
    pub(crate) preparation_fixture:
        Arc<crate::session_owner_preparation_fixture::PreparationFixture>,
    pub(crate) owner_door_slot: FixtureOwnerDoorSlot,
}

pub struct ExternalProviderBinding {
    pub(super) identity: ProviderBindingIdentity,
    pub(super) connection: Arc<dyn HarnessConnection>,
    pub(super) retirement: CancellationToken,
    pub(super) ingress: Arc<SessionOwnerIngress>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) test_support: HarnessConnectionTestSupport,
    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
    pub(super) work_observer: Arc<dyn crate::session_owner_work_observation::WorkObservationSink>,
}

impl ExternalProviderBinding {
    pub(crate) fn new(
        identity: ProviderBindingIdentity,
        connection: Arc<dyn HarnessConnection>,
        retirement: CancellationToken,
        ingress: Arc<SessionOwnerIngress>,
        #[cfg(any(test, feature = "test-support"))] test_support: HarnessConnectionTestSupport,
        #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
        work_observer: Arc<dyn crate::session_owner_work_observation::WorkObservationSink>,
    ) -> Self {
        Self {
            identity,
            connection,
            retirement,
            ingress,
            #[cfg(any(test, feature = "test-support"))]
            test_support,
            #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
            work_observer,
        }
    }
}
