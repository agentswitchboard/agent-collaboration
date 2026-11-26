//! Shared collaboration vocabulary and capability ports; no runtime resource ownership.
mod machine_identity;
pub use machine_identity::MachineIdentity;
mod native_delivery_result;
mod provider_conversation_backend;
mod session_command_port;
mod session_event_hub;
mod session_model_catalog;
mod session_token_at_creation;
#[cfg(any(test, feature = "test-support"))]
pub use native_delivery_result::scripted_delivery_result;
mod native_route_claim;
pub use native_route_claim::NativeRouteClaim;
mod session_delivery_contract;
pub use native_delivery_result::{
    NativeDeliveryResult, NativeObservedOutcome, NativeRefusalCode, UnsentCause,
};
pub use provider_conversation_backend::{
    ProviderConversationBackend, ProviderConversationFuture, ProviderSessionInspectFuture,
    ProviderSettingsFuture,
};
pub use session_command_port::{
    CommandContent, CommandFailure, CommandFuture, CreateSessionCommand, PromptSessionCommand,
    QueueInputCommand, QueuedInputOutcome, QueuedSessionInput, SessionCommandPort,
    SessionSettingsCommand, SessionSteerIntent, SessionSteerOutcome, SessionTargetCommand,
    SessionTurnHandle, SetSessionSettingCommand, SteerSessionCommand,
};
pub use session_delivery_contract::layer_zero;
pub use session_delivery_contract::{
    AttemptEvidenceSink, AttemptReconciliation, AttemptReconciliationContext,
    DeliveryContractError, DeliveryFuture, DeliveryPrecondition, LoadPolicy, NOT_LOADED_REASON,
    RouteClaim, RoutePresence, RouteUnavailableReason, SessionDeliveryRoute,
    SessionMessageDelivery, TargetPresence, TargetPresenceProbe, UnstoredAttemptEvidenceSink,
};
pub use session_event_hub::{
    HubEvent, HubFuture, HubSessionSummary, SessionEventAttachment, SessionEventHub,
    SessionEventHubError, SkillViewAttachment,
};
mod router_identity;
pub use router_identity::{RouterIdentity, RouterIdentityError};

pub use session_model_catalog::{
    InvalidProviderModelEntry, ProviderModelEntry, ProviderModelReasoning, ProviderReasoningChoice,
    SessionModelCatalog, SessionModelCatalogError,
};
pub use session_token_at_creation::{
    SessionTokenAtCreation, SessionTokenIssuance, SessionTokenIssuanceFuture,
    SessionTokenNotIssued, V0SessionTokenIssuer,
};
