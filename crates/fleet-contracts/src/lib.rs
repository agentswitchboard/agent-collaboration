//! Shared vocabulary and interfaces for FleetRuntime features.
#![forbid(unsafe_code)]

mod central_observation;
mod client_id;
mod engine_attempts;
mod event_completion;
mod issuer_refusals;
mod legacy_units;
mod machine_connection;
mod machine_connection_refusal;
mod mail_delivery_ready;
pub mod mail_link;
mod revision_failure;
mod session_lifecycle_rpc;
mod statement_refusal;
pub mod workspace_description;
mod workspace_directory;
mod workspace_rpc;

pub use central_observation::CentralObservation;
pub use client_id::ClientId;
pub use event_completion::{AlarmNotPublished, AlarmResult, EventCompletion, EventCompletionHook};
pub use issuer_refusals::{IssuerKeysUnavailable, IssuerSignatureRefusal};
pub use machine_connection::{
    EnrollmentInvalidation, LiveMachineConnection, MachineConnectionAttachment,
};
pub use machine_connection_refusal::MachineConnectionRefusal;
pub use revision_failure::AuthorizationRevisionFailure;
pub use session_lifecycle_rpc::{BindFleetSessionRpc, CreateFleetSessionRpc, EndFleetSessionRpc};
pub use statement_refusal::{FleetOrganizationStatementRefusal, StoredStatementRefusal};

pub use engine_attempts::{
    AttemptGuard, ControlGate, EngineSubject, FailureOutcome, ObservedControl, SubjectVersion,
};
pub use legacy_units::LegacyUnitOutcome;
pub use mail_delivery_ready::MailDeliveryReady;
pub use workspace_description::MAXIMUM_DESCRIPTION_BYTES;
pub use workspace_directory::{DirectoryWorkspace, WorkspaceDirectory, WorkspaceView};
pub use workspace_rpc::{
    ReadWorkspaceRequestStatus, RetainedWorkspaceStatus, RetainedWriteAnswer, WorkspaceRequest,
    WorkspaceResponse, WorkspaceServiceClient, WorkspaceServiceProtocol, WorkspaceServiceRpc,
    WorkspaceStatusFailure, dispatch_workspace_request,
};

mod organization_provisioning;
pub use organization_provisioning::*;
mod machine_token_claims;
pub use machine_token_claims::{MachinePrincipal, MachineTokenClaims};

mod stamp_failures;
pub use stamp_failures::{DigestFailure, ProvisionFailure};

mod stamp_values;
pub use stamp_values::{IssuerKeyId, OrganizationStatementDigest, StatementSeconds};

mod machine_login;
pub use machine_login::*;
