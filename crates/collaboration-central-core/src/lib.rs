//! One asynchronous application core shared by every runtime.
#![forbid(unsafe_code)]

mod engine;
/// Test support: the engine's alarm-input aggregate, one row of
/// `(on_grid_due, legacy_due)` in UTC milliseconds or NULL.
#[cfg(feature = "test-support")]
pub const ENGINE_ALARM_INPUTS: &str = engine::query_registry::ALARM_INPUTS;
#[cfg(feature = "test-support")]
pub mod engine_owed_probe;
#[cfg(feature = "test-support")]
pub mod engine_probe;
mod engine_wiring;
pub mod identity_contract;
#[cfg(feature = "test-support")]
pub mod signin_test_observation;
#[cfg(feature = "test-support")]
pub mod storage_conformance;

pub use fleet_machine_login::link_authority::{
    ValidatedMachineConnection, recheck_lent_link_authority,
};
pub use fleet_machine_login::link_transitions::{
    MachineTokenRenewalRefusal, RenewedMachineConnection,
};
mod machine_connection_admission;
mod machine_login_plans;
mod typed_link_application;
mod typed_link_dispatch;
pub use typed_link_application::{
    AppliedApprovalDeadline, CentralCallDispatch, CentralCallWork, CentralLinkDispatcher,
    LinkCallCompletion, TypedCallCarrier, is_approval_holder_call,
};
#[cfg(feature = "test-support")]
pub use typed_link_application::{FixtureApprovalAnswer, FixtureApprovalDecision};
pub use typed_link_dispatch::{
    DispatchedCentralCall, MacCallResult, TYPED_LINK_CALL_TIMEOUT, TargetCheckedCentralCall,
    dispatch_fallible_central_call, dispatch_fallible_correlated_central_call,
    dispatch_registered_central_call, finish_mac_call,
};
#[cfg(feature = "test-support")]
mod machine_login_test_control;
pub use machine_connection_admission::{
    MachineLinkAdmission, MachineLinkRequestAuthority, RenewalTermination, project_renewal_refusal,
};
mod workspace_caller;
pub use workspace_caller::WorkspaceCaller;
mod workspace_runtime;
#[cfg(feature = "test-support")]
pub use machine_login_test_control::{MachineLoginControl, machine_login_control};
pub use workspace_runtime::WorkspaceRuntime;
mod applying_request_failure;
mod organization_workspace_plans;
mod workspace_directory_requests;
mod workspace_item_reads;
mod workspace_read_queries;
pub use applying_request_failure::ApplyingRequestFailure;
mod workspace_request_gate;
mod workspace_request_status;
mod workspace_rule_dispatch;
mod workspace_rule_inputs;

mod central_mail_requests;
mod central_request_targets;
mod central_statement_values;
mod central_timer_plans;
mod central_timers;
mod direct_send_requests;
mod mail_delivery_authority;
mod notice_mailbox;
mod workspace_drain;
pub use workspace_drain::DrainResult;
mod central_organization_identity;
pub use central_organization_identity::{
    CentralOrganizationIdentity, prepare_central_alarm, read_central_organization,
};
mod central_mail_time;
pub use central_mail_time::MAX_ALARM_UNIX_MILLIS;

// Sign-in authority on the org-scoped statement runtime.
pub mod organization_authority;
pub mod person_request;
mod sign_in_trust_settings;
pub use sign_in_trust_settings::SignInTrustSettings;
mod authorization_floor_rows;
mod authorization_revision_plans;
mod organization_admin_contract;
mod organization_admin_plans;
mod organization_authority_cache;
mod organization_engine_admin_plans;
mod organization_provisioning_plans;
mod organization_signin_http;
mod organization_signin_port;
mod person_admission_plans;
mod person_proof_repository;
mod private_machine_revocation;
pub use organization_admin_contract::*;
pub use organization_signin_http::organization_signin_router;
pub use organization_signin_port::OrganizationSignInPort;

#[cfg(test)]
#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
mod signin_wire_export_test;
