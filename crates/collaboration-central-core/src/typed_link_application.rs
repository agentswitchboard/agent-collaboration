//! The application port for typed link calls and scheduled approval expiry.
use crate::ValidatedMachineConnection;
use central_link_protocol::{CentralCall, CentralCallReply, TypedCallResult};
use central_storage::StoreFailure;
use std::{future::Future, pin::Pin};

pub type CentralCallDispatch = Result<TypedCallResult<CentralCallReply>, StoreFailure>;
pub struct AppliedApprovalDeadline {
    pub outcome: fleet_engine::CheckedDueUnit<std::convert::Infallible>,
    pub completion: Option<LinkCallCompletion>,
}

pub type LinkCallCompletion = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Post-commit work starts only after the host has sent the reply and bound any run.
pub struct CentralCallWork {
    pub result: TypedCallResult<CentralCallReply>,
    pub completion: Option<LinkCallCompletion>,
}

impl From<TypedCallResult<CentralCallReply>> for CentralCallWork {
    fn from(result: TypedCallResult<CentralCallReply>) -> Self {
        Self {
            result,
            completion: None,
        }
    }
}

#[cfg(feature = "test-support")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FixtureApprovalDecision {
    #[serde(default)]
    pub answer: FixtureApprovalAnswer,
    pub request_id: workspace_ids::ApprovalRequestId,
    pub operation_id: workspace_ids::RequestId,
    pub person_id: workspace_ids::PersonId,
}

#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FixtureApprovalAnswer {
    #[default]
    HeldStart,
    ProviderApproval,
}

/// The host holds the organization turn while using this physical socket's binding.
pub enum TypedCallCarrier {
    Session,
    Approval {
        registered_run: Option<workspace_ids::RouterRunId>,
    },
}

pub fn is_approval_holder_call(request: Option<&CentralCall>) -> bool {
    match request {
        Some(CentralCall::RegisterRun(_) | CentralCall::Raise(_) | CentralCall::Stop(_)) => true,
        Some(
            CentralCall::CreateFleetSession(_)
            | CentralCall::BindFleetSession(_)
            | CentralCall::EndFleetSession(_)
            | CentralCall::ListApprovalsForSession(_)
            | CentralCall::ReadApprovalForSession(_)
            | CentralCall::DecideApprovalAsSession(_)
            | CentralCall::PassOnApproval(_),
        )
        | None => false,
    }
}

/// Dispatches a registered call using the machine identity verified on this connection.
pub trait CentralLinkDispatcher: Send + Sync {
    #[cfg(feature = "test-support")]
    fn fixture_decide_approval(
        &self,
        _organization: fleet_identity_protocol::OrganizationId,
        _decision: FixtureApprovalDecision,
    ) -> Pin<Box<dyn Future<Output = Result<Option<LinkCallCompletion>, StoreFailure>> + Send + '_>>
    {
        Box::pin(async { Err(StoreFailure::Unavailable) })
    }

    fn end_approval_enrollment(
        &self,
        organization: fleet_identity_protocol::OrganizationId,
        invalidation: fleet_contracts::EnrollmentInvalidation,
        statements: Vec<central_storage::GuardedStatement>,
    ) -> Pin<Box<dyn Future<Output = Result<(), StoreFailure>> + Send + '_>>;

    fn recover_approval_notices(&self, machine: ValidatedMachineConnection) -> LinkCallCompletion;

    fn apply_approval_deadline(
        &self,
        organization: fleet_identity_protocol::OrganizationId,
        deadline: central_engine_model::DueRow,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<AppliedApprovalDeadline, fleet_engine::CheckedDueFailure>>
                + Send
                + '_,
        >,
    >;

    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
        carrier: TypedCallCarrier,
    ) -> Pin<Box<dyn Future<Output = Result<CentralCallWork, StoreFailure>> + Send + 'call>>;
}
