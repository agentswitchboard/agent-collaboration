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
    pub request_id: workspace_ids::ApprovalRequestId,
    pub operation_id: workspace_ids::RequestId,
    pub person_id: workspace_ids::PersonId,
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
    ) -> Pin<Box<dyn Future<Output = Result<CentralCallWork, StoreFailure>> + Send + 'call>>;
}
