use central_engine_model::IdSource;
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::RequestId;

use crate::{ApprovalEnrollment, ApprovalRun};

/// Central's immutable primary key for an approval request.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApprovalRowId(RequestId);

impl ApprovalRowId {
    pub(crate) fn mint(ids: &mut dyn IdSource) -> Result<Self, StoreFailure> {
        Self::parse(&ids.next_id().to_string())
    }

    pub(crate) fn parse(value: &str) -> Result<Self, StoreFailure> {
        value
            .parse()
            .map(Self)
            .map_err(|_| corrupt("approval_requests.approval_row_id"))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalEnrollmentScope {
    pub organization_id: OrganizationId,
    pub enrollment: ApprovalEnrollment,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalHolderScope {
    pub organization_id: OrganizationId,
    pub run: ApprovalRun,
}

impl ApprovalHolderScope {
    pub(crate) fn enrollment_scope(&self) -> ApprovalEnrollmentScope {
        ApprovalEnrollmentScope {
            organization_id: self.organization_id.clone(),
            enrollment: self.run.enrollment.clone(),
        }
    }
}
