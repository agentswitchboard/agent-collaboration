use central_engine_model::IdSource;
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_machine_login::link_authority::ValidatedMachineConnection;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use workspace_ids::{RequestId, RouterRunId};

use crate::{ApprovalEnrollment, ApprovalRun};

/// Central's immutable primary key for an approval request.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApprovalRequestRowId(RequestId);

impl ApprovalRequestRowId {
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
pub(crate) struct ApprovalDestinationRowId(RequestId);

impl ApprovalDestinationRowId {
    pub(crate) fn mint(ids: &mut dyn IdSource) -> Result<Self, StoreFailure> {
        Self::parse(&ids.next_id().to_string())
    }

    pub(crate) fn parse(value: &str) -> Result<Self, StoreFailure> {
        value
            .parse()
            .map(Self)
            .map_err(|_| corrupt("approval_request_destinations.approval_destination_id"))
    }

    pub(crate) fn as_str(&self) -> &str {
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

/// Machine-login proof bound to the run named by an approval command.
///
/// An ordinary caller cannot mint a holder from a scope.
/// ```compile_fail,E0451
/// use fleet_approvals::{ApprovalHolderAdmission, ApprovalHolderScope};
/// fn forge(scope: ApprovalHolderScope) -> ApprovalHolderAdmission {
///     ApprovalHolderAdmission { scope }
/// }
/// ```
///
/// A message body cannot deserialize a holder admission.
/// ```compile_fail,E0277
/// let _ = serde_json::from_str::<fleet_approvals::ApprovalHolderAdmission>("{}");
/// ```
#[derive(Clone)]
pub struct ApprovalHolderAdmission {
    scope: ApprovalHolderScope,
}

impl ApprovalHolderAdmission {
    pub fn for_connection(
        connection: &ValidatedMachineConnection,
        claimed_run: RouterRunId,
    ) -> Self {
        Self {
            scope: ApprovalHolderScope {
                organization_id: connection.organization_id().clone(),
                run: ApprovalRun {
                    router_run: claimed_run,
                    enrollment: ApprovalEnrollment {
                        machine_id: connection.machine_id().clone(),
                        enrollment_id: connection.enrollment_id().clone(),
                    },
                },
            },
        }
    }

    pub(crate) fn holder_scope(&self) -> &ApprovalHolderScope {
        &self.scope
    }

    #[cfg(feature = "test-support")]
    pub fn for_fixture_scope(scope: &ApprovalHolderScope) -> Self {
        Self {
            scope: scope.clone(),
        }
    }
}

impl ApprovalHolderScope {
    pub(crate) fn enrollment_scope(&self) -> ApprovalEnrollmentScope {
        ApprovalEnrollmentScope {
            organization_id: self.organization_id.clone(),
            enrollment: self.run.enrollment.clone(),
        }
    }
}
