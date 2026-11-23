//! Apply a verified revocation and its fresh-proof reservation in one org turn.
use crate::{OrganizationAdminAnswer, WorkspaceRuntime, person_request::VerifiedPersonRequest};
use central_storage::{GuardedStatement, StoreFailure};
use fleet_contracts::{
    AdmittedRevoke, MachineEnrollmentRevocationResult as ResultKind, RevokePermission,
};
use fleet_identity_protocol::EnrollmentId;
use fleet_machine_login::{PreparedEnrollmentRevocation, policy::DecisionTime};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    /// The caller holds the central turn through admission, commit and notification.
    pub(crate) async fn revoke_machine_enrollment_from_admin(
        &self,
        request: &VerifiedPersonRequest,
        enrollment_id: &EnrollmentId,
        mut statements: Vec<GuardedStatement>,
        now_ms: i64,
    ) -> Result<OrganizationAdminAnswer, StoreFailure> {
        let authority = AdmittedRevoke {
            actor: request
                .person_id
                .clone()
                .try_into()
                .map_err(|_| corrupt("person_request.person_id"))?,
            proof_request_id: request
                .request_id
                .to_string()
                .try_into()
                .map_err(|_| corrupt("person_request.request_id"))?,
            permission: RevokePermission::MachineRevoke,
        };
        let prepared = self
            .machine_login_service()
            .prepare_revoke_enrollment(authority, enrollment_id, DecisionTime { now_ms })
            .await?;
        let (result, invalidation) = match prepared {
            PreparedEnrollmentRevocation::NotFound => (ResultKind::NotFound {}, None),
            PreparedEnrollmentRevocation::AlreadyEnded => (ResultKind::AlreadyEnded {}, None),
            PreparedEnrollmentRevocation::Revoked(revocation) => {
                let (_, invalidation, effects) = revocation.into_applying();
                statements.extend(effects);
                (ResultKind::Revoked {}, Some(invalidation))
            }
        };
        self.apply_machine_login_write(statements, invalidation.as_ref())
            .await?;
        if let Some(invalidation) = invalidation {
            (self.enrollment_invalidated)(invalidation);
        }
        Ok(OrganizationAdminAnswer::MachineEnrollmentRevocation(result))
    }
}
