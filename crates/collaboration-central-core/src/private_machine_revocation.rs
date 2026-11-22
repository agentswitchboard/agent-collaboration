//! Apply a verified revocation and its fresh-proof reservation in one org turn.
use crate::{
    MachineEnrollmentRevocationResult as ResultKind, OrganizationAdminAnswer, WorkspaceRuntime,
    person_request::VerifiedPersonRequest,
};
use central_storage::{AlarmUpdate, BatchOutcome, GuardedStatement, StatementBatch, StoreFailure};
use fleet_contracts::{AdmittedRevoke, RevokePermission};
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
        let (result, revoked_machine) = match prepared {
            PreparedEnrollmentRevocation::NotFound => (ResultKind::NotFound {}, None),
            PreparedEnrollmentRevocation::AlreadyEnded => (ResultKind::AlreadyEnded {}, None),
            PreparedEnrollmentRevocation::Revoked {
                machine_id,
                statements: effects,
                ..
            } => {
                statements.extend(effects);
                (ResultKind::Revoked {}, Some(machine_id))
            }
        };
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterSignInSample)
            .await;
        if !matches!(
            self.execution
                .statement_host()
                .run_batch(StatementBatch {
                    statements,
                    alarm: AlarmUpdate::Unchanged,
                })
                .await?,
            BatchOutcome::Applied { .. }
        ) {
            return Err(StoreFailure::Unavailable);
        }
        if let Some(machine_id) = revoked_machine {
            (self.machine_revoked)(machine_id);
        }
        Ok(OrganizationAdminAnswer::MachineEnrollmentRevocation(result))
    }
}
