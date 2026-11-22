//! Closed test-support dispatch to the real private revocation decision.
use crate::WorkspaceRuntime;
use crate::person_request::AdminOperationId;
use central_storage::StoreFailure;
use fleet_contracts::{AdmittedRevoke, EndedMachineClient, PersonId, RevokePermission};
use fleet_identity_protocol::EnrollmentId;
use fleet_machine_login::{MachineLoginPlan, PreparedEnrollmentRevocation};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use serde::{Deserialize, Serialize};
use workspace_ids::MachineId;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MachineLoginControl {
    Revoke {
        person_id: PersonId,
        operation_id: AdminOperationId,
        machine_id: MachineId,
        enrollment_id: EnrollmentId,
    },
}

pub async fn machine_login_control<H: CentralStore, S: WorkspaceStores>(
    runtime: &WorkspaceRuntime<H, S>,
    control: MachineLoginControl,
) -> Result<Option<EndedMachineClient>, StoreFailure> {
    let _turn = runtime.execution.take_central_turn().await;
    let time = runtime
        .machine_login_time(runtime.execution.organization())
        .await?;
    match control {
        MachineLoginControl::Revoke {
            person_id,
            operation_id,
            machine_id,
            enrollment_id,
        } => {
            let authority = AdmittedRevoke {
                actor: person_id,
                proof_request_id: String::from(operation_id)
                    .try_into()
                    .map_err(|_| StoreFailure::Unavailable)?,
                permission: RevokePermission::MachineRevoke,
            };
            let prepared = runtime
                .machine_login_service()
                .prepare_revoke_enrollment(authority, &enrollment_id, time)
                .await?;
            match prepared {
                PreparedEnrollmentRevocation::Revoked {
                    machine_id: owner,
                    ended,
                    statements,
                } if owner == machine_id => {
                    runtime
                        .apply_machine_login(MachineLoginPlan {
                            answer: Some(ended),
                            statements,
                        })
                        .await
                }
                PreparedEnrollmentRevocation::Revoked { .. }
                | PreparedEnrollmentRevocation::NotFound
                | PreparedEnrollmentRevocation::AlreadyEnded => Ok(None),
            }
        }
    }
}
