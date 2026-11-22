//! Closed test-support dispatch to the real private revocation decision.
use crate::WorkspaceRuntime;
use crate::person_request::AdminOperationId;
use central_storage::StoreFailure;
use fleet_contracts::{EndedMachineClient, PersonId};
use fleet_identity_protocol::EnrollmentId;
use fleet_machine_login::policy::{self as machine_login, EnrollmentQuery};
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
            let query = EnrollmentQuery {
                machine_id: Some(machine_id),
                enrollment: Some(enrollment_id.clone()),
                ..EnrollmentQuery::default()
            };
            let facts = fleet_machine_login::enrollment_reads::facts(
                runtime.execution.statement_host(),
                runtime.execution.organization().clone(),
                &query,
            )
            .await?;
            if facts
                .active
                .as_ref()
                .is_none_or(|active| active.id != enrollment_id)
            {
                return Ok(None);
            }
            let decided = machine_login::revoke_machine_enrollment(
                &facts,
                &person_id,
                &String::from(operation_id)
                    .try_into()
                    .map_err(|_| StoreFailure::Unavailable)?,
                "machine:revoke",
                time,
                runtime.execution.ids(),
            )?;
            runtime.apply_machine_login(&facts, decided, time).await
        }
    }
}
