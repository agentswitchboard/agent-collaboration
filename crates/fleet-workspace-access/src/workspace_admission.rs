//! Live request admission. Callers hold the central turn through the admitted effect.
use crate::WorkspaceAccess;
use central_storage::{SqlValue, Statement, StoreFailure};
use fleet_contracts::{LiveMachineConnection, MachineConnectionAttachment};
use fleet_runtime_ports::stored_values::column;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use workspace_model::{Caller, RefusalReason, WorkspaceId};

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceAccess<'_, Host, Stores> {
    pub async fn admit_workspace(
        &self,
        live: &impl LiveMachineConnection,
        caller: &Caller,
        attachment: &MachineConnectionAttachment,
        workspace: Option<&WorkspaceId>,
    ) -> Result<Result<(), RefusalReason>, StoreFailure> {
        if attachment.organization_id() != self.execution.organization()
            || caller.machine_id() != attachment.machine_id()
        {
            return Ok(Err(RefusalReason::MachineRevoked));
        }
        if live.restore_held(attachment).await?.is_err() {
            return Ok(Err(RefusalReason::MachineRevoked));
        }
        if let Some(workspace) = workspace {
            let allowed = live.active_workspaces_held(attachment.machine_id()).await?;
            if !allowed.contains(workspace) {
                return Ok(Err(RefusalReason::WorkspaceNotAllowed));
            }
            let rows = self.execution.statement_host().read(Statement {
                sql: "SELECT state AS workspace_state FROM directory_workspaces WHERE workspace_id=?",
                args: vec![SqlValue::Text(workspace.to_string())],
            }).await?;
            let Some(row) = rows.rows.first() else {
                return Ok(Err(RefusalReason::WorkspaceNotAllowed));
            };
            if rows.rows.len() != 1 {
                return Err(corrupt("directory_workspaces.workspace_id"));
            }
            match column(&rows, row, "workspace_state")? {
                SqlValue::Text(value) if value == "active" => {}
                SqlValue::Text(value) if value == "archived" => {
                    return Ok(Err(RefusalReason::WorkspaceNotAllowed));
                }
                SqlValue::Null => return Ok(Err(RefusalReason::WorkspaceNotAllowed)),
                _ => return Err(corrupt("directory_workspaces.state")),
            }
        }
        Ok(Ok(()))
    }
}
