//! Live request admission. Callers hold the central turn through the admitted effect.
use crate::WorkspaceAccess;
use central_storage::{ReadOutcome, SqlValue, Statement, StoreFailure};
use fleet_contracts::{
    LiveMachineConnection, MachineConnectionAttachment, mail_link::MachineLinkCloseCode,
};
use fleet_runtime_ports::stored_values::column;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use workspace_model::{Caller, RefusalReason, WorkspaceId, WorkspaceState};

struct StoredWorkspaceStateRow {
    workspace_state: String,
}

impl StoredWorkspaceStateRow {
    fn decode(&self) -> Result<WorkspaceState, StoreFailure> {
        match self.workspace_state.as_str() {
            "active" => Ok(WorkspaceState::Active),
            "archived" => Ok(WorkspaceState::Archived),
            _ => Err(corrupt("directory_workspaces.state")),
        }
    }
}

fn read_workspace_state(workspace: &WorkspaceId) -> Statement {
    let workspace_id = workspace.as_str();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredWorkspaceStateRow,
        "src/queries/read_workspace_state.sql",
        workspace_id
    );
    Statement {
        sql: include_str!("queries/read_workspace_state.sql"),
        args: vec![SqlValue::Text(workspace_id.to_owned())],
    }
}

fn decode_workspace_state(rows: &ReadOutcome) -> Result<Option<WorkspaceState>, StoreFailure> {
    let row = match rows.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(corrupt("directory_workspaces.workspace_id")),
    };
    match column(rows, row, "workspace_state")? {
        SqlValue::Text(value) => StoredWorkspaceStateRow {
            workspace_state: value.clone(),
        }
        .decode()
        .map(Some),
        SqlValue::Null => Ok(None),
        _ => Err(corrupt("directory_workspaces.state")),
    }
}

pub enum WorkspaceAdmissionRefusal {
    Authority(MachineLinkCloseCode),
    Workspace(RefusalReason),
}

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceAccess<'_, Host, Stores> {
    pub async fn admit_workspace(
        &self,
        live: &impl LiveMachineConnection,
        caller: &Caller,
        attachment: &MachineConnectionAttachment,
        workspace: Option<&WorkspaceId>,
    ) -> Result<Result<(), WorkspaceAdmissionRefusal>, StoreFailure> {
        if attachment.organization_id() != self.execution.organization()
            || caller.machine_id() != attachment.machine_id()
        {
            return Ok(Err(WorkspaceAdmissionRefusal::Workspace(
                RefusalReason::MachineRevoked,
            )));
        }
        if let Err(cause) = live.restore_held(attachment).await? {
            return Ok(Err(WorkspaceAdmissionRefusal::Authority(cause)));
        }
        if let Some(workspace) = workspace {
            if !live
                .allows_workspace_held(attachment.machine_id(), workspace)
                .await?
            {
                return Ok(Err(WorkspaceAdmissionRefusal::Workspace(
                    RefusalReason::WorkspaceNotAllowed,
                )));
            }
            let rows = self
                .execution
                .statement_host()
                .read(read_workspace_state(workspace))
                .await?;
            if decode_workspace_state(&rows)? != Some(WorkspaceState::Active) {
                return Ok(Err(WorkspaceAdmissionRefusal::Workspace(
                    RefusalReason::WorkspaceNotAllowed,
                )));
            }
        }
        Ok(Ok(()))
    }
}

#[cfg(test)]
#[path = "workspace_admission_test.rs"]
mod tests;
