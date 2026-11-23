//! Read a named session project's membership at its owning workspace.

use central_storage::{SqlValue, Statement, StoreFailure};
use fleet_identity_protocol::ProjectRef;
use fleet_runtime_ports::{CentralStore, WorkspaceStores, workspace_decode_error::corrupt};

use crate::fleet_runtime::FleetRuntime;

impl<Host: CentralStore, Stores: WorkspaceStores> FleetRuntime<Host, Stores> {
    pub async fn read_session_start_project(
        &self,
        project: &ProjectRef,
    ) -> Result<bool, StoreFailure> {
        // A project cannot leave its workspace while it has sessions (E16).
        // No workspace command moves or deletes a project today. The owning
        // store's answer therefore holds across the later organization write.
        let found = self
            .read_workspace(
                &project.workspace,
                Statement {
                    sql: include_str!("queries/session_start_project_exists.sql"),
                    args: vec![SqlValue::Text(project.project.as_str().to_owned())],
                },
            )
            .await?;
        match found.rows.as_slice() {
            [] => Ok(false),
            [row] if row.as_slice() == [SqlValue::Text(project.project.as_str().to_owned())] => {
                Ok(true)
            }
            _ => Err(corrupt("workspace_projects.project_id")),
        }
    }
}
