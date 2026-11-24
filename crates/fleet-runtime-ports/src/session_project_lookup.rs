//! A project membership result issued by the workspace owner read.

use central_storage::{SqlValue, Statement, StoreCall, StoreFailure, StoreReply};
use workspace_ids::{ProjectId, WorkspaceId};

use crate::{WorkspaceStores, workspace_decode_error::corrupt};

pub enum SessionProjectLookup {
    NotRead,
    Named(NamedProjectMembership),
}

/// An owner read is tied to both coordinates and consumed by the writer.
///
/// ```compile_fail,E0451
/// use fleet_runtime_ports::NamedProjectMembership;
/// let workspace = "019a3dce-2000-7000-8000-000000000004".parse().unwrap();
/// let project = "019a3dce-2000-7000-8000-000000000005".parse().unwrap();
/// let _ = NamedProjectMembership { workspace, project, result: Ok(true) };
/// ```
pub struct NamedProjectMembership {
    workspace: WorkspaceId,
    project: ProjectId,
    result: Result<bool, StoreFailure>,
}

impl SessionProjectLookup {
    pub async fn read_named(
        stores: &impl WorkspaceStores,
        workspace: &WorkspaceId,
        project: &ProjectId,
    ) -> Self {
        // E16 prevents a move while the project has sessions. No workspace
        // command moves or deletes a project today, so this answer stays valid
        // across the later organization transaction.
        let project_value = SqlValue::Text(project.as_str().to_owned());
        let outcome = stores
            .call_workspace(
                workspace,
                StoreCall::Read(Statement {
                    sql: include_str!("queries/session_start_project_exists.sql"),
                    args: vec![project_value.clone()],
                }),
            )
            .await;
        let result = match outcome {
            Ok(StoreReply::Read(found)) => match found.rows.as_slice() {
                [] => Ok(false),
                [row] if row.as_slice() == [project_value] => Ok(true),
                _ => Err(corrupt("workspace_projects.project_id")),
            },
            Ok(StoreReply::Batch(_)) => Err(corrupt("session project store reply")),
            Err(failure) => Err(failure),
        };
        Self::Named(NamedProjectMembership {
            workspace: workspace.clone(),
            project: project.clone(),
            result,
        })
    }

    /// Returns absence if preflight had no permission to make the owner read.
    pub fn consume_for(
        self,
        workspace: &WorkspaceId,
        project: &ProjectId,
    ) -> Result<Option<bool>, StoreFailure> {
        match self {
            Self::NotRead => Ok(None),
            Self::Named(checked)
                if &checked.workspace == workspace && &checked.project == project =>
            {
                checked.result.map(Some)
            }
            Self::Named(_) => Err(corrupt("session project lookup target")),
        }
    }

    #[cfg(feature = "test-support")]
    pub fn fixture_named_project(workspace: &WorkspaceId, project: &ProjectId) -> Self {
        Self::Named(NamedProjectMembership {
            workspace: workspace.clone(),
            project: project.clone(),
            result: Ok(true),
        })
    }
}
