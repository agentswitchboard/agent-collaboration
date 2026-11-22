//! Directory operations used by workspace requests and organization admin.
use central_storage::{GuardedStatement, StoreFailure};
use std::future::Future;
use workspace_model::{Description, ResourceName, Timestamp, WorkspaceId, WorkspaceState};

pub struct DirectoryWorkspace {
    pub workspace_id: WorkspaceId,
    pub name: ResourceName,
    pub description: Description,
    pub state: WorkspaceState,
}

/// The verified admin view of the org-owned workspace directory.
#[derive(
    Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct WorkspaceView {
    pub workspace_id: message_board::WorkspaceId,
    pub name: message_board::ResourceName,
    #[serde(deserialize_with = "crate::workspace_description::deserialize_description")]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = message_board::Description))]
    #[schemars(schema_with = "crate::workspace_description::description_schema")]
    pub description: message_board::Description,
    pub state: message_board::WorkspaceState,
}

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
const _: fn(&WorkspaceView) -> &message_board::Description = |workspace| &workspace.description;

impl From<DirectoryWorkspace> for workspace_model::Workspace {
    fn from(row: DirectoryWorkspace) -> Self {
        Self {
            workspace_id: row.workspace_id,
            name: row.name,
            description: row.description,
            state: row.state,
        }
    }
}

pub trait WorkspaceDirectory: Send + Sync {
    fn read_by_id(
        &self,
        workspace_id: &WorkspaceId,
    ) -> impl Future<Output = Result<Option<DirectoryWorkspace>, StoreFailure>> + Send;

    fn read_by_name(
        &self,
        name: &ResourceName,
    ) -> impl Future<Output = Result<Option<DirectoryWorkspace>, StoreFailure>> + Send;

    fn plan_guarded_insert(
        &self,
        workspace_id: &WorkspaceId,
        name: &ResourceName,
        description: &Description,
        now: Timestamp,
    ) -> Result<GuardedStatement, StoreFailure>;

    fn workspace_view(&self, row: DirectoryWorkspace) -> Result<WorkspaceView, StoreFailure>;
}
