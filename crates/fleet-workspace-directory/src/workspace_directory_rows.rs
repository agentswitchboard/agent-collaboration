//! Directory reads and write planning against the caller's central store.
//!
//! The caller admits the request and applies guarded plans under its own turn,
//! with its own receipt.
use central_storage::{
    GuardedStatement, ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure,
};
use fleet_contracts::{
    DirectoryWorkspace, MAXIMUM_DESCRIPTION_BYTES, WorkspaceDirectory, WorkspaceView,
};
use fleet_runtime_ports::CentralStore;
use fleet_runtime_ports::{
    guarded_statements::{exact, text},
    stored_values::string,
    workspace_decode_error::corrupt,
};
use workspace_model::{Description, ResourceName, Timestamp, WorkspaceId, WorkspaceState};

fn decode(rows: &ReadOutcome) -> Result<Option<DirectoryWorkspace>, StoreFailure> {
    if rows.rows.len() > 1 {
        return Err(corrupt("directory_workspaces.workspace_id"));
    }
    rows.rows
        .first()
        .map(|row| decode_row(rows, row))
        .transpose()
}

pub(super) struct StoredWorkspaceRow {
    pub(super) workspace_id: Option<String>,
    pub(super) name: String,
    pub(super) description: String,
    pub(super) state: String,
    pub(super) created_at: String,
    pub(super) updated_at: String,
}

pub(super) fn decode_row(
    rows: &ReadOutcome,
    row: &[central_storage::SqlValue],
) -> Result<DirectoryWorkspace, StoreFailure> {
    let stored_text = |name| string(rows, row, name).map(str::to_owned);
    StoredWorkspaceRow {
        workspace_id: Some(stored_text("workspace_id")?),
        name: stored_text("name")?,
        description: stored_text("description")?,
        state: stored_text("state")?,
        created_at: stored_text("created_at")?,
        updated_at: stored_text("updated_at")?,
    }
    .decode()
}

impl StoredWorkspaceRow {
    fn decode(self) -> Result<DirectoryWorkspace, StoreFailure> {
        let workspace_id = self
            .workspace_id
            .ok_or_else(|| corrupt("directory_workspaces.workspace_id"))?
            .try_into()
            .map_err(|_| corrupt("directory_workspaces.workspace_id"))?;
        let name = ResourceName::try_from(self.name.clone())
            .map_err(|_| corrupt("directory_workspaces.name"))?;
        if name.as_str() != self.name {
            return Err(corrupt("directory_workspaces.name"));
        }
        if self.description.len() > MAXIMUM_DESCRIPTION_BYTES {
            return Err(corrupt("directory_workspaces.description"));
        }
        let description = Description::try_from(self.description)
            .map_err(|_| corrupt("directory_workspaces.description"))?;
        let state = match self.state.as_str() {
            "active" => WorkspaceState::Active,
            "archived" => WorkspaceState::Archived,
            _ => return Err(corrupt("directory_workspaces.state")),
        };
        for (column, timestamp) in [
            ("created_at", self.created_at),
            ("updated_at", self.updated_at),
        ] {
            let _: Timestamp = timestamp
                .parse()
                .map_err(|_| corrupt(&format!("workspaces.{column}")))?;
        }
        Ok(DirectoryWorkspace {
            workspace_id,
            name,
            description,
            state,
        })
    }
}

pub struct WorkspaceDirectoryPage {
    pub workspaces: Vec<WorkspaceView>,
    pub next: Option<message_board::WorkspaceId>,
}

pub struct CentralWorkspaceDirectory<'store, Host: CentralStore> {
    pub(super) host: &'store Host,
}

impl<'store, Host: CentralStore> CentralWorkspaceDirectory<'store, Host> {
    /// Borrows the central store of one organization.
    pub fn new(host: &'store Host) -> Self {
        Self { host }
    }

    /// Reads the organization directory in ID order, looking one row ahead.
    pub async fn read_workspace_page(
        &self,
        after: Option<&message_board::WorkspaceId>,
        limit: u16,
    ) -> Result<WorkspaceDirectoryPage, StoreFailure> {
        let cursor = after.map_or("", message_board::WorkspaceId::as_str);
        let read_limit = i64::from(limit) + 1;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file_as!(
            StoredWorkspaceRow,
            "src/queries/read_workspace_page.sql",
            cursor,
            read_limit
        );
        let read_limit =
            SafeInteger::new(read_limit).map_err(|_| corrupt("directory_workspaces.page_limit"))?;
        let rows = self
            .host
            .read(Statement {
                sql: include_str!("queries/read_workspace_page.sql"),
                args: vec![text(cursor), SqlValue::Integer(read_limit)],
            })
            .await?;
        let mut workspaces = rows
            .rows
            .iter()
            .map(|row| self.workspace_view(decode_row(&rows, row)?))
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = workspaces.len() > usize::from(limit);
        workspaces.truncate(usize::from(limit));
        let next = workspaces
            .last()
            .filter(|_| has_more)
            .map(|workspace| workspace.workspace_id.clone());
        Ok(WorkspaceDirectoryPage { workspaces, next })
    }
}

impl<Host: CentralStore> WorkspaceDirectory for CentralWorkspaceDirectory<'_, Host> {
    async fn read_by_id(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<DirectoryWorkspace>, StoreFailure> {
        let rows = self.host.read(Statement {
            sql: "SELECT workspace_id,name,description,state,created_at,updated_at FROM directory_workspaces WHERE workspace_id=?",
            args: vec![text(workspace_id)],
        }).await?;
        let workspace = decode(&rows)?;
        if workspace
            .as_ref()
            .is_some_and(|workspace| workspace.workspace_id != *workspace_id)
        {
            return Err(corrupt("directory_workspaces.workspace_id"));
        }
        Ok(workspace)
    }

    async fn read_by_name(
        &self,
        name: &ResourceName,
    ) -> Result<Option<DirectoryWorkspace>, StoreFailure> {
        // Match workspaces_canonical_name exactly: SQLite folds ASCII only and trim()
        // removes surrounding ASCII spaces. No Rust/Unicode comparison substitutes.
        let rows = self.host.read(Statement {
            sql: "SELECT workspace_id,name,description,state,created_at,updated_at FROM directory_workspaces WHERE lower(trim(name))=lower(trim(?))",
            args: vec![text(name.as_str())],
        }).await?;
        decode(&rows)
    }

    fn plan_guarded_insert(
        &self,
        workspace_id: &WorkspaceId,
        name: &ResourceName,
        description: &Description,
        now: Timestamp,
    ) -> Result<GuardedStatement, StoreFailure> {
        Ok(exact(
            "INSERT INTO directory_workspaces(workspace_id,name,description,state,created_at,updated_at) VALUES (?,?,?,'active',?,?)",
            vec![
                text(workspace_id),
                text(name.as_str()),
                text(description.as_str()),
                text(now.to_rfc3339()),
                text(now.to_rfc3339()),
            ],
        ))
    }

    fn workspace_view(&self, row: DirectoryWorkspace) -> Result<WorkspaceView, StoreFailure> {
        Ok(WorkspaceView {
            workspace_id: row
                .workspace_id
                .to_string()
                .try_into()
                .map_err(|_| corrupt("directory_workspaces.workspace_id"))?,
            name: row
                .name
                .as_str()
                .to_owned()
                .try_into()
                .map_err(|_| corrupt("directory_workspaces.name"))?,
            description: row
                .description
                .as_str()
                .to_owned()
                .try_into()
                .map_err(|_| corrupt("directory_workspaces.description"))?,
            state: match row.state {
                WorkspaceState::Active => message_board::WorkspaceState::Active,
                WorkspaceState::Archived => message_board::WorkspaceState::Archived,
            },
        })
    }
}

#[cfg(test)]
#[path = "workspace_directory_rows_test.rs"]
mod tests;
