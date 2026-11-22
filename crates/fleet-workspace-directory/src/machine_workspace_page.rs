//! Pages a machine's permitted workspaces under the caller's central turn.

#[cfg(all(test, not(target_arch = "wasm32")))]
use crate::workspace_directory_rows::StoredWorkspaceRow;
use crate::workspace_directory_rows::{CentralWorkspaceDirectory, decode_row};
use central_storage::{ReadOutcome, SafeInteger, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::{
    CentralStore,
    guarded_statements::text,
    stored_values::{column, string},
    workspace_decode_error::corrupt,
};
use workspace_model::{
    ChangePosition, ItemView, ListCursor, ListOrder, PageLimit, SearchQuery, WorkspaceAnswer,
    WorkspaceId,
};

impl<Host: CentralStore> CentralWorkspaceDirectory<'_, Host> {
    pub async fn read_machine_workspace_page(
        &self,
        permitted: &[WorkspaceId],
        search: Option<&SearchQuery>,
        include_archived: bool,
        cursor: ListCursor,
        limit: PageLimit,
    ) -> Result<WorkspaceAnswer, StoreFailure> {
        let empty_page = || WorkspaceAnswer::Page {
            items: Vec::new(),
            next_cursor: None,
        };
        if permitted.is_empty() {
            return Ok(empty_page());
        }

        // ListCursor positions count every organization workspace, including
        // archived and unpermitted workspaces. Only the ID index is needed here.
        let oldest_first = cursor.order() == ListOrder::OldestFirst;
        let after = match cursor.position().map(ChangePosition::get) {
            None => None,
            Some(0) if oldest_first => None,
            Some(0) => return Ok(empty_page()),
            Some(ordinal) => {
                let offset = i64::try_from(ordinal - 1)
                    .map_err(|_| corrupt("directory_workspaces.page_offset"))?;
                let rows = self.host.read(workspace_id_at_offset(offset)?).await?;
                let boundary = decode_workspace_id(&rows)?;
                if oldest_first && boundary.is_none() {
                    return Ok(empty_page());
                }
                boundary
            }
        };
        let rows = self
            .host
            .read(machine_workspace_page(
                permitted,
                search,
                include_archived,
                after.as_ref(),
                oldest_first,
                limit,
            )?)
            .await?;
        let mut workspaces = rows
            .rows
            .iter()
            .map(|row| decode_row(&rows, row))
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = workspaces.len() > usize::from(limit.get());
        workspaces.truncate(usize::from(limit.get()));
        let next_cursor = match workspaces.last().filter(|_| has_more) {
            Some(workspace) => {
                let rows = self
                    .host
                    .read(workspace_ordinal(&workspace.workspace_id))
                    .await?;
                Some(cursor.continue_past(decode_workspace_ordinal(&rows)?))
            }
            None => None,
        };
        Ok(WorkspaceAnswer::Page {
            items: workspaces
                .into_iter()
                .map(|workspace| ItemView::Workspace(workspace.into()))
                .collect(),
            next_cursor,
        })
    }
}

fn machine_workspace_page(
    permitted: &[WorkspaceId],
    search: Option<&SearchQuery>,
    include_archived: bool,
    after: Option<&WorkspaceId>,
    oldest_first: bool,
    limit: PageLimit,
) -> Result<Statement, StoreFailure> {
    let permitted =
        serde_json::to_string(permitted).map_err(|_| corrupt("directory_workspaces.permitted"))?;
    let search = search.map(SearchQuery::as_str);
    let include_archived = i64::from(include_archived);
    let after = after.map(WorkspaceId::as_str);
    let oldest_first = i64::from(oldest_first);
    let limit = i64::from(limit.lookahead());
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredWorkspaceRow,
        "src/queries/read_machine_workspace_page.sql",
        permitted,
        include_archived,
        search,
        after,
        oldest_first,
        limit,
    );
    Ok(Statement {
        sql: include_str!("queries/read_machine_workspace_page.sql"),
        args: vec![
            SqlValue::Text(permitted),
            directory_integer(include_archived)?,
            search.map_or(SqlValue::Null, text),
            after.map_or(SqlValue::Null, text),
            directory_integer(oldest_first)?,
            directory_integer(limit)?,
        ],
    })
}

fn workspace_id_at_offset(offset: i64) -> Result<Statement, StoreFailure> {
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredWorkspaceIdRow,
        "src/queries/read_workspace_id_at_ordinal.sql",
        offset,
    );
    Ok(Statement {
        sql: include_str!("queries/read_workspace_id_at_ordinal.sql"),
        args: vec![directory_integer(offset)?],
    })
}

struct StoredWorkspaceIdRow {
    workspace_id: Option<String>,
}

fn decode_workspace_id(rows: &ReadOutcome) -> Result<Option<WorkspaceId>, StoreFailure> {
    let row = match rows.rows.as_slice() {
        [] => return Ok(None),
        [row] => row,
        _ => return Err(corrupt("directory_workspaces.workspace_id")),
    };
    let stored = StoredWorkspaceIdRow {
        workspace_id: Some(string(rows, row, "workspace_id")?.to_owned()),
    };
    let workspace = stored
        .workspace_id
        .ok_or_else(|| corrupt("directory_workspaces.workspace_id"))?
        .try_into()
        .map_err(|_| corrupt("directory_workspaces.workspace_id"))?;
    Ok(Some(workspace))
}

fn workspace_ordinal(workspace: &WorkspaceId) -> Statement {
    let workspace_id = workspace.as_str();
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(
        StoredWorkspaceOrdinalRow,
        "src/queries/read_workspace_ordinal.sql",
        workspace_id,
    );
    Statement {
        sql: include_str!("queries/read_workspace_ordinal.sql"),
        args: vec![text(workspace_id)],
    }
}

struct StoredWorkspaceOrdinalRow {
    ordinal: i64,
}

fn decode_workspace_ordinal(rows: &ReadOutcome) -> Result<ChangePosition, StoreFailure> {
    let [row] = rows.rows.as_slice() else {
        return Err(corrupt("directory_workspaces.ordinal"));
    };
    let SqlValue::Integer(ordinal) = column(rows, row, "ordinal!: i64")? else {
        return Err(corrupt("directory_workspaces.ordinal"));
    };
    let stored = StoredWorkspaceOrdinalRow {
        ordinal: ordinal.get(),
    };
    u64::try_from(stored.ordinal)
        .ok()
        .filter(|ordinal| *ordinal > 0)
        .and_then(|ordinal| ordinal.try_into().ok())
        .ok_or_else(|| corrupt("directory_workspaces.ordinal"))
}

fn directory_integer(value: i64) -> Result<SqlValue, StoreFailure> {
    SafeInteger::new(value)
        .map(SqlValue::Integer)
        .map_err(|_| corrupt("directory_workspaces.page"))
}
