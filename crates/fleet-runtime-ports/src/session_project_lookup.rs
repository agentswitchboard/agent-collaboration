//! The pre-transaction project lookup carried to the organization writer.

use central_storage::StoreFailure;
use workspace_ids::{ProjectId, WorkspaceId};

/// The workspace read is useful only for the exact named request that led to it.
/// The organization must still recheck the machine's allowance before using it.
pub enum SessionProjectLookup {
    NotRead,
    Named {
        workspace: WorkspaceId,
        project: ProjectId,
        result: Result<bool, StoreFailure>,
    },
}
