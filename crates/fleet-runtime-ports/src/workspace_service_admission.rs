//! The workspace-service admission rule shared by existing work and session starts.

use central_storage::StoreFailure;

use crate::workspace_decode_error::corrupt;

/// A machine can serve a workspace only with allowance and an active directory entry.
pub fn workspace_is_servable(
    allowance_active: bool,
    directory_state: Option<&str>,
) -> Result<bool, StoreFailure> {
    if !allowance_active {
        return Ok(false);
    }
    match directory_state {
        Some("active") => Ok(true),
        Some("archived") | None => Ok(false),
        Some(_) => Err(corrupt("directory_workspaces.state")),
    }
}
