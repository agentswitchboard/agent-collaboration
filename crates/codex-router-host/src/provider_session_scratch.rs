//! The scratch folder a Claude or Cursor session is created with (R4).
//!
//! Each create gets `<router root>/scratch/session-<uuidv7>`, prepared the same
//! way as a Codex session's (owner-private, stamped as used, under the scope
//! lock the Host's sweep honours). The folder is not registered as open: as
//! `collaboration_runtime::OpenScratchScopes` records, these sessions are kept
//! by the use stamp and the folder's newest change.

use crate::external_provider_supervisor::PrepareProviderScratch;
use router_clock::RuntimeClock;
use std::{path::PathBuf, sync::Arc};

pub(crate) fn provider_scratch_preparer(
    scratch_root: PathBuf,
    clock: Arc<dyn RuntimeClock>,
) -> PrepareProviderScratch {
    Arc::new(move || {
        let scratch_root = scratch_root.clone();
        let clock = clock.clone();
        Box::pin(async move { prepare_fresh_folder(scratch_root, clock.as_ref()).await })
    })
}

async fn prepare_fresh_folder(
    scratch_root: PathBuf,
    clock: &dyn RuntimeClock,
) -> Result<PathBuf, &'static str> {
    let scope = collaboration_runtime::new_service_uuid()
        .map_err(|_| "session scratch scope could not be minted")?;
    let folder = scratch_root.join(format!("session-{}", String::from(scope)));
    let prepared = folder.clone();
    let used_at = std::time::SystemTime::from(clock.utc_now());
    tokio::task::spawn_blocking(move || {
        codex_native_integration::prepare_scratch_scope_used_at(&prepared, used_at)
    })
    .await
    .map_err(|_| "session scratch folder preparation did not complete")?
    .map_err(|_| "session scratch folder could not be prepared")?;
    Ok(folder)
}

#[cfg(test)]
#[path = "provider_session_scratch_test.rs"]
mod tests;
