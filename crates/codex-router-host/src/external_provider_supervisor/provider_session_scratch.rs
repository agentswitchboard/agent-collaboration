//! The optional scratch preparation supplied to a provider create.

use std::{path::PathBuf, sync::Arc};

pub(crate) type PrepareProviderScratch = Arc<
    dyn Fn() -> futures_util::future::BoxFuture<'static, Result<PathBuf, &'static str>>
        + Send
        + Sync,
>;

pub(super) async fn prepare_provider_session_scratch(
    prepare_scratch: Option<&PrepareProviderScratch>,
) -> Result<Option<PathBuf>, &'static str> {
    match prepare_scratch {
        Some(prepare) => prepare().await.map(Some),
        None => Ok(None),
    }
}

#[cfg(test)]
#[path = "provider_session_scratch_test.rs"]
mod tests;
