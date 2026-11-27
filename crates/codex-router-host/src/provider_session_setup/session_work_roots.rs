//! Work-roots policy and hook command for Router-started provider sessions.

use std::path::{Path, PathBuf};

/// A provider's work-roots rule, as its startup configured it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProviderWorkRoots {
    /// The configured roots, already expanded to absolute paths.
    pub(crate) roots: Vec<PathBuf>,
    /// The hook that makes Claude ask outside the roots.
    pub(crate) hook: WorkRootsHook,
}

/// Whether a provider's sessions in auto have their work-roots hook.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) enum WorkRootsHook {
    /// The provider needs none (Cursor: the Router's allowance decides).
    #[default]
    NotNeeded,
    /// Installed, with the Node that runs it.
    Ready(AskOutsideHook),
    /// Claude needs it and the Host couldn't set it up: an auto start is
    /// refused rather than run without it (D1 R18).
    Unavailable(String),
}

/// The installed hook and the Node that runs it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AskOutsideHook {
    pub(crate) node: PathBuf,
    pub(crate) hook: PathBuf,
}

impl AskOutsideHook {
    /// The command Claude runs before a write or a shell call, naming this
    /// session's roots.
    pub(crate) fn command(&self, roots: &[PathBuf]) -> String {
        let roots = serde_json::to_string(
            &roots
                .iter()
                .map(|root| root.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|_| "[]".to_owned());
        format!(
            "{} {} {}",
            shell_quoted(&self.node.to_string_lossy()),
            shell_quoted(&self.hook.to_string_lossy()),
            shell_quoted(&roots)
        )
    }
}

/// A session's roots beyond its working and scratch folders: the configured
/// list, the project's tmp/, and the roots its creator named, once each.
pub(crate) fn session_work_roots(
    configured: &[PathBuf],
    working_directory: &Path,
    named: &[PathBuf],
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for root in configured
        .iter()
        .cloned()
        .chain(std::iter::once(working_directory.join("tmp")))
        .chain(named.iter().cloned())
    {
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    roots
}

fn shell_quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "'\\''"))
}

#[cfg(test)]
#[path = "session_work_roots_test.rs"]
mod tests;
