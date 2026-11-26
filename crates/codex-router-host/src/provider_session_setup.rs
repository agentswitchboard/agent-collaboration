//! What a binding's sessions are set up with, kept on the binding's runtime
//! for its whole life (D1 R18; owner Decision 3): the work-roots rule the
//! binding was configured with, and for each session the mode its create
//! asked for and why the agent's mode differs. A create builds a session's
//! folders from them; a load or resume rebuilds them from the session's
//! record, whichever path restores it.

use crate::provider_work_roots::{ProviderWorkRoots, WorkRootsHook};
use provider_session_machines::harness_lifecycle::{
    AUTO_PERMISSION_MODE, OpenSetup, PermissionModeReport, SessionWorkRoots,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use switchboard_session_store::ProviderSessionResumeInputs;

/// A session's requested mode and why the agent's mode differs, for show.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RequestedModeRecord {
    pub(crate) requested_mode: Option<String>,
    pub(crate) mode_reason: Option<collaboration_protocol::ProviderModeReason>,
}

impl RequestedModeRecord {
    fn from_report(report: &PermissionModeReport) -> Self {
        Self {
            requested_mode: report.requested.clone(),
            mode_reason: report
                .reason
                .map(crate::provider_operation_settlement::provider_mode_reason),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct BindingSessionSetup {
    work_roots: Mutex<ProviderWorkRoots>,
    requested_modes: Mutex<HashMap<String, RequestedModeRecord>>,
}

impl BindingSessionSetup {
    pub(crate) fn set_work_roots(&self, work_roots: ProviderWorkRoots) {
        *self
            .work_roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = work_roots;
    }

    pub(crate) fn work_roots(&self) -> ProviderWorkRoots {
        self.work_roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn record_requested_mode(
        &self,
        provider_session_id: &str,
        record: RequestedModeRecord,
    ) {
        self.requested_modes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(provider_session_id.to_owned(), record);
    }

    pub(crate) fn record_created_mode(
        &self,
        provider_session_id: &str,
        report: &PermissionModeReport,
    ) {
        self.record_requested_mode(
            provider_session_id,
            RequestedModeRecord::from_report(report),
        );
    }

    /// Keeps a restored session's mode report when its record named the mode
    /// its create asked for; a session recorded before that was kept keeps
    /// whatever this Host already holds.
    #[cfg(test)]
    pub(crate) fn record_restored_mode(
        &self,
        provider_session_id: &str,
        report: &PermissionModeReport,
    ) {
        if report.requested.is_some() {
            self.record_created_mode(provider_session_id, report);
        }
    }

    pub(crate) fn requested_mode(&self, provider_session_id: &str) -> RequestedModeRecord {
        self.requested_modes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(provider_session_id)
            .cloned()
            .unwrap_or_default()
    }
}

/// A session's setup, rebuilt as its create built it: in auto, the binding's
/// configured roots and the roots its creator named; in a named mode, only
/// its own folders. A record without a requested mode (written before these
/// were kept) rebuilds nothing.
pub(crate) fn restore_setup(
    configured: &ProviderWorkRoots,
    cwd: PathBuf,
    resume: &ProviderSessionResumeInputs,
) -> OpenSetup {
    let Some(requested_mode) = resume.requested_mode.as_deref() else {
        return OpenSetup {
            cwd,
            work_roots: SessionWorkRoots::default(),
            scratch_folder: None,
        };
    };
    let auto = requested_mode == AUTO_PERMISSION_MODE;
    let scratch_folder = resume
        .scratch_folder
        .clone()
        .map(|folder| PathBuf::from(String::from(folder)));
    let configured = if auto {
        configured.clone()
    } else {
        ProviderWorkRoots::default()
    };
    let named: Vec<PathBuf> = if auto {
        resume
            .work_roots
            .iter()
            .flatten()
            .map(|root| PathBuf::from(String::from(root.clone())))
            .collect()
    } else {
        Vec::new()
    };
    let work_roots = session_work_roots(&configured, &cwd, scratch_folder.as_deref(), &named, auto);
    OpenSetup {
        cwd,
        scratch_folder,
        work_roots,
    }
}

/// A session's work roots for its agent: the configured roots, its project's
/// tmp/ and the roots its creator named, and for Claude the command its hook
/// runs over every folder the session works in.
pub(crate) fn session_work_roots(
    configured: &ProviderWorkRoots,
    cwd: &Path,
    scratch_folder: Option<&Path>,
    named: &[PathBuf],
    router_auto: bool,
) -> SessionWorkRoots {
    let ready_hook = match &configured.hook {
        WorkRootsHook::Ready(hook) => Some(hook),
        _ => None,
    };
    if configured.roots.is_empty() && ready_hook.is_none() && named.is_empty() {
        return SessionWorkRoots {
            router_auto,
            ..SessionWorkRoots::default()
        };
    }
    let roots = crate::provider_work_roots::session_work_roots(&configured.roots, cwd, named);
    let ask_outside_command = ready_hook.map(|hook| {
        let inside = std::iter::once(cwd.to_path_buf())
            .chain(scratch_folder.map(Path::to_path_buf))
            .chain(roots.iter().cloned())
            .collect::<Vec<_>>();
        hook.command(&inside)
    });
    SessionWorkRoots {
        roots,
        ask_outside_command,
        router_auto,
    }
}

#[cfg(test)]
#[path = "provider_session_setup_test.rs"]
mod tests;
