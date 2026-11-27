//! Opening requests, correlation, and evidence retained before adoption.

use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use crate::{
    LifecycleMove, LifecycleRefusal, LoadAttempt, OwnerAddress, ProviderSettingsCatalogRecord,
    ProviderWireFailure, RestoreVia, RunAttempt, SessionGeneration, SettingApplication,
    SettingValueId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessSessionId(pub String);

/// The caller supplies the current create operation UUID before a generation exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct CreateAttempt(pub uuid::Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenAttempt {
    Create(CreateAttempt),
    Restore(RunAttempt<LoadAttempt>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenSetup {
    pub cwd: PathBuf,
    pub work_roots: SessionWorkRoots,
    pub scratch_folder: Option<PathBuf>,
}

/// Settings requested for the new session. A harness may need these before it can open.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RequestedProviderSettings {
    pub mode: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub fast: FastRequest,
}

impl RequestedProviderSettings {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mode.is_none()
            && self.model.is_none()
            && self.effort.is_none()
            && self.fast == FastRequest::Leave
    }
}

/// What a request asks of the agent's fast option. The Router can turn it off.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FastRequest {
    /// Leave the current value alone.
    #[default]
    Leave,
    /// The create default. Turn fast off when the agent offers the option.
    OffIfOffered,
    /// Explicit fast off. A harness without the option refuses the setting.
    Off,
}

/// The folders a Router-started session in auto works in without asking,
/// beyond its working folder (D1 R18).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionWorkRoots {
    pub roots: Vec<PathBuf>,
    pub ask_outside_command: Option<String>,
    /// The session starts in Router auto and requires the boundary these roots define.
    pub router_auto: bool,
}

pub enum OpenRequest {
    Create {
        owner: OwnerAddress,
        attempt: CreateAttempt,
        setup: OpenSetup,
        requested: RequestedProviderSettings,
    },
    Restore {
        owner: OwnerAddress,
        run: RunAttempt<LoadAttempt>,
        session: HarnessSessionId,
        via: RestoreVia,
        setup: OpenSetup,
        requested_mode: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Held {
    Held,
    NotHeld(LifecycleRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationSetup {
    Created {
        attempt: CreateAttempt,
        generation: SessionGeneration,
    },
    Restored(RunAttempt<LoadAttempt>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayCompleteness {
    Complete,
    Incomplete { skipped_updates: NonZeroUsize },
}

pub enum RestoredOpening {
    /// The owner accepted all captured events before the harness sent this reply.
    Loaded {
        readback: RestoreLoadReadback,
        accepted_mode_fallback: Option<SettingValueId>,
        replay: ReplayCompleteness,
    },
    /// Resume provides readback without making a claim about replay.
    Resumed {
        readback: RestoreLoadReadback,
        accepted_mode_fallback: Option<SettingValueId>,
    },
}

/// The same projection used to decide restore preparation. The catalogue crosses as data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreLoadReadback<Catalogue = ProviderSettingsCatalogRecord> {
    pub catalogue: Arc<Catalogue>,
    pub router_auto_requested: bool,
    pub mode: RestoredMode,
    /// Fast was read back on. This identifies the application that puts it back to off.
    pub fast_on: Option<SettingApplication>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoredMode {
    Observed,
    NotOffered {
        current: Option<SettingValueId>,
    },
    Reapply {
        application: SettingApplication,
        value: SettingValueId,
    },
}

pub enum OpeningFact {
    /// The caller validates the ID and checks duplicate ownership before adopting and
    /// configuring it. The opening remains held at its provisional create attempt.
    Created {
        attempt: CreateAttempt,
        session: HarnessSessionId,
    },
    CreateFailed {
        attempt: CreateAttempt,
        remote: CreationEffect,
        failure: OpeningFailure,
    },
    Restored {
        run: RunAttempt<LoadAttempt>,
        opening: RestoredOpening,
    },
    RestoreFailed {
        run: RunAttempt<LoadAttempt>,
        failure: OpeningFailure,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreationEffect {
    None,
    Applied,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpeningFailure {
    Lost {
        reported: Option<ProviderWireFailure>,
    },
    Refused(ProviderWireFailure),
    ProviderError(ProviderWireFailure),
    Disconnected,
    NoAnswer,
    OutcomeUnknown,
    ReplayNotKnown,
    /// A close displaced the opening. The close can still own the route.
    Closed,
    /// An uncoded diagnostic already sanitized by the harness adapter.
    Unavailable {
        diagnostic: String,
    },
}

impl OpeningFailure {
    #[must_use]
    pub const fn valid_moves(&self) -> &'static [LifecycleMove] {
        match self {
            Self::Lost { .. } => &[LifecycleMove::CreateFromSavedSetup],
            Self::Refused(_) => &[
                LifecycleMove::CorrectRequest,
                LifecycleMove::ReportUnavailable,
            ],
            Self::ProviderError(_) | Self::ReplayNotKnown => &[LifecycleMove::Reopen],
            Self::Disconnected => &[LifecycleMove::StopOwner],
            Self::Closed => &[LifecycleMove::WaitForClose],
            Self::NoAnswer | Self::OutcomeUnknown | Self::Unavailable { .. } => {
                &[LifecycleMove::KeepOutcomeUnknown]
            }
        }
    }
}
