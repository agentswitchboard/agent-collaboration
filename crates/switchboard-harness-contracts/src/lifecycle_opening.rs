//! Opening requests, correlation, and evidence retained before adoption.

use std::{path::PathBuf, sync::Arc};

use crate::{
    LifecycleMove, LifecycleRefusal, LoadAttempt, OwnerAddress, ProviderSettingsCatalogRecord,
    ProviderWireFailure, RestoreVia, RunAttempt, SessionGeneration, SettingApplication,
    SettingValueId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessSessionId(pub String);

/// The caller supplies the current create operation UUID before a generation exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FastRequest {
    #[default]
    Leave,
    OffIfOffered,
    Off,
}

/// The harness installs the caller's folder grant according to its own capabilities.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionWorkRoots {
    pub roots: Vec<PathBuf>,
    pub ask_outside_command: Option<String>,
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

pub enum RestoredOpening {
    /// The owner accepted the complete replay before the harness published this reply.
    Loaded { readback: RestoreLoadReadback },
    /// Resume provides readback without making a claim about replay.
    Resumed { readback: RestoreLoadReadback },
}

/// The same projection used to decide restore preparation. The catalogue crosses as data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreLoadReadback {
    pub catalogue: Arc<ProviderSettingsCatalogRecord>,
    pub router_auto_requested: bool,
    pub mode: RestoredMode,
    pub fast_off: Option<SettingApplication>,
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
        accepted_fallback: Option<SettingValueId>,
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
    ReplayNotKnown,
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
            Self::ProviderError(_) | Self::ReplayNotKnown | Self::Closed => {
                &[LifecycleMove::Reopen]
            }
            Self::Disconnected => &[LifecycleMove::Reopen, LifecycleMove::KeepOutcomeUnknown],
            Self::NoAnswer | Self::Unavailable { .. } => &[LifecycleMove::KeepOutcomeUnknown],
        }
    }
}
