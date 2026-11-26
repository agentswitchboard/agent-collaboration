//! Harness observations and refusal guidance for lifecycle calls.

use std::sync::Arc;

use crate::{
    CloseAttempt, EventPort, LoadAttempt, ModelReasoningMetadata, OwnerAddress,
    ProviderConfigOption, ProviderSettingsCatalogRecord, ReservedReply, RunAttempt,
    SessionGeneration, TurnId, WriterState,
};

pub type LifecycleReply<Fact> = Box<dyn ReservedReply<Fact>>;

/// The receiver binds this port to an exact opening attempt or activated run.
pub type LifecycleEventPort<Event, SkillView, TurnOutcome> =
    Arc<dyn EventPort<LifecycleFact<Event, SkillView, TurnOutcome>>>;

pub enum LifecycleFact<Event, SkillView, TurnOutcome> {
    /// Captured load history in ingress order. The opening answer reports capture
    /// completeness. Empty history is valid, and unavailable replay fails the open.
    Replay {
        run: RunAttempt<LoadAttempt>,
        events: Vec<Event>,
    },
    Stream {
        run: SessionGeneration,
        turn: StreamTurn,
        event: Event,
    },
    CatalogueObserved {
        run: SessionGeneration,
        update: ProviderSettingsUpdate,
    },
    SkillView {
        run: SessionGeneration,
        view: SkillView,
    },
    ObservedTurnEnded {
        run: SessionGeneration,
        turn: TurnId,
        outcome: TurnOutcome,
    },
    OutputLimit {
        run: SessionGeneration,
        turn: TurnId,
    },
    /// This reader can no longer observe its run. The harness may still be alive.
    RunObservationFailed { run: SessionGeneration },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamTurn {
    /// The reader or callback captured the turn, or observed that there was none.
    Observed(Option<TurnId>),
    /// The producer knows the generation. The receiving controller looks up its current turn.
    AtIngress,
}

/// A decoded catalogue delta. An omitted reasoning update keeps the prior reasoning metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderSettingsUpdate {
    CurrentMode(String),
    ConfigOptions {
        options: Vec<ProviderConfigOption>,
        model_reasoning: Option<ModelReasoningMetadata>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogueRequest {
    Harness,
    Session {
        owner: OwnerAddress,
        run: SessionGeneration,
    },
}

pub enum CatalogueFact {
    Reported(Arc<ProviderSettingsCatalogRecord>),
    Unavailable(LifecycleRefusal),
}

/// A close observation never substitutes for local release of the connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseFact {
    Closed {
        run: RunAttempt<CloseAttempt>,
    },
    RefusedStillLive {
        run: RunAttempt<CloseAttempt>,
    },
    NotOffered {
        run: RunAttempt<CloseAttempt>,
    },
    Unknown {
        run: RunAttempt<CloseAttempt>,
        cause: CloseUncertainty,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseUncertainty {
    ReplyLost,
    InvalidReply,
    EffectUnconfirmed,
}

impl CloseFact {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [LifecycleMove] {
        match self {
            Self::Closed { .. } | Self::NotOffered { .. } => &[LifecycleMove::Retire],
            Self::RefusedStillLive { .. } => &[LifecycleMove::AdmitAgain, LifecycleMove::Retire],
            Self::Unknown { .. } => &[LifecycleMove::KeepOutcomeUnknown],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleMove {
    CorrectRequest,
    WaitForWriter,
    /// Start a new opening attempt through the owner binding.
    Reopen,
    CreateFromSavedSetup,
    WaitForClose,
    UseSavedCatalogue,
    ReportUnavailable,
    /// The owner cannot keep executing with this binding or completion path.
    StopOwner,
    KeepOutcomeUnknown,
    KeepActiveRun,
    AdmitAgain,
    Retire,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivationRefusal {
    AttemptMismatch,
    OpeningNotHeld,
    AlreadyActive,
    RouteUnavailable,
}

impl ActivationRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [LifecycleMove] {
        match self {
            Self::AttemptMismatch => &[LifecycleMove::CorrectRequest],
            Self::OpeningNotHeld | Self::RouteUnavailable => &[LifecycleMove::ReportUnavailable],
            Self::AlreadyActive => &[LifecycleMove::KeepActiveRun, LifecycleMove::Retire],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleRefusal {
    Encoding,
    Writer(WriterState),
    RouteUnavailable,
    RouteClosing,
    AttemptMismatch,
    OpeningNotHeld,
    SettingValueNotOffered,
    SettingSetterUnavailable,
    CatalogueUnavailable,
    InvalidCatalogueReply,
    OperationNotOffered,
}

impl LifecycleRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [LifecycleMove] {
        match self {
            Self::Encoding | Self::AttemptMismatch | Self::SettingValueNotOffered => {
                &[LifecycleMove::CorrectRequest]
            }
            Self::Writer(WriterState::Full) => &[LifecycleMove::WaitForWriter],
            Self::Writer(WriterState::Stopped) => &[LifecycleMove::StopOwner],
            Self::RouteUnavailable | Self::OpeningNotHeld => &[LifecycleMove::Reopen],
            Self::RouteClosing => &[LifecycleMove::WaitForClose],
            Self::CatalogueUnavailable | Self::InvalidCatalogueReply => &[
                LifecycleMove::UseSavedCatalogue,
                LifecycleMove::ReportUnavailable,
            ],
            Self::OperationNotOffered | Self::SettingSetterUnavailable => {
                &[LifecycleMove::ReportUnavailable]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreSettingRefusal {
    Harness(LifecycleRefusal),
    ReplyReservation(WriterState),
}

impl RestoreSettingRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [LifecycleMove] {
        match self {
            Self::Harness(refusal) => refusal.valid_moves(),
            Self::ReplyReservation(_) => &[LifecycleMove::StopOwner],
        }
    }
}
