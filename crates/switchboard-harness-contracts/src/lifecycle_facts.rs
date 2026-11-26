//! Harness observations and refusal guidance for lifecycle calls.

use std::sync::Arc;

use crate::{
    CloseAttempt, EventPort, LoadAttempt, ModelReasoningMetadata, OwnerAddress,
    ProviderConfigOption, ProviderSettingsCatalogRecord, ReservedReply, RunAttempt,
    SessionGeneration, TurnId, WriterState,
};

pub type LifecycleReply<Fact> = Box<dyn ReservedReply<Fact>>;

/// The receiver binds this port to the exact owner. Each generation-scoped observation
/// identifies its own run within that owner.
pub type LifecycleEventPort<Event, SkillView, TurnOutcome, AuthStatus> =
    Arc<dyn EventPort<LifecycleFact<Event, SkillView, TurnOutcome, AuthStatus>>>;

pub enum LifecycleFact<Event, SkillView, TurnOutcome, AuthStatus> {
    /// Complete load history in ingress order. Empty history is valid. Unavailable
    /// replay settles the open as a failure instead of sending this fact.
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
    /// The connection supplies its observed auth status. The controller applies it only
    /// to its current live run's known capability report.
    ConnectionAuthChanged { status: AuthStatus },
    /// The whole owner lost its harness, even if it had no live generation yet.
    HarnessGone,
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
    Reconnect,
    Reopen,
    CreateFromSavedSetup,
    WaitForClose,
    UseSavedCatalogue,
    ReportUnavailable,
    KeepOutcomeUnknown,
    AdmitAgain,
    Retire,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleRefusal {
    Encoding,
    Writer(WriterState),
    RouteUnavailable,
    RouteClosing,
    AttemptMismatch,
    OpeningNotHeld,
    CatalogueUnavailable,
    InvalidCatalogueReply,
    OperationNotOffered,
}

impl LifecycleRefusal {
    #[must_use]
    pub const fn valid_moves(self) -> &'static [LifecycleMove] {
        match self {
            Self::Encoding | Self::AttemptMismatch => &[LifecycleMove::CorrectRequest],
            Self::Writer(WriterState::Full) => &[LifecycleMove::WaitForWriter],
            Self::Writer(WriterState::Stopped) => &[LifecycleMove::Reconnect],
            Self::RouteUnavailable | Self::OpeningNotHeld => &[LifecycleMove::Reopen],
            Self::RouteClosing => &[LifecycleMove::WaitForClose],
            Self::CatalogueUnavailable | Self::InvalidCatalogueReply => &[
                LifecycleMove::UseSavedCatalogue,
                LifecycleMove::ReportUnavailable,
            ],
            Self::OperationNotOffered => &[LifecycleMove::ReportUnavailable],
        }
    }
}
