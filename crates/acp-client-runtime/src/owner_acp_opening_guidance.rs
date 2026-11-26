//! State-based guidance for an ACP lifecycle call that cannot proceed.

use provider_session_machines::harness_lifecycle::{ActivationRefusal, LifecycleRefusal};
use switchboard_harness_contracts::{HeldOpeningKind, WriterState};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriterAvailability {
    Live,
    Stopped,
    ConnectionRetired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OpeningPhase {
    None,
    Opening,
    Held(HeldOpeningKind),
    Configuring,
    Configured,
    Consumed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AttemptRelation {
    Same,
    Different,
    NoStoredMatch,
    Unnamed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OpeningRefusal {
    WriterStopped,
    ConnectionRetired,
    InProgress,
    AlreadyHeld(HeldOpeningKind),
    NotOffered,
    AttemptMismatch,
    Missing,
}

pub(super) const fn classify_refusal(
    writer: WriterAvailability,
    phase: OpeningPhase,
    caller: AttemptRelation,
) -> OpeningRefusal {
    use AttemptRelation::{Different, NoStoredMatch, Same, Unnamed};
    use OpeningPhase as Phase;
    use WriterAvailability::{ConnectionRetired, Live, Stopped};

    match (writer, phase, caller) {
        (ConnectionRetired, _, _) => OpeningRefusal::ConnectionRetired,
        (Stopped, _, _) => OpeningRefusal::WriterStopped,
        (Live, _, Different) => OpeningRefusal::AttemptMismatch,
        (Live, Phase::None, Same | NoStoredMatch | Unnamed) => OpeningRefusal::Missing,
        (Live, Phase::Opening | Phase::Configuring, Same | NoStoredMatch | Unnamed) => {
            OpeningRefusal::InProgress
        }
        (Live, Phase::Held(kind), Same | NoStoredMatch | Unnamed) => {
            OpeningRefusal::AlreadyHeld(kind)
        }
        (Live, Phase::Configured | Phase::Consumed, Same | NoStoredMatch | Unnamed) => {
            OpeningRefusal::NotOffered
        }
    }
}

impl OpeningRefusal {
    pub(super) const fn lifecycle(self) -> LifecycleRefusal {
        match self {
            Self::WriterStopped => LifecycleRefusal::Writer(WriterState::Stopped),
            Self::ConnectionRetired => LifecycleRefusal::ConnectionRetired,
            Self::InProgress => LifecycleRefusal::OpeningInProgress,
            Self::AlreadyHeld(kind) => LifecycleRefusal::OpeningAlreadyHeld(kind),
            Self::NotOffered => LifecycleRefusal::OperationNotOffered,
            Self::AttemptMismatch => LifecycleRefusal::AttemptMismatch,
            Self::Missing => LifecycleRefusal::OpeningNotHeld,
        }
    }

    pub(super) const fn activation(self) -> ActivationRefusal {
        match self {
            Self::WriterStopped => ActivationRefusal::WriterStopped,
            Self::ConnectionRetired => ActivationRefusal::ConnectionRetired,
            Self::InProgress => ActivationRefusal::OpeningInProgress,
            Self::AlreadyHeld(kind) => ActivationRefusal::OpeningAlreadyHeld(kind),
            Self::NotOffered => ActivationRefusal::OperationNotOffered,
            Self::AttemptMismatch => ActivationRefusal::AttemptMismatch,
            Self::Missing => ActivationRefusal::OpeningNotHeld,
        }
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_guidance_test.rs"]
mod tests;
