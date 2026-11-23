use fleet_identity_protocol::CreateSessionAttempt;
use workspace_ids::{CentralSessionId, SessionRef};

use crate::{
    ConversationFact, EndLeg, EndNotDue, Exposure, FailureReason, PointerFact, SecretFact,
    StartEnvelope, StartFacts, UnknownReason,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotKind {
    CreateIntended,
    CreateUnknown,
    LaunchIntended,
    Held,
    BindIntended,
    BindUnknown,
    CleaningUp,
    Opened,
    Failed,
    Settled,
    Fenced,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotFacts {
    pub envelope: StartEnvelope,
    pub attempt: CreateSessionAttempt,
    pub facts: StartFacts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenedSession {
    pub address: SessionRef,
    pub session_id: CentralSessionId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CleanupOutcome {
    Failed(FailureReason),
    Settled,
}

/// The facts that would be durable if a record write were confirmed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartSnapshot {
    CreateIntended(Box<SnapshotFacts>),
    CreateUnknown(Box<SnapshotFacts>),
    LaunchIntended(Box<SnapshotFacts>),
    Held(Box<SnapshotFacts>),
    BindIntended(Box<SnapshotFacts>),
    BindUnknown(Box<SnapshotFacts>),
    CleaningUp {
        start: Box<SnapshotFacts>,
        outcome: CleanupOutcome,
    },
    Opened {
        start: Box<SnapshotFacts>,
        session: OpenedSession,
    },
    Failed {
        start: Box<SnapshotFacts>,
        reason: FailureReason,
    },
    Settled(Box<SnapshotFacts>),
    Fenced(Box<SnapshotFacts>),
    Unknown {
        start: Box<SnapshotFacts>,
        reason: UnknownReason,
    },
}

impl StartSnapshot {
    pub const fn kind(&self) -> SnapshotKind {
        match self {
            Self::CreateIntended(_) => SnapshotKind::CreateIntended,
            Self::CreateUnknown(_) => SnapshotKind::CreateUnknown,
            Self::LaunchIntended(_) => SnapshotKind::LaunchIntended,
            Self::Held(_) => SnapshotKind::Held,
            Self::BindIntended(_) => SnapshotKind::BindIntended,
            Self::BindUnknown(_) => SnapshotKind::BindUnknown,
            Self::CleaningUp { .. } => SnapshotKind::CleaningUp,
            Self::Opened { .. } => SnapshotKind::Opened,
            Self::Failed { .. } => SnapshotKind::Failed,
            Self::Settled(_) => SnapshotKind::Settled,
            Self::Fenced(_) => SnapshotKind::Fenced,
            Self::Unknown { .. } => SnapshotKind::Unknown,
        }
    }

    pub fn start(&self) -> &SnapshotFacts {
        match self {
            Self::CreateIntended(start)
            | Self::CreateUnknown(start)
            | Self::LaunchIntended(start)
            | Self::Held(start)
            | Self::BindIntended(start)
            | Self::BindUnknown(start)
            | Self::CleaningUp { start, .. }
            | Self::Opened { start, .. }
            | Self::Failed { start, .. }
            | Self::Settled(start)
            | Self::Fenced(start)
            | Self::Unknown { start, .. } => start,
        }
    }

    #[must_use]
    pub fn must_be_retained(&self) -> bool {
        match self {
            Self::CreateIntended(_)
            | Self::CreateUnknown(_)
            | Self::LaunchIntended(_)
            | Self::Held(_)
            | Self::BindIntended(_)
            | Self::BindUnknown(_)
            | Self::CleaningUp { .. } => true,
            Self::Opened { start, .. }
            | Self::Failed { start, .. }
            | Self::Settled(start)
            | Self::Fenced(start)
            | Self::Unknown { start, .. } => {
                let facts = &start.facts;
                let central_clear = facts.create_exposure == Exposure::None
                    || facts.settlement.is_some()
                    || matches!(
                        facts.end,
                        EndLeg::NoSuchStart | EndLeg::NotDue(EndNotDue::NoSuchStart)
                    );
                let conversation_clear = matches!(
                    facts.conversation,
                    ConversationFact::None | ConversationFact::Closed
                );
                let secret_clear =
                    facts.secret == SecretFact::None && facts.pointer == PointerFact::None;
                !(central_clear && conversation_clear && secret_clear)
            }
        }
    }
}
