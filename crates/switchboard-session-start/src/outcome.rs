use fleet_identity_protocol::{CreateFleetSessionReply, CreateSessionRefusal};
use workspace_ids::{CentralSessionId, SessionRef};

use crate::{
    BindDispute, EnvelopeRefusal, FailureReason, Recording, SnapshotKind, StartFacts, StartScope,
    StopReason, UnknownReason,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CentralStanding {
    Ready,
    LinkNotReady,
    NotEnrolled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinishedOutcome {
    Failed(FailureReason),
    Settled,
    Fenced,
    Unknown(UnknownReason),
    Stopped {
        why: StopReason,
        while_saving: SnapshotKind,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotSentNowReason {
    LinkNotReady,
    SecretUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartAnswer {
    Started {
        address: SessionRef,
        session_id: CentralSessionId,
    },
    RefusedNoIdentity,
    Refused(EnvelopeRefusal),
    RefusedDifferentRequest,
    RefusedNoCentral(CentralStanding),
    NotSentNow {
        why: NotSentNowReason,
        facts: Box<StartFacts>,
    },
    OutcomeNotKnown {
        facts: Box<StartFacts>,
    },
    Finished {
        outcome: FinishedOutcome,
        facts: Box<StartFacts>,
        recording: Recording,
    },
    Interrupted {
        facts: Box<StartFacts>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateReplyKind {
    Reserved,
    Superseded,
    Active,
    Settled,
    Refused(CreateSessionRefusal),
    Conflict,
    NoSecret,
    NoSuchSession,
    SessionEnded,
    WrongSecret,
}

impl From<&CreateFleetSessionReply> for CreateReplyKind {
    fn from(reply: &CreateFleetSessionReply) -> Self {
        match reply {
            CreateFleetSessionReply::Reserved { .. } => Self::Reserved,
            CreateFleetSessionReply::Superseded {} => Self::Superseded,
            CreateFleetSessionReply::Active { .. } => Self::Active,
            CreateFleetSessionReply::Settled { .. } => Self::Settled,
            CreateFleetSessionReply::Refused { reason } => Self::Refused(reason.clone()),
            CreateFleetSessionReply::Conflict {} => Self::Conflict,
            CreateFleetSessionReply::NoSecret {} => Self::NoSecret,
            CreateFleetSessionReply::NoSuchSession {} => Self::NoSuchSession,
            CreateFleetSessionReply::SessionEnded {} => Self::SessionEnded,
            CreateFleetSessionReply::WrongSecret {} => Self::WrongSecret,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartFault {
    CreateContradicted {
        reply: Option<CreateReplyKind>,
        context: FaultContext,
    },
    BindContradicted(FaultContext),
    BindAssociationDisputed(BindDispute, FaultContext),
    EndContradicted(FaultContext),
    EndNotYours(FaultContext),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultContext {
    pub scope: StartScope,
    pub facts: Box<StartFacts>,
}
