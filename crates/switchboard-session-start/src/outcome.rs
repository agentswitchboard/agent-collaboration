use workspace_ids::{CentralSessionId, SessionRef};

use crate::{
    BindDispute, CentralRefusalNow, CreateReplyKind, EnvelopeRefusal, FailureReason,
    IntendedOutcome, Recording, SnapshotKind, StartFacts, StartScope, StopReason, UnknownReason,
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
        intended: Option<IntendedOutcome>,
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
    RefusedByCentralNow {
        reason: CentralRefusalNow,
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
pub enum StartFault {
    CreateContradicted {
        reply: Option<CreateReplyKind>,
        context: FaultContext,
    },
    BindContradicted(FaultContext),
    BindAssociationDisputed(BindDispute, FaultContext),
    SecretIssueCollision(FaultContext),
    EndContradicted(FaultContext),
    EndNotYours(FaultContext),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultContext {
    pub scope: StartScope,
    pub facts: Box<StartFacts>,
}
