use fleet_identity_protocol::{
    BindFleetSessionReply, CreateFleetSession, CreateFleetSessionReply, EndFleetSession,
    EndFleetSessionReply, SessionSecret,
};
use state_machine_core::{EventTag, RefusalTag};
use workspace_ids::{CentralSessionId, RequestId, SessionId};

use crate::{
    CentralStanding, EnvelopeRefusal, OpeningCommit, OpeningOutcome, SecretIssue, StartAnswer,
    StartEnvelope, StartFault, StartRevision, StartSnapshot,
};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct StartInstanceId(RequestId);

impl StartInstanceId {
    pub const fn new(request_id: RequestId) -> Self {
        Self(request_id)
    }

    pub const fn request_id(&self) -> &RequestId {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DirectiveId {
    pub instance: StartInstanceId,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WaiterSlot(u64);

impl WaiterSlot {
    pub const fn new(slot: u64) -> Self {
        Self(slot)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartInput {
    Ask {
        slot: WaiterSlot,
        candidate: Result<StartEnvelope, EnvelopeRefusal>,
        central: CentralStanding,
    },
    DirectiveResult {
        id: DirectiveId,
        result: DirectiveResult,
    },
}

impl EventTag for StartInput {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Ask { .. } => "Ask",
            Self::DirectiveResult { result, .. } => result.tag(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum StartEffect {
    Directive(Directed),
    Notification(StartNotification),
}

#[derive(Debug, Eq, PartialEq)]
pub struct Directed {
    pub id: DirectiveId,
    pub directive: StartDirective,
}

#[derive(Debug, Eq, PartialEq)]
pub enum StartDirective {
    Save {
        expected: Option<StartRevision>,
        snapshot: StartSnapshot,
    },
    SendCreate(CreateFleetSession),
    PutSecret {
        issue: SecretIssue,
        secret: SessionSecret,
    },
    Launch,
    SetUp,
    SendBind {
        session_id: CentralSessionId,
        harness_session_id: SessionId,
        issue: SecretIssue,
    },
    CommitOpening {
        expected: StartRevision,
        opening: Box<OpeningCommit>,
    },
    SendEnd(EndFleetSession),
    CloseConversation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StartNotification {
    Tell {
        slot: WaiterSlot,
        answer: StartAnswer,
    },
    ReportFault(StartFault),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DirectiveResult {
    Save(SaveResult),
    SendCreate(LinkResult<CreateFleetSessionReply, LinkNotTaken>),
    PutSecret(PutSecretResult),
    Launch(LaunchResult),
    SetUp(SetUpResult),
    SendBind(LinkResult<BindFleetSessionReply, BindNotTaken>),
    CommitOpening(OpeningOutcome),
    SendEnd(LinkResult<EndFleetSessionReply, LinkNotTaken>),
    CloseConversation(CloseResult),
}

impl DirectiveResult {
    const fn tag(&self) -> &'static str {
        match self {
            Self::Save(_) => "Save",
            Self::SendCreate(_) => "SendCreate",
            Self::PutSecret(_) => "PutSecret",
            Self::Launch(_) => "Launch",
            Self::SetUp(_) => "SetUp",
            Self::SendBind(_) => "SendBind",
            Self::CommitOpening(_) => "CommitOpening",
            Self::SendEnd(_) => "SendEnd",
            Self::CloseConversation(_) => "CloseConversation",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaveResult {
    Saved(StartRevision),
    NotCommitted,
    RevisionMismatch,
    OutcomeNotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkNotTaken {
    LinkNotReady,
    EnrollmentEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindNotTaken {
    LinkNotReady,
    EnrollmentEnded,
    SecretUnavailable,
    HoldLost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkResult<Reply, NotTaken> {
    Answered(Reply),
    NotTaken(NotTaken),
    AnswerLost,
    ProvenNotWritten,
    Contradicted,
    EnrollmentEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutSecretResult {
    Stored,
    NotTaken(EnrollmentEnded),
    NotStored,
    OutcomeNotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EnrollmentEnded;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchNotTaken {
    LinkNotReady,
    EnrollmentEnded,
    ReservationElapsed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchResult {
    Started { harness_session_id: SessionId },
    NotTaken(LaunchNotTaken),
    FailedNoConversation,
    FailedDuplicateId,
    OutcomeNotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetUpNotTaken {
    EnrollmentEnded,
    HoldLost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetUpResult {
    Done,
    NotTaken(SetUpNotTaken),
    Failed,
    OutcomeNotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseResult {
    Closed,
    CloseFailed,
    OutcomeNotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StartRefusal {
    #[error("the result does not identify the outstanding directive")]
    NotTheOutstandingDirective,
    #[error("the result is not expected in this state")]
    NotExpectedHere,
    #[error("no directive sequences remain in this machine instance")]
    DirectiveSequenceExhausted,
}

impl RefusalTag for StartRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::NotTheOutstandingDirective => "NotTheOutstandingDirective",
            Self::NotExpectedHere => "NotExpectedHere",
            Self::DirectiveSequenceExhausted => "DirectiveSequenceExhausted",
        }
    }
}
