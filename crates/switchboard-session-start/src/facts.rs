use fleet_identity_protocol::{
    CreateSessionAttempt, EnrollmentId, NewEndSettlement, SessionSettlement, SessionTime,
};
use workspace_ids::{CentralSessionId, SessionId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretIssue {
    pub enrollment: EnrollmentId,
    pub session_id: CentralSessionId,
    pub attempt: CreateSessionAttempt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Exposure {
    None,
    MayHaveReached,
    Witnessed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reservation {
    pub session_id: CentralSessionId,
    pub reserved_until: SessionTime,
    pub attempt: CreateSessionAttempt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretFact {
    None,
    Stored(SecretIssue),
    PossiblyStored(SecretIssue),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PointerFact {
    None,
    Committed(SecretIssue),
    PossiblyCommitted(SecretIssue),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchFact {
    NotSent,
    Sent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConversationFact {
    None,
    Held(Option<SessionId>),
    Closed,
    CloseFailed,
    NotKnown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndNotDue {
    NothingToEnd,
    RanOutAtCentral,
    BindMayHaveReached,
    AssociationDisputed,
    NoSuchStart,
    Settled,
    CentralFault,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndNotSent {
    LinkNotReady,
    EnrollmentEnded,
    RecordUnavailable,
    CentralFault,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndLeg {
    NotDue(EndNotDue),
    Due,
    Settled(NewEndSettlement),
    AlreadySettled(SessionSettlement),
    NoSuchStart,
    NotSent(EndNotSent),
    NotKnown,
    AuthorityRefused,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Recording {
    Recorded,
    NotRecorded,
    NotKnown,
    Conflict,
}

/// The evidence that travels with a start. Secret storage is represented by its issue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartFacts {
    pub create_exposure: Exposure,
    pub reservation: Option<Reservation>,
    pub secret: SecretFact,
    pub pointer: PointerFact,
    pub launch: LaunchFact,
    pub conversation: ConversationFact,
    pub bind_exposure: Exposure,
    pub settlement: Option<SessionSettlement>,
    pub end: EndLeg,
}

impl Default for StartFacts {
    fn default() -> Self {
        Self {
            create_exposure: Exposure::None,
            reservation: None,
            secret: SecretFact::None,
            pointer: PointerFact::None,
            launch: LaunchFact::NotSent,
            conversation: ConversationFact::None,
            bind_exposure: Exposure::None,
            settlement: None,
            end: EndLeg::NotDue(EndNotDue::NothingToEnd),
        }
    }
}
