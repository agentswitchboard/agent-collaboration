//! Credential-free results and evidence delivered by the checked link bridge.

use fleet_identity_protocol::{CreateSessionRefusal, SessionSettlement, SessionTime};
use workspace_ids::{CentralSessionId, SessionRef};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateAnswer {
    Reserved {
        session_id: CentralSessionId,
        reserved_until: SessionTime,
    },
    Superseded,
    Active {
        session_id: CentralSessionId,
        address: SessionRef,
    },
    Settled {
        settlement: SessionSettlement,
    },
    Refused {
        reason: CreateSessionRefusal,
    },
    Conflict,
    NoSecret,
    NoSuchSession,
    SessionEnded,
    WrongSecret,
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

impl From<&CreateAnswer> for CreateReplyKind {
    fn from(reply: &CreateAnswer) -> Self {
        match reply {
            CreateAnswer::Reserved { .. } => Self::Reserved,
            CreateAnswer::Superseded => Self::Superseded,
            CreateAnswer::Active { .. } => Self::Active,
            CreateAnswer::Settled { .. } => Self::Settled,
            CreateAnswer::Refused { reason } => Self::Refused(reason.clone()),
            CreateAnswer::Conflict => Self::Conflict,
            CreateAnswer::NoSecret => Self::NoSecret,
            CreateAnswer::NoSuchSession => Self::NoSuchSession,
            CreateAnswer::SessionEnded => Self::SessionEnded,
            CreateAnswer::WrongSecret => Self::WrongSecret,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedReservation {
    pub session_id: CentralSessionId,
    pub reserved_until: SessionTime,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateContradiction {
    pub reply: Option<CreateReplyKind>,
    pub reservation: Option<ObservedReservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindReplyKind {
    Active,
    WrongSecret,
    AlreadyBoundToDifferentId,
    AddressTaken,
    Settled,
    NoSuchStart,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BindContradiction {
    pub reply: Option<BindReplyKind>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndReplyKind {
    Settled,
    AlreadySettled,
    NoSuchStart,
    NotYoursToEnd,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EndContradiction {
    pub reply: Option<EndReplyKind>,
}
