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

/// The take refusal vocabulary associated with each checked call's evidence.
pub trait LinkContradiction {
    type NotTaken: Clone + std::fmt::Debug + Eq;
}

impl LinkContradiction for CreateContradiction {
    type NotTaken = LinkNotTaken;
}

impl LinkContradiction for BindContradiction {
    type NotTaken = BindNotTaken;
}

impl LinkContradiction for EndContradiction {
    type NotTaken = LinkNotTaken;
}

/// The outcome of the exactly correlated link call.
///
/// The bridge checks positive reply currency at delivery and stages a Reserved
/// credential only after that check. If the reducer refuses that delivery, the
/// bridge destroys its staged credential and preserves any earlier pending slot.
/// A successful reservation gives the slot to the start instance. The bridge
/// moves its credential out at PutSecret take and destroys the slot when the
/// machine is dropped. The bridge also matches Bind Active against both address
/// fields. An enrollment-ended result needs a terminal close on the current
/// connection or a confirmed refresh. An advisory AuthorityLapsed refusal maps
/// to RefusedBeforeApply. An expired lease allows reconnecting under the same
/// enrollment. A close on an older connection gives no evidence about the current one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkResult<Reply, Contradiction: LinkContradiction> {
    Answered(Reply),
    NotTaken(Contradiction::NotTaken),
    AnswerLost,
    ProvenNotWritten,
    RefusedBeforeApply(CentralRefusalNow),
    Contradicted(Contradiction),
    EnrollmentEnded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CentralRefusalNow {
    AuthorityLapsed,
}
