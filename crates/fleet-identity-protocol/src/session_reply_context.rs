//! Check the parts of a session reply that the originating request determines.

use crate::{
    CreateFleetSession, CreateFleetSessionReply, CreateSessionKind, CreateSessionRefusal,
    EndFleetSession, EndFleetSessionReply, EndSessionTarget, NewAbandonmentCause, NewEndSettlement,
    PresentedSessionSecret, RouterEndCause, SessionCreator, SessionSettlement, SessionTime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionReplyMismatch {
    #[error("only a session creator can receive a session admission refusal")]
    RefusalForOperator,
    #[error("NoSecret must correspond to a session creator presenting Nothing")]
    SecretPresentation,
    #[error("only a delegate create can receive ParentNotValid")]
    ParentForOrdinarySession,
    #[error("only an operation-id End can receive NotYoursToEnd")]
    NotYoursForCentralId,
    #[error("an operation-id End can only newly settle as Abandoned/GivenUp")]
    EndedByOperationId,
    #[error("the new Ended settlement must use the request's cause")]
    EndedCause,
}

/// A Create reply checked against its caller and kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedCreateReply(CreateFleetSessionReply);

impl CheckedCreateReply {
    #[must_use]
    pub fn into_wire(self) -> CreateFleetSessionReply {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
enum CreateCaller {
    Operator,
    SessionWithSecret,
    SessionWithoutSecret,
}

/// The facts needed to check a response, without retaining the credential.
#[derive(Debug, Clone, Copy)]
pub struct CreateReplyContext {
    caller: CreateCaller,
    delegate: bool,
}

impl CreateFleetSession {
    #[must_use]
    pub fn reply_context(&self) -> CreateReplyContext {
        let caller = match &self.creator {
            SessionCreator::RouterOperator {} => CreateCaller::Operator,
            SessionCreator::Session { presented, .. } => match presented {
                PresentedSessionSecret::Secret { .. } => CreateCaller::SessionWithSecret,
                PresentedSessionSecret::Nothing {} => CreateCaller::SessionWithoutSecret,
            },
        };
        CreateReplyContext {
            caller,
            delegate: matches!(self.session_kind, CreateSessionKind::Delegate { .. }),
        }
    }
}

impl CreateReplyContext {
    pub fn check(
        self,
        reply: CreateFleetSessionReply,
    ) -> Result<CheckedCreateReply, SessionReplyMismatch> {
        let no_secret = matches!(reply, CreateFleetSessionReply::NoSecret {});
        if no_secret != matches!(self.caller, CreateCaller::SessionWithoutSecret) {
            return Err(SessionReplyMismatch::SecretPresentation);
        }
        match &reply {
            CreateFleetSessionReply::NoSuchSession {}
            | CreateFleetSessionReply::SessionEnded {}
            | CreateFleetSessionReply::WrongSecret {}
                if matches!(self.caller, CreateCaller::Operator) =>
            {
                Err(SessionReplyMismatch::RefusalForOperator)
            }
            CreateFleetSessionReply::Refused {
                reason: CreateSessionRefusal::ParentNotValid {},
            } if !self.delegate => Err(SessionReplyMismatch::ParentForOrdinarySession),
            CreateFleetSessionReply::Reserved { .. }
            | CreateFleetSessionReply::Superseded {}
            | CreateFleetSessionReply::Active { .. }
            | CreateFleetSessionReply::Settled { .. }
            | CreateFleetSessionReply::Refused { .. }
            | CreateFleetSessionReply::Conflict {}
            | CreateFleetSessionReply::NoSecret {}
            | CreateFleetSessionReply::NoSuchSession {}
            | CreateFleetSessionReply::SessionEnded {}
            | CreateFleetSessionReply::WrongSecret {} => Ok(CheckedCreateReply(reply)),
        }
    }
}

/// The verified outcome of End, with cases specific to the request target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndSessionOutcome {
    ByCentralId(CentralIdEndOutcome),
    ByOperationId(OperationIdEndOutcome),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CentralIdEndOutcome {
    Settled(NewEndSettlement),
    AlreadySettled(SessionSettlement),
    NoSuchStart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationIdEndOutcome {
    Abandoned { time: SessionTime },
    AlreadySettled(SessionSettlement),
    NoSuchStart,
    NotYoursToEnd,
}

impl EndSessionOutcome {
    #[must_use]
    pub fn into_wire(self) -> EndFleetSessionReply {
        match self {
            Self::ByCentralId(outcome) => match outcome {
                CentralIdEndOutcome::Settled(settlement) => {
                    EndFleetSessionReply::Settled { settlement }
                }
                CentralIdEndOutcome::AlreadySettled(settlement) => {
                    EndFleetSessionReply::AlreadySettled { settlement }
                }
                CentralIdEndOutcome::NoSuchStart => EndFleetSessionReply::NoSuchStart {},
            },
            Self::ByOperationId(outcome) => match outcome {
                OperationIdEndOutcome::Abandoned { time } => EndFleetSessionReply::Settled {
                    settlement: NewEndSettlement::Abandoned {
                        cause: NewAbandonmentCause::GivenUp {},
                        time,
                    },
                },
                OperationIdEndOutcome::AlreadySettled(settlement) => {
                    EndFleetSessionReply::AlreadySettled { settlement }
                }
                OperationIdEndOutcome::NoSuchStart => EndFleetSessionReply::NoSuchStart {},
                OperationIdEndOutcome::NotYoursToEnd => EndFleetSessionReply::NotYoursToEnd {},
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum EndReplyContext {
    CentralId { cause: RouterEndCause },
    OperationId,
}

impl EndFleetSession {
    #[must_use]
    pub fn reply_context(&self) -> EndReplyContext {
        match self.target {
            EndSessionTarget::CentralId { .. } => EndReplyContext::CentralId {
                cause: self.cause.clone(),
            },
            EndSessionTarget::OperationId { .. } => EndReplyContext::OperationId,
        }
    }
}

impl EndReplyContext {
    pub fn check(
        self,
        reply: EndFleetSessionReply,
    ) -> Result<EndSessionOutcome, SessionReplyMismatch> {
        match self {
            Self::CentralId { cause } => {
                check_central_id_end(cause, reply).map(EndSessionOutcome::ByCentralId)
            }
            Self::OperationId => {
                check_operation_id_end(reply).map(EndSessionOutcome::ByOperationId)
            }
        }
    }
}

fn check_central_id_end(
    request_cause: RouterEndCause,
    reply: EndFleetSessionReply,
) -> Result<CentralIdEndOutcome, SessionReplyMismatch> {
    match reply {
        EndFleetSessionReply::Settled { settlement } => {
            if let NewEndSettlement::Ended { cause, .. } = &settlement
                && cause != &request_cause
            {
                return Err(SessionReplyMismatch::EndedCause);
            }
            Ok(CentralIdEndOutcome::Settled(settlement))
        }
        EndFleetSessionReply::AlreadySettled { settlement } => {
            Ok(CentralIdEndOutcome::AlreadySettled(settlement))
        }
        EndFleetSessionReply::NoSuchStart {} => Ok(CentralIdEndOutcome::NoSuchStart),
        EndFleetSessionReply::NotYoursToEnd {} => Err(SessionReplyMismatch::NotYoursForCentralId),
    }
}

fn check_operation_id_end(
    reply: EndFleetSessionReply,
) -> Result<OperationIdEndOutcome, SessionReplyMismatch> {
    match reply {
        EndFleetSessionReply::Settled { settlement } => match settlement {
            NewEndSettlement::Abandoned {
                cause: NewAbandonmentCause::GivenUp {},
                time,
            } => Ok(OperationIdEndOutcome::Abandoned { time }),
            NewEndSettlement::Ended { .. } => Err(SessionReplyMismatch::EndedByOperationId),
        },
        EndFleetSessionReply::AlreadySettled { settlement } => {
            Ok(OperationIdEndOutcome::AlreadySettled(settlement))
        }
        EndFleetSessionReply::NoSuchStart {} => Ok(OperationIdEndOutcome::NoSuchStart),
        EndFleetSessionReply::NotYoursToEnd {} => Ok(OperationIdEndOutcome::NotYoursToEnd),
    }
}
