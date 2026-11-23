use central_engine_model::TransitionError;
use central_link_protocol::{CentralCallReply, MachineLinkCloseCode};
use central_storage::StoreFailure;
use fleet_actor_identity::{SessionMachineRefusal, SessionServiceError};
use fleet_identity_protocol::SessionReplyMismatch;

/// An applying callback rejects its transaction on every failure.
#[derive(Debug)]
pub enum SessionApplyFailure {
    Store(StoreFailure),
    EntropyUnavailable,
    Terminal(SessionApplyFault),
}

#[derive(Debug)]
pub enum SessionApplyFault {
    ReservationTimeOutOfRange,
    InvalidSessionId,
    InvalidCreateContents,
    SessionMachine(TransitionError<SessionMachineRefusal>),
    ApplyingClockOutOfRange,
    CheckedReply(SessionReplyMismatch),
    InconsistentEnrollment,
}

/// These outcomes may leave a transaction successfully. Wire faults are projected afterwards.
pub enum SessionApplySuccess {
    Answered(CentralCallReply),
    AuthorityLapsed(MachineLinkCloseCode),
}

pub type SessionApplyResult = Result<SessionApplySuccess, SessionApplyFailure>;

impl From<StoreFailure> for SessionApplyFailure {
    fn from(failure: StoreFailure) -> Self {
        Self::Store(failure)
    }
}

impl From<SessionReplyMismatch> for SessionApplyFailure {
    fn from(failure: SessionReplyMismatch) -> Self {
        Self::Terminal(SessionApplyFault::CheckedReply(failure))
    }
}

impl From<SessionServiceError> for SessionApplyFailure {
    fn from(failure: SessionServiceError) -> Self {
        match failure {
            SessionServiceError::Store(failure) => Self::Store(failure),
            SessionServiceError::SecretUnavailable => Self::EntropyUnavailable,
            SessionServiceError::TimeOutOfRange => {
                Self::Terminal(SessionApplyFault::ReservationTimeOutOfRange)
            }
            SessionServiceError::InvalidSessionId => {
                Self::Terminal(SessionApplyFault::InvalidSessionId)
            }
            SessionServiceError::InvalidCreateContents => {
                Self::Terminal(SessionApplyFault::InvalidCreateContents)
            }
            SessionServiceError::Machine(failure) => {
                Self::Terminal(SessionApplyFault::SessionMachine(failure))
            }
        }
    }
}

/// Test-only: Reserved is emitted by Create only after the session write.
#[cfg(feature = "test-support")]
pub fn inject_reserved_reply_fault(
    armed: &std::sync::atomic::AtomicBool,
    result: SessionApplyResult,
) -> SessionApplyResult {
    use fleet_identity_protocol::CreateFleetSessionReply;
    use std::sync::atomic::Ordering;
    if matches!(&result,
        Ok(SessionApplySuccess::Answered(CentralCallReply::CreateFleetSession(envelope)))
        if matches!(envelope.body, CreateFleetSessionReply::Reserved { .. })
    ) && armed.swap(false, Ordering::AcqRel)
    {
        return Err(SessionReplyMismatch::RefusalForOperator.into());
    }
    result
}
