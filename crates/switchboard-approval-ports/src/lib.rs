//! The in-process contract between the ACP harness and Mac approval custody.

mod approval_messages;
mod approval_scope;

pub use approval_messages::{
    ApprovalAsk, ApprovalCustodyPort, ApprovalHarnessPort, ApprovalReply, CustodyApprovalValues,
    HarnessApprovalValues, LocalApprovalRefusal, MacWaitCause, MacWaitEnded, ReplyHandover,
    ReplyHandoverKind, ReplyInstruction, ReplyKind, TerminalRaiseRefusal, UncarriableAsk,
    UncarriableShape, WaitReleaseKind, WaitReleaseOutcome, WaitingRequestRelease,
    approval_value_builders,
};
pub use approval_scope::{ApprovalScope, ApprovalTurnId, InvalidApprovalTurnId, WaitingRequestId};

use workspace_ids::{CentralSessionId, SessionId};

/// The session record's last known opened association or a lasting fence on it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenedSessionIdentity {
    NotKnownToCentral,
    Opened {
        central_session_id: CentralSessionId,
    },
    KnownButFenced {
        reason: OpenedSessionFence,
    },
}

/// Why the session record cannot make a known opened association available to callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenedSessionFence {
    SettledByCentral,
    EndInProgress,
    EnrollmentEnded,
}

/// The local session record implements this read of its opened central associations and fences.
///
/// ```compile_fail,E0308
/// use switchboard_approval_ports::OpenedSessionIdentityLookup;
/// use workspace_ids::{CentralSessionId, SessionId};
/// fn wrong_return(read: &impl OpenedSessionIdentityLookup, id: &SessionId) -> Option<CentralSessionId> {
///     read.opened_session_identity(id)
/// }
/// ```
pub trait OpenedSessionIdentityLookup {
    /// Reads the existing association and lasting fence independently of link readiness.
    fn opened_session_identity(&self, local_session: &SessionId) -> OpenedSessionIdentity;
}
