//! The in-process contract between the ACP harness and Mac approval custody.

mod approval_messages;
mod approval_scope;

pub use approval_messages::{
    ApprovalAsk, ApprovalCustodyPort, ApprovalHarnessPort, ApprovalReply, CustodyApprovalValues,
    HarnessApprovalValues, LocalApprovalRefusal, MacWaitCause, MacWaitEnded, ReplyHandover,
    ReplyHandoverKind, ReplyInstruction, ReplyKind, UncarriableAsk, UncarriableShape,
    WaitingRequestRelease, approval_value_builders,
};
pub use approval_scope::{ApprovalScope, ApprovalTurnId, InvalidApprovalTurnId, WaitingRequestId};

use workspace_ids::{CentralSessionId, SessionId};

/// The session record implements this read and supplies the existing central identity, if known.
pub trait CentralSessionLookup {
    /// Reads the local session record without a central request.
    fn central_session_id(&self, local_session: &SessionId) -> Option<CentralSessionId>;
}
