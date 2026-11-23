//! The in-process contract between the ACP harness and Mac approval custody.

mod approval_messages;
mod approval_scope;

pub use approval_messages::{
    ApprovalAsk, ApprovalCustodyPort, ApprovalHarnessPort, ApprovalReply, CustodyApprovalValues,
    HarnessApprovalValues, LocalApprovalRefusal, MacWaitCause, MacWaitEnded, ReplyHandoverKind,
    ReplyKind, ReplySubmissionKind, ReplySubmissionOutcome, ReplyableStopCause,
    StartedReplySubmission, TerminalRaiseRefusal, UncarriableAsk, UncarriableShape,
    UnstartedWaitRelease, WaitBindingRevocation, WaitReleaseKind, WaitReleaseOutcome,
    approval_value_builders,
};
pub use approval_scope::{
    ApprovalScope, ApprovalTurnId, InvalidApprovalTurnId, LocalSessionBindingId, WaitingRequestId,
};
