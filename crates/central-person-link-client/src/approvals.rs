//! Approval arguments and results exposed by the person link.

pub use approval_request_protocol::{
    Answer, ApprovalDecideReply, ApprovalInvalidReason, ApprovalListFilter, ApprovalListReply,
    ApprovalReadReply, ApprovalRequestId, ApprovalSummaryEnding, ApprovalSummaryState, DecidedBy,
    InvalidAnswer, OfferedOptionId, RequestKind, RequestSummary, RequestView, SessionCallDecision,
    SessionCallOperation,
};
pub use workspace_ids::RequestId;

pub use super::person_link_client::{ApprovalDecisionOutcome, ApprovalNotSent};
