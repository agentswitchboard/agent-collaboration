//! Approval arguments and results exposed by the person link.

pub use approval_request_protocol::{
    Answer, ApprovalDecideReply, ApprovalInvalidReason, ApprovalListFilter, ApprovalListReply,
    ApprovalReadReply, ApprovalRequestId, ApprovalSummaryEnding, ApprovalSummaryState, DecidedBy,
    InvalidAnswer, OfferedOptionId, OptionEffect, OptionLifetime, RequestKind, RequestSummary,
    RequestView, RetainedOptionStorage, SessionCallDecision, SessionCallOperation,
    TransientOptionStorage,
};
pub use central_link_protocol::LinkCallRefusal;
pub use workspace_ids::RequestId;

pub use super::person_link_client::{ApprovalDecisionOutcome, ApprovalNotSent};
