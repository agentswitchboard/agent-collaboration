//! Event-driven client policy. The Host owns socket I/O and drives the injected-clock deadline.
mod error;
mod link_state;
mod pending_calls;
mod ready_lane;
mod reply_matcher;
mod workspace_miss;

pub use reply_matcher::ReplyExpectation;

pub use link_state::{
    AttemptOutcome, ClientAction, ClientEvent, ConnectionAttempt, EnrollmentEnd, LinkClient,
    LinkFault, LinkStatus,
};
pub use pending_calls::{CallFailure, FailedCall, SendFailure};
pub use ready_lane::{
    AdmittedConnection, BatchResultsError, CheckedBatchMember, CheckedBatchResults,
    CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, InvalidDeliveryOffer, LINK_LANE_FRAME_BYTES,
    LINK_LANE_SETTLEMENT_LIMIT, LaneFrameSizeError, LinkLaneDisposition, LinkLaneEvent,
    LinkLocalFault, PendingRunRegistration, ReadyLinkLane, RegisteredConnection,
    RequestStatusCommand, RunRegistrationDecision, RunRegistrationEvidence, SubmitReceiptsCommand,
    UntouchedCommand, ValidatedDeliveryOffer, WRITE_BATCH_ENVELOPE_BYTES, WelcomeClockAnchor,
    WriteBatchCommand, WriteBatchDisposition, WriteBatchMemberDisposition, encoded_request_size,
    link_frame_answer_deadline, link_frame_write_deadline,
};

pub use error::ClientError;
