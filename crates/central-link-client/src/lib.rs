//! Event-driven client policy. The Host owns socket I/O and drives the injected-clock deadline.
mod error;
mod link_state;
mod pending_calls;
mod ready_lane;

pub use link_state::{
    AttemptOutcome, ClientAction, ClientEvent, ConnectionAttempt, EnrollmentEnd, LinkClient,
    LinkFault, LinkStatus,
};
pub use pending_calls::{CallFailure, FailedCall, SendFailure};
pub use ready_lane::{
    AdmittedConnection, BatchResultsError, CheckedBatchMember, CheckedBatchResults,
    CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, InvalidDeliveryOffer, LINK_LANE_FRAME_BYTES, LaneFrameSizeError,
    LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane, RegisteredConnection, RequestStatusCommand,
    SubmitReceiptsCommand, UntouchedCommand, ValidatedDeliveryOffer, WRITE_BATCH_ENVELOPE_BYTES,
    WelcomeClockAnchor, WriteBatchCommand, WriteBatchDisposition, WriteBatchMemberDisposition,
    encoded_request_size,
};

pub use error::ClientError;
