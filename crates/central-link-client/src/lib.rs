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
    AdmittedConnection, CorrelatedRequestReply, LinkLaneCommand, LinkLaneDisposition,
    LinkLaneEvent, LinkLaneReply, ReadyLinkLane, RegisteredConnection, WelcomeClockAnchor,
    WriteBatchCommand, WriteBatchDisposition, WriteBatchMemberDisposition,
};

pub use error::ClientError;
