use serde::{Deserialize, Serialize};
use switchboard_central_link::link_policy::LinkStatus;

use crate::{FrontRefusal, ObservationPhase, State};

/// Live message-system status returned through the Host's operator interface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageSystemStatus {
    Starting,
    NotEnrolled,
    EnrollmentEnded,
    EnrollmentUnreadable,
    Connecting,
    Connected,
    Stopping,
    Restarting,
    EnrollmentMismatch,
    Stopped,
}

/// Projects the Supervisor's state and the current link's observation for status.
#[must_use]
pub fn message_system_status(state: &State, link: &LinkStatus) -> MessageSystemStatus {
    use MessageSystemStatus as Status;

    match state {
        State::Observing { phase } => match phase {
            ObservationPhase::RestartCooldown { .. } => Status::Restarting,
            ObservationPhase::Polling(reason) => match reason {
                FrontRefusal::AwaitingFirstRead => Status::Starting,
                FrontRefusal::NotEnrolled => Status::NotEnrolled,
                FrontRefusal::EnrollmentEnded => Status::EnrollmentEnded,
                FrontRefusal::MetadataUnreadable | FrontRefusal::CouldNotStart => {
                    Status::EnrollmentUnreadable
                }
                FrontRefusal::Starting => Status::Connecting,
                FrontRefusal::Restarting => Status::Restarting,
                FrontRefusal::EnrollmentMismatch => Status::EnrollmentMismatch,
                FrontRefusal::ShuttingDown => Status::Stopping,
            },
        },
        State::Opening { .. } => Status::Connecting,
        State::Running { .. } => match link {
            LinkStatus::Welcomed { .. } | LinkStatus::Connected { .. } => Status::Connected,
            LinkStatus::NotEnrolled
            | LinkStatus::Connecting
            | LinkStatus::Fault { .. }
            | LinkStatus::EnrollmentEnded { .. } => Status::Connecting,
        },
        State::Quiescing(_) => Status::Stopping,
        State::Faulted { .. } => Status::EnrollmentMismatch,
        State::ClosedWithOutstanding { .. } | State::Closed => Status::Stopped,
    }
}

#[cfg(test)]
#[path = "status_test.rs"]
mod tests;
