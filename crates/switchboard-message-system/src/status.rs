use serde::{Deserialize, Serialize};
use switchboard_central_link::link_policy::LinkStatus;

use switchboard_central_link::link_policy::RegistrationFailure;
use switchboard_machine_enrollment::EnrollmentEnd;

use crate::{FrontRefusal, MessageSystemReason, ObservationPhase, State};

/// Live message-system status returned through the Host's operator interface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageSystemStatus {
    Starting,
    NotEnrolled,
    EnrollmentEnded,
    EnrollmentUnreadable,
    Connecting,
    Registering,
    RegistrationFailed,
    RunRetired,
    Connected,
    Fault,
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
            LinkStatus::Connected { .. } => Status::Connected,
            LinkStatus::Welcomed { .. } | LinkStatus::Registering => Status::Registering,
            LinkStatus::RegistrationFault { .. } => Status::Fault,
            LinkStatus::RegistrationFailed { .. } => Status::RegistrationFailed,
            LinkStatus::RunRetired => Status::RunRetired,
            LinkStatus::Fault { .. } => Status::Fault,
            LinkStatus::EnrollmentEnded { .. } => Status::EnrollmentEnded,
            LinkStatus::NotEnrolled
            | LinkStatus::Connecting
            | LinkStatus::DiscoveryPending
            | LinkStatus::DiscoveryUnavailable => Status::Connecting,
        },
        State::Quiescing(_) => Status::Stopping,
        State::Faulted { .. } => Status::EnrollmentMismatch,
        State::ClosedWithOutstanding { .. } | State::Closed => Status::Stopped,
    }
}

/// One atomic status publication and the reason reported by this generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageSystemObservation {
    pub status: MessageSystemStatus,
    pub reason: MessageSystemReason,
}

impl MessageSystemObservation {
    pub(crate) fn project(state: &State, link: &LinkStatus, ending: Option<EnrollmentEnd>) -> Self {
        use MessageSystemReason as Reason;
        use MessageSystemStatus as Status;

        let status = message_system_status(state, link);
        let reason = match (status, link) {
            (Status::EnrollmentEnded, _) => {
                ending.map_or(Reason::EnrollmentEndingPending, Reason::stored_ending)
            }
            (Status::Stopping, _)
                if matches!(
                    state.front(),
                    crate::Front::Refusing(FrontRefusal::EnrollmentEnded)
                ) =>
            {
                ending.map_or(Reason::EnrollmentEndingPending, Reason::stored_ending)
            }
            (Status::RunRetired, _) => Reason::AnotherRunTookOver,
            (Status::RegistrationFailed, LinkStatus::RegistrationFailed { reason }) => {
                Reason::registration(*reason)
            }
            (Status::Fault, LinkStatus::RegistrationFault { reason }) => {
                Reason::registration(RegistrationFailure::Refused(*reason))
            }
            (Status::Fault, LinkStatus::Fault { kind }) => Reason::link_fault(*kind),
            (Status::Connecting, LinkStatus::DiscoveryPending) => Reason::DiscoveryPending,
            (Status::Connecting, LinkStatus::DiscoveryUnavailable) => Reason::DiscoveryUnavailable,
            _ => Reason::NoReason,
        };
        Self { status, reason }
    }
}

#[cfg(all(feature = "test-support", debug_assertions))]
mod publication_fixture {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use tokio::sync::watch;

    use super::MessageSystemObservation;

    /// Holds the real operator publication while the Interpreter keeps applying events.
    #[derive(Clone, Default)]
    pub struct StatusPublicationHold {
        frozen: Arc<AtomicBool>,
        held: watch::Sender<Option<MessageSystemObservation>>,
    }

    impl StatusPublicationHold {
        pub fn freeze(&self) {
            self.frozen.store(true, Ordering::Relaxed);
            self.held.send_replace(None);
        }

        pub fn release(&self) {
            self.frozen.store(false, Ordering::Relaxed);
        }

        pub fn held_observations(&self) -> watch::Receiver<Option<MessageSystemObservation>> {
            self.held.subscribe()
        }

        pub(crate) fn withhold(&self, observation: MessageSystemObservation) -> bool {
            let frozen = self.frozen.load(Ordering::Relaxed);
            if frozen {
                self.held.send_replace(Some(observation));
            }
            frozen
        }
    }
}

#[cfg(all(feature = "test-support", debug_assertions))]
pub use publication_fixture::StatusPublicationHold;

#[cfg(test)]
#[path = "status_test.rs"]
mod tests;
