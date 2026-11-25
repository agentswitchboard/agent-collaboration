//! Decisions and observer receipts for one enrollment worker's lifetime.
#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_machine_core_store::MachineLocalStoreError;

/// The cause of termination observed by the enrollment store for its worker task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrollmentWorkerExit {
    /// The worker stopped its request loop in response to `request_close`.
    Requested,
    /// The worker ended without completing a requested stop, including on unwind.
    Autonomous,
}

/// A physical join receipt published after the worker and its blocking tasks
/// end and SQLite acknowledges the connection close.
#[derive(Clone, Debug)]
pub struct EnrollmentJoin {
    pub outcome: Result<(), EnrollmentStoreCloseError>,
}

/// Why the enrollment store could not provide the requested lifecycle witness.
/// Every `join()` error leaves ownership unconfirmed. Keep the store outstanding
/// and withhold replacement until physical termination is established.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentObservationError {
    /// The publisher ended before recording the requested witness.
    #[error("the enrollment reaper stopped before publishing the requested witness")]
    ReaperLost,
    /// SQLite close returned an error before confirming connection release.
    #[error("the enrollment reaper could not confirm the database connection closed")]
    ConnectionCloseUnconfirmed { exit: EnrollmentWorkerExit },
}

/// A failure reported by the enrollment store when reaping its worker and connection.
#[derive(Clone, Debug, thiserror::Error)]
pub enum EnrollmentStoreCloseError {
    #[error("the enrollment worker stopped before closing its connection")]
    WorkerStopped,
    #[error("could not close the enrollment database connection")]
    Connection(#[source] Arc<MachineLocalStoreError>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WorkerAdmission {
    Open,
    Closing,
}

impl WorkerAdmission {
    pub(super) fn allows_request(self) -> bool {
        self == Self::Open
    }

    pub(super) fn loop_exit(self) -> EnrollmentWorkerExit {
        match self {
            Self::Open => EnrollmentWorkerExit::Autonomous,
            Self::Closing => EnrollmentWorkerExit::Requested,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum WorkerTaskEnd {
    Returned(EnrollmentWorkerExit),
    Failed,
}

#[derive(Clone, Copy, Debug)]
enum WorkerTaskOutcome {
    Returned,
    Failed,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ExitedWorker {
    cause: EnrollmentWorkerExit,
    outcome: WorkerTaskOutcome,
}

pub(super) type ConnectionClose = Result<(), Arc<MachineLocalStoreError>>;

#[derive(Clone, Debug)]
pub(super) struct WorkerReport {
    exit: ExitedWorker,
    connection: ConnectionClose,
}

impl WorkerReport {
    pub(super) fn join(&self) -> Result<EnrollmentJoin, EnrollmentObservationError> {
        match self.connection {
            Ok(()) => Ok(EnrollmentJoin {
                outcome: self.close_outcome(),
            }),
            Err(_) => Err(EnrollmentObservationError::ConnectionCloseUnconfirmed {
                exit: self.exit.cause,
            }),
        }
    }

    pub(super) fn close_outcome(&self) -> Result<(), EnrollmentStoreCloseError> {
        match self.exit.outcome {
            WorkerTaskOutcome::Returned => self
                .connection
                .clone()
                .map_err(EnrollmentStoreCloseError::Connection),
            WorkerTaskOutcome::Failed => Err(EnrollmentStoreCloseError::WorkerStopped),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum WorkerProgress {
    Exited(ExitedWorker),
    Reported(WorkerReport),
}

impl WorkerProgress {
    pub(super) fn exit(&self) -> EnrollmentWorkerExit {
        match self {
            Self::Exited(exit) | Self::Reported(WorkerReport { exit, .. }) => exit.cause,
        }
    }

    pub(super) fn report(&self) -> Option<&WorkerReport> {
        match self {
            Self::Exited(_) => None,
            Self::Reported(report) => Some(report),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum WorkerState {
    Running(WorkerAdmission),
    Progress(WorkerProgress),
}

impl StateTag for WorkerState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Running(WorkerAdmission::Open) => "runningOpen",
            Self::Running(WorkerAdmission::Closing) => "runningClosing",
            Self::Progress(WorkerProgress::Exited(_)) => "exited",
            Self::Progress(WorkerProgress::Reported(report)) => match report.connection {
                Ok(()) => "reportedJoined",
                Err(_) => "reportedCloseUnconfirmed",
            },
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Progress(WorkerProgress::Reported(_)))
    }
}

#[derive(Clone)]
pub(super) enum WorkerEvent {
    RequestClose,
    Enqueue,
    BeginRequest,
    TaskExited(WorkerTaskEnd),
    SqliteCloseFinished(ConnectionClose),
}

impl EventTag for WorkerEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::RequestClose => "requestClose",
            Self::Enqueue => "enqueue",
            Self::BeginRequest => "beginRequest",
            Self::TaskExited(_) => "taskExited",
            Self::SqliteCloseFinished(Ok(())) => "sqliteCloseAcknowledged",
            Self::SqliteCloseFinished(Err(_)) => "sqliteCloseUnconfirmed",
        }
    }
}

#[derive(Debug)]
pub(super) enum WorkerTransitionRefusal {}

impl RefusalTag for WorkerTransitionRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

pub(super) struct EnrollmentWorkerMachine;

impl WorkerTaskEnd {
    fn exited(self) -> ExitedWorker {
        let (cause, outcome) = match self {
            WorkerTaskEnd::Returned(cause) => (cause, WorkerTaskOutcome::Returned),
            WorkerTaskEnd::Failed => (EnrollmentWorkerExit::Autonomous, WorkerTaskOutcome::Failed),
        };
        ExitedWorker { cause, outcome }
    }
}

impl StateMachine for EnrollmentWorkerMachine {
    type State = WorkerState;
    type Event = WorkerEvent;
    type Effect = ();
    type Error = WorkerTransitionRefusal;

    fn transition(state: &WorkerState, event: WorkerEvent) -> StateMachineResult<Self> {
        use WorkerEvent as Event;
        use WorkerProgress as Progress;
        use WorkerState as State;

        let event_tag = event.event_tag();
        let next = match (state, event) {
            (State::Running(_), Event::RequestClose) => State::Running(WorkerAdmission::Closing),
            (State::Running(admission), Event::Enqueue | Event::BeginRequest)
                if admission.allows_request() =>
            {
                State::Running(*admission)
            }
            (State::Running(_), Event::TaskExited(end)) => {
                State::Progress(Progress::Exited(end.exited()))
            }
            (State::Progress(Progress::Exited(exit)), Event::SqliteCloseFinished(connection)) => {
                State::Progress(Progress::Reported(WorkerReport {
                    exit: *exit,
                    connection,
                }))
            }
            (
                State::Running(_),
                Event::Enqueue | Event::BeginRequest | Event::SqliteCloseFinished(_),
            )
            | (
                State::Progress(Progress::Exited(_)),
                Event::RequestClose | Event::Enqueue | Event::BeginRequest | Event::TaskExited(_),
            )
            | (
                State::Progress(Progress::Reported(_)),
                Event::RequestClose
                | Event::Enqueue
                | Event::BeginRequest
                | Event::TaskExited(_)
                | Event::SqliteCloseFinished(_),
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event_tag,
                });
            }
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        &[
            Transition {
                from: "runningOpen",
                event: "requestClose",
                to: "runningClosing",
                terminal: false,
            },
            Transition {
                from: "runningClosing",
                event: "requestClose",
                to: "runningClosing",
                terminal: false,
            },
            Transition {
                from: "runningOpen",
                event: "enqueue",
                to: "runningOpen",
                terminal: false,
            },
            Transition {
                from: "runningOpen",
                event: "beginRequest",
                to: "runningOpen",
                terminal: false,
            },
            Transition {
                from: "runningOpen",
                event: "taskExited",
                to: "exited",
                terminal: false,
            },
            Transition {
                from: "runningClosing",
                event: "taskExited",
                to: "exited",
                terminal: false,
            },
            Transition {
                from: "exited",
                event: "sqliteCloseAcknowledged",
                to: "reportedJoined",
                terminal: true,
            },
            Transition {
                from: "exited",
                event: "sqliteCloseUnconfirmed",
                to: "reportedCloseUnconfirmed",
                terminal: true,
            },
        ]
    }
}

#[cfg(test)]
#[path = "enrollment_worker_machine_test.rs"]
mod tests;
