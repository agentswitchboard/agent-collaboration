//! Declared transitions for call admission and the evidence retained by one accepted call.
#![deny(clippy::wildcard_enum_match_arm)]

use central_link_client::{ConnectionAttempt, EnrollmentEnd};
use central_link_protocol::{LinkCallRefusal, MacWorkReply, MachineLinkCloseCode};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

/// The end observed by the connection that owned an accepted call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallEnding {
    Deadline,
    InvalidFrame,
    WriterWindowElapsed,
    CarrierClosed(Option<MachineLinkCloseCode>),
    TransportLost,
    EnrollmentEnded(EnrollmentEnd),
    DeliveryConsumerStalled,
    SettingsChanged,
    Shutdown,
    DriverDropped,
    CallerCancelled,
}

/// Reasons that establish that none of this call's bytes crossed the writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallNotReached {
    LaneFull,
    InvalidFrame,
    Ended(CallEnding),
}

#[derive(Debug)]
pub enum CallSettlement {
    Answered(Box<MacWorkReply>),
    RefusedByCentral(LinkCallRefusal),
    NotReached(CallNotReached),
    AnswerLost(CallEnding),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallPhase {
    Queued,
    Writing,
    Written,
    Settled,
}

impl StateTag for CallPhase {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Writing => "writing",
            Self::Written => "written",
            Self::Settled => "settled",
        }
    }

    fn is_terminal(&self) -> bool {
        *self == Self::Settled
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameExposure {
    Unwritten,
    PossiblyWritten,
}

#[derive(Clone)]
pub enum CallEvent {
    BeginWrite,
    FlushCompleted,
    End(CallEnding),
    WriteEnded {
        exposure: FrameExposure,
        cause: CallEnding,
    },
    Answered(Box<MacWorkReply>),
    RefusedByCentral(LinkCallRefusal),
}

impl EventTag for CallEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::BeginWrite => "begin_write",
            Self::FlushCompleted => "flush_completed",
            Self::End(_) => "end",
            Self::WriteEnded { .. } => "write_ended",
            Self::Answered(_) => "answered",
            Self::RefusedByCentral(_) => "refused_by_central",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallPhaseRefusal {
    WrongPhase,
}

impl RefusalTag for CallPhaseRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::WrongPhase => "wrong_phase",
        }
    }
}

/// The receive path filters replies that do not fit before offering an event here.
pub struct CallPolicy;

impl StateMachine for CallPolicy {
    type State = CallPhase;
    type Event = CallEvent;
    type Effect = CallSettlement;
    type Error = CallPhaseRefusal;

    fn transition(state: &CallPhase, event: CallEvent) -> StateMachineResult<Self> {
        use CallEvent as Event;
        use CallPhase as Phase;
        use CallSettlement as Settlement;

        let ending = match (*state, event) {
            (Phase::Queued, Event::BeginWrite) => return Ok((Phase::Writing, vec![])),
            (Phase::Writing, Event::FlushCompleted) => return Ok((Phase::Written, vec![])),
            (Phase::Queued, Event::End(cause)) => {
                Settlement::NotReached(CallNotReached::Ended(cause))
            }
            (Phase::Written, Event::End(cause)) => Settlement::AnswerLost(cause),
            (Phase::Writing, Event::WriteEnded { exposure, cause }) => match exposure {
                FrameExposure::Unwritten if cause == CallEnding::InvalidFrame => {
                    Settlement::NotReached(CallNotReached::InvalidFrame)
                }
                FrameExposure::Unwritten => Settlement::NotReached(CallNotReached::Ended(cause)),
                FrameExposure::PossiblyWritten => Settlement::AnswerLost(cause),
            },
            (Phase::Written, Event::Answered(reply)) => Settlement::Answered(reply),
            (Phase::Written, Event::RefusedByCentral(reason)) => {
                Settlement::RefusedByCentral(reason)
            }
            (Phase::Settled, _) => return Err(TransitionError::Stale),
            (
                Phase::Queued,
                Event::FlushCompleted
                | Event::WriteEnded { .. }
                | Event::Answered(_)
                | Event::RefusedByCentral(_),
            )
            | (
                Phase::Writing,
                Event::BeginWrite | Event::End(_) | Event::Answered(_) | Event::RefusedByCentral(_),
            )
            | (
                Phase::Written,
                Event::BeginWrite | Event::FlushCompleted | Event::WriteEnded { .. },
            ) => {
                return Err(TransitionError::Domain(CallPhaseRefusal::WrongPhase));
            }
        };
        Ok((Phase::Settled, vec![ending]))
    }

    fn transitions() -> &'static [Transition] {
        const EDGES: &[Transition] = &[
            edge("queued", "begin_write", "writing", false),
            edge("writing", "flush_completed", "written", false),
            edge("queued", "end", "settled", true),
            edge("written", "end", "settled", true),
            edge("writing", "write_ended", "settled", true),
            edge("written", "answered", "settled", true),
            edge("written", "refused_by_central", "settled", true),
        ];
        EDGES
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum CallGate {
    #[default]
    Unavailable,
    Open(ConnectionAttempt),
    EnrollmentEnded,
}

impl StateTag for CallGate {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Open(_) => "open",
            Self::EnrollmentEnded => "enrollment_ended",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub enum CallGateEvent {
    Registered(ConnectionAttempt),
    Withdraw(ConnectionAttempt),
    EndEnrollment,
    NewBinding,
}

impl EventTag for CallGateEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Registered(_) => "registered",
            Self::Withdraw(_) => "withdraw",
            Self::EndEnrollment => "end_enrollment",
            Self::NewBinding => "new_binding",
        }
    }
}

pub struct CallGatePolicy;

impl StateMachine for CallGatePolicy {
    type State = CallGate;
    type Event = CallGateEvent;
    type Effect = ();
    type Error = CallPhaseRefusal;

    fn transition(state: &CallGate, event: CallGateEvent) -> StateMachineResult<Self> {
        use CallGate as Gate;
        use CallGateEvent as Event;

        let next = match (state, event) {
            (Gate::Unavailable, Event::Registered(attempt)) => Gate::Open(attempt),
            (Gate::Open(current), Event::Withdraw(attempt)) if current == &attempt => {
                Gate::Unavailable
            }
            (Gate::Unavailable | Gate::Open(_), Event::EndEnrollment) => Gate::EnrollmentEnded,
            (Gate::EnrollmentEnded, Event::NewBinding) => Gate::Unavailable,
            (Gate::Unavailable | Gate::Open(_) | Gate::EnrollmentEnded, Event::Withdraw(_)) => {
                return Err(TransitionError::Stale);
            }
            (Gate::Open(_) | Gate::EnrollmentEnded, Event::Registered(_))
            | (Gate::Unavailable | Gate::Open(_), Event::NewBinding)
            | (Gate::EnrollmentEnded, Event::EndEnrollment) => {
                return Err(TransitionError::Domain(CallPhaseRefusal::WrongPhase));
            }
        };
        Ok((next, vec![]))
    }

    fn transitions() -> &'static [Transition] {
        const EDGES: &[Transition] = &[
            edge("unavailable", "registered", "open", false),
            edge("open", "withdraw", "unavailable", false),
            edge("unavailable", "end_enrollment", "enrollment_ended", false),
            edge("open", "end_enrollment", "enrollment_ended", false),
            edge("enrollment_ended", "new_binding", "unavailable", false),
        ];
        EDGES
    }
}

const fn edge(
    from: &'static str,
    event: &'static str,
    to: &'static str,
    terminal: bool,
) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal,
    }
}

#[cfg(test)]
#[path = "central_call_policy_test.rs"]
mod tests;
