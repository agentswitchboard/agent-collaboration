//! The exact close mark that temporarily stops a run from taking work.

#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_harness_contracts::CloseAttempt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteCloseState {
    Open,
    Closing { attempt: CloseAttempt },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteCloseEvent {
    CloseSent { attempt: CloseAttempt },
    Readmit { attempt: CloseAttempt },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteCloseRefusal {
    AlreadyClosing,
    DifferentMarker,
    MarkerAbsent,
}

impl StateTag for RouteCloseState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closing { .. } => "closing",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

impl EventTag for RouteCloseEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::CloseSent { .. } => "close_sent",
            Self::Readmit { .. } => "readmit",
        }
    }
}

impl RefusalTag for RouteCloseRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::AlreadyClosing => "already_closing",
            Self::DifferentMarker => "different_marker",
            Self::MarkerAbsent => "marker_absent",
        }
    }
}

pub struct RouteCloseMachine;

impl StateMachine for RouteCloseMachine {
    type State = RouteCloseState;
    type Event = RouteCloseEvent;
    type Effect = ();
    type Error = RouteCloseRefusal;

    fn transition(state: &RouteCloseState, event: RouteCloseEvent) -> StateMachineResult<Self> {
        use RouteCloseEvent::{CloseSent, Readmit};
        use RouteCloseRefusal::{AlreadyClosing, DifferentMarker, MarkerAbsent};
        use RouteCloseState::{Closing, Open};

        let next = match (*state, event) {
            (Open, CloseSent { attempt }) => Closing { attempt },
            (Closing { attempt: marked }, Readmit { attempt }) if marked == attempt => Open,
            (Closing { .. }, CloseSent { .. }) => {
                return Err(TransitionError::Domain(AlreadyClosing));
            }
            (Closing { .. }, Readmit { .. }) => {
                return Err(TransitionError::Domain(DifferentMarker));
            }
            (Open, Readmit { .. }) => return Err(TransitionError::Domain(MarkerAbsent)),
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        &[
            Transition {
                from: "open",
                event: "close_sent",
                to: "closing",
                terminal: false,
            },
            Transition {
                from: "closing",
                event: "readmit",
                to: "open",
                terminal: false,
            },
        ]
    }
}

#[cfg(test)]
#[path = "route_close_machine_test.rs"]
mod tests;
