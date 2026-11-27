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

impl RouteCloseMachine {
    pub(super) fn reduce(
        state: RouteCloseState,
        event: RouteCloseEvent,
    ) -> Result<RouteCloseState, RouteCloseRefusal> {
        use RouteCloseEvent::{CloseSent, Readmit};
        use RouteCloseRefusal::{AlreadyClosing, DifferentMarker, MarkerAbsent};
        use RouteCloseState::{Closing, Open};

        match (state, event) {
            (Open, CloseSent { attempt }) => Ok(Closing { attempt }),
            (Closing { attempt: marked }, Readmit { attempt }) if marked == attempt => Ok(Open),
            (Closing { .. }, CloseSent { .. }) => Err(AlreadyClosing),
            (Closing { .. }, Readmit { .. }) => Err(DifferentMarker),
            (Open, Readmit { .. }) => Err(MarkerAbsent),
        }
    }
}

impl StateMachine for RouteCloseMachine {
    type State = RouteCloseState;
    type Event = RouteCloseEvent;
    type Effect = ();
    type Error = RouteCloseRefusal;

    fn transition(state: &RouteCloseState, event: RouteCloseEvent) -> StateMachineResult<Self> {
        Self::reduce(*state, event)
            .map(|next| (next, Vec::new()))
            .map_err(TransitionError::Domain)
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
