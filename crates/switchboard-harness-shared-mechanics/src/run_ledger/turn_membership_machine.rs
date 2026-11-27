//! The turn that may own callbacks within one run.

#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_harness_contracts::TurnId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnMembershipState {
    Loading,
    Idle,
    Provisional { turn: TurnId },
    Eligible { turn: TurnId },
    ProviderStarted { turn: TurnId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnMembershipEvent {
    Activate,
    Begin { turn: TurnId },
    ObserveEligible { turn: TurnId },
    TakeNew { turn: TurnId },
    RemoveOnRevoke { turn: TurnId },
    End { turn: TurnId },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnMembershipRefusal {
    AlreadyLive,
    NotLive,
    TurnAlreadySet,
    NotThatTurn,
}

impl StateTag for TurnMembershipState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Loading => "loading",
            Self::Idle => "idle",
            Self::Provisional { .. } => "provisional",
            Self::Eligible { .. } => "eligible",
            Self::ProviderStarted { .. } => "provider_started",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

impl EventTag for TurnMembershipEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Activate => "activate",
            Self::Begin { .. } => "begin",
            Self::ObserveEligible { .. } => "observe_eligible",
            Self::TakeNew { .. } => "take_new",
            Self::RemoveOnRevoke { .. } => "remove_on_revoke",
            Self::End { .. } => "end",
        }
    }
}

impl RefusalTag for TurnMembershipRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::AlreadyLive => "already_live",
            Self::NotLive => "not_live",
            Self::TurnAlreadySet => "turn_already_set",
            Self::NotThatTurn => "not_that_turn",
        }
    }
}

const fn edge(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

const TRANSITIONS: &[Transition] = &[
    edge("loading", "activate", "idle"),
    edge("idle", "begin", "provisional"),
    edge("idle", "observe_eligible", "provider_started"),
    edge("provisional", "take_new", "eligible"),
    edge("provisional", "remove_on_revoke", "idle"),
    edge("eligible", "end", "idle"),
    edge("provider_started", "end", "idle"),
];

pub struct TurnMembershipMachine;

impl StateMachine for TurnMembershipMachine {
    type State = TurnMembershipState;
    type Event = TurnMembershipEvent;
    type Effect = ();
    type Error = TurnMembershipRefusal;

    fn transition(
        state: &TurnMembershipState,
        event: TurnMembershipEvent,
    ) -> StateMachineResult<Self> {
        use TurnMembershipEvent as Event;
        use TurnMembershipRefusal as Refusal;
        use TurnMembershipState as State;

        let next = match (*state, event) {
            (State::Loading, Event::Activate) => State::Idle,
            (State::Idle, Event::Begin { turn }) => State::Provisional { turn },
            (State::Idle, Event::ObserveEligible { turn }) => State::ProviderStarted { turn },
            (State::Provisional { turn: saved }, Event::TakeNew { turn }) if turn == saved => {
                State::Eligible { turn }
            }
            (State::Provisional { turn: saved }, Event::RemoveOnRevoke { turn })
                if turn == saved =>
            {
                State::Idle
            }
            (
                State::Eligible { turn: saved } | State::ProviderStarted { turn: saved },
                Event::End { turn },
            ) if turn == saved => State::Idle,
            (
                State::Idle
                | State::Provisional { .. }
                | State::Eligible { .. }
                | State::ProviderStarted { .. },
                Event::Activate,
            ) => return Err(TransitionError::Domain(Refusal::AlreadyLive)),
            (State::Loading, Event::Begin { .. } | Event::ObserveEligible { .. }) => {
                return Err(TransitionError::Domain(Refusal::NotLive));
            }
            (
                State::Provisional { .. } | State::Eligible { .. } | State::ProviderStarted { .. },
                Event::Begin { .. } | Event::ObserveEligible { .. },
            ) => return Err(TransitionError::Domain(Refusal::TurnAlreadySet)),
            (
                State::Loading
                | State::Idle
                | State::Provisional { .. }
                | State::Eligible { .. }
                | State::ProviderStarted { .. },
                Event::TakeNew { .. } | Event::RemoveOnRevoke { .. } | Event::End { .. },
            ) => return Err(TransitionError::Domain(Refusal::NotThatTurn)),
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

#[cfg(test)]
#[path = "turn_membership_machine_test.rs"]
mod tests;
