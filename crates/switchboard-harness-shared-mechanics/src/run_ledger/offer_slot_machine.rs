//! Custody of one authorization's writer slot.

#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferSlotState {
    Offered,
    Taken,
    Revoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferSlotEvent {
    HandOver,
    Revoke,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OfferSlotRefusal {
    AlreadyTaken,
    AlreadyRevoked,
}

impl StateTag for OfferSlotState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Offered => "offered",
            Self::Taken => "taken",
            Self::Revoked => "revoked",
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Offered => false,
            Self::Taken | Self::Revoked => true,
        }
    }
}

impl EventTag for OfferSlotEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::HandOver => "hand_over",
            Self::Revoke => "revoke",
        }
    }
}

impl RefusalTag for OfferSlotRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::AlreadyTaken => "already_taken",
            Self::AlreadyRevoked => "already_revoked",
        }
    }
}

pub struct OfferSlotMachine;

impl OfferSlotMachine {
    pub(super) fn reduce(
        state: OfferSlotState,
        event: OfferSlotEvent,
    ) -> Result<OfferSlotState, OfferSlotRefusal> {
        use OfferSlotEvent::{HandOver, Revoke};
        use OfferSlotRefusal::{AlreadyRevoked, AlreadyTaken};
        use OfferSlotState::{Offered, Revoked, Taken};

        match (state, event) {
            (Offered, HandOver) => Ok(Taken),
            (Offered, Revoke) => Ok(Revoked),
            (Taken, HandOver | Revoke) => Err(AlreadyTaken),
            (Revoked, HandOver | Revoke) => Err(AlreadyRevoked),
        }
    }
}

impl StateMachine for OfferSlotMachine {
    type State = OfferSlotState;
    type Event = OfferSlotEvent;
    type Effect = ();
    type Error = OfferSlotRefusal;

    fn transition(state: &OfferSlotState, event: OfferSlotEvent) -> StateMachineResult<Self> {
        Self::reduce(*state, event)
            .map(|next| (next, Vec::new()))
            .map_err(TransitionError::Domain)
    }

    fn transitions() -> &'static [Transition] {
        &[
            Transition {
                from: "offered",
                event: "hand_over",
                to: "taken",
                terminal: true,
            },
            Transition {
                from: "offered",
                event: "revoke",
                to: "revoked",
                terminal: true,
            },
        ]
    }
}

#[cfg(test)]
#[path = "offer_slot_machine_test.rs"]
mod tests;
