//! Recipient custody through the confirmation of a durable inbox decision.

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_machine_core_store::{InboxDecision, InboxRefusalReason};

/// Custody's view of one offered message before and after the inbox commits its decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryCustodyState {
    AwaitingDecision,
    KeptAwaitingReceipt,
    KeptAcknowledged,
    RefusedAwaitingReceipt { reason: InboxRefusalReason },
    RefusalAcknowledged { reason: InboxRefusalReason },
}

/// A committed inbox decision or correlated receipt confirmation observed by custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryCustodyEvent {
    DecisionCommitted(InboxDecision),
    ReceiptConfirmed,
}

/// A receipt the delivery policy makes eligible to send after an inbox commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryCustodyEffect {
    ReceiptPending(InboxDecision),
}

/// A domain refusal of the delivery policy. Transition ordering uses the library's errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryCustodyRefusal {}

impl RefusalTag for DeliveryCustodyRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

impl StateTag for DeliveryCustodyState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::AwaitingDecision => "awaitingDecision",
            Self::KeptAwaitingReceipt => "keptAwaitingReceipt",
            Self::KeptAcknowledged => "keptAcknowledged",
            Self::RefusedAwaitingReceipt { .. } => "refusedAwaitingReceipt",
            Self::RefusalAcknowledged { .. } => "refusalAcknowledged",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::KeptAcknowledged | Self::RefusalAcknowledged { .. }
        )
    }
}

impl EventTag for DeliveryCustodyEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::DecisionCommitted(InboxDecision::Accepted) => "kept",
            Self::DecisionCommitted(InboxDecision::Refused { .. }) => "refused",
            Self::ReceiptConfirmed => "receiptConfirmed",
        }
    }
}

/// The pure policy custody applies to a recipient inbox decision.
pub struct DeliveryCustodyMachine;

impl StateMachine for DeliveryCustodyMachine {
    type State = DeliveryCustodyState;
    type Event = DeliveryCustodyEvent;
    type Effect = DeliveryCustodyEffect;
    type Error = DeliveryCustodyRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use DeliveryCustodyEvent::{DecisionCommitted, ReceiptConfirmed};
        use DeliveryCustodyState::{
            AwaitingDecision, KeptAcknowledged, KeptAwaitingReceipt, RefusalAcknowledged,
            RefusedAwaitingReceipt,
        };
        match (*state, event) {
            (AwaitingDecision, DecisionCommitted(decision)) => {
                let next = match decision {
                    InboxDecision::Accepted => KeptAwaitingReceipt,
                    InboxDecision::Refused { reason } => RefusedAwaitingReceipt { reason },
                };
                Ok((next, vec![DeliveryCustodyEffect::ReceiptPending(decision)]))
            }
            (KeptAwaitingReceipt, ReceiptConfirmed) => Ok((KeptAcknowledged, Vec::new())),
            (RefusedAwaitingReceipt { reason }, ReceiptConfirmed) => {
                Ok((RefusalAcknowledged { reason }, Vec::new()))
            }
            (AwaitingDecision, ReceiptConfirmed) => Err(TransitionError::NotAllowed {
                state: state.state_tag(),
                event: event.event_tag(),
            }),
            (KeptAwaitingReceipt | RefusedAwaitingReceipt { .. }, DecisionCommitted(_))
            | (
                KeptAcknowledged | RefusalAcknowledged { .. },
                DecisionCommitted(_) | ReceiptConfirmed,
            ) => Err(TransitionError::Stale),
        }
    }

    fn transitions() -> &'static [Transition] {
        const DECLARED: &[Transition] = &[
            delivery_transition("awaitingDecision", "kept", "keptAwaitingReceipt", false),
            delivery_transition(
                "awaitingDecision",
                "refused",
                "refusedAwaitingReceipt",
                false,
            ),
            delivery_transition(
                "keptAwaitingReceipt",
                "receiptConfirmed",
                "keptAcknowledged",
                true,
            ),
            delivery_transition(
                "refusedAwaitingReceipt",
                "receiptConfirmed",
                "refusalAcknowledged",
                true,
            ),
        ];
        DECLARED
    }
}

const fn delivery_transition(
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
#[path = "delivery_custody_test.rs"]
mod tests;
