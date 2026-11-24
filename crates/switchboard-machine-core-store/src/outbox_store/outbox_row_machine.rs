//! The durable transitions of one outbox row.
#![deny(clippy::wildcard_enum_match_arm)]

use crate::{OutboxCancellation, outbox_item_state::OutboxItemState};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use OutboxItemState::{
    Answered, InOutbox, NotSent, Refused, Resent, Sent, SentAnswerLost, Unconfirmed,
};

#[derive(Clone, Copy)]
pub(super) struct OutboxRowState {
    pub(super) item_state: OutboxItemState,
    pub(super) flush_at_ms: i64,
    pub(super) deadline_ms: i64,
}

#[derive(Clone)]
pub(super) enum OutboxRowEvent {
    Claim {
        now_ms: i64,
        eligible_through_ms: i64,
        required_deadline_after_ms: i64,
    },
    ReturnNotTransmitted {
        now_ms: i64,
    },
    ReturnUnanswered {
        now_ms: i64,
    },
    EndDue {
        now_ms: i64,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum OutboxRowEffect {
    Stamp {
        now_ms: i64,
    },
    Finish {
        now_ms: i64,
        reason: OutboxCancellation,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum OutboxRowRefusal {
    NotDue,
}

pub(super) struct OutboxRowMachine;

const CLAIM: &str = "claim";
const RETURN_NOT_TRANSMITTED: &str = "returnNotTransmitted";
const RETURN_UNANSWERED: &str = "returnUnanswered";
const END_DUE: &str = "endDue";

const TRANSITIONS: &[Transition] = &[
    edge(InOutbox, CLAIM, Sent),
    edge(SentAnswerLost, CLAIM, Resent),
    // Evidence before handing a frame to the lane, even when cutoff just passed.
    edge(Sent, RETURN_NOT_TRANSMITTED, InOutbox),
    edge(Resent, RETURN_NOT_TRANSMITTED, SentAnswerLost),
    // An inconclusive live result returns the claim. An inconclusive orphan
    // read leaves it alone, so the store receives no event.
    edge(Sent, RETURN_UNANSWERED, SentAnswerLost),
    edge(Resent, RETURN_UNANSWERED, SentAnswerLost),
    // Includes a lane-held ticket whose fence reached the write's cutoff.
    edge(Sent, RETURN_UNANSWERED, Unconfirmed),
    edge(Resent, RETURN_UNANSWERED, Unconfirmed),
    edge(InOutbox, END_DUE, NotSent),
    edge(SentAnswerLost, END_DUE, Unconfirmed),
    edge(Sent, END_DUE, Unconfirmed),
    edge(Resent, END_DUE, Unconfirmed),
];

const fn edge(from: OutboxItemState, event: &'static str, to: OutboxItemState) -> Transition {
    Transition {
        from: from.as_str(),
        event,
        to: to.as_str(),
        terminal: ends_sending(to),
    }
}

const fn ends_sending(state: OutboxItemState) -> bool {
    match state {
        Answered | Refused | NotSent | Unconfirmed => true,
        InOutbox | SentAnswerLost | Sent | Resent => false,
    }
}

impl StateTag for OutboxRowState {
    fn state_tag(&self) -> &'static str {
        self.item_state.as_str()
    }

    fn is_terminal(&self) -> bool {
        ends_sending(self.item_state)
    }
}

impl EventTag for OutboxRowEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Claim { .. } => CLAIM,
            Self::ReturnNotTransmitted { .. } => RETURN_NOT_TRANSMITTED,
            Self::ReturnUnanswered { .. } => RETURN_UNANSWERED,
            Self::EndDue { .. } => END_DUE,
        }
    }
}

impl RefusalTag for OutboxRowRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::NotDue => "notDue",
        }
    }
}

impl StateMachine for OutboxRowMachine {
    type State = OutboxRowState;
    type Event = OutboxRowEvent;
    type Effect = OutboxRowEffect;
    type Error = OutboxRowRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use OutboxCancellation::{NotConfirmedWithin15Minutes, NotSentWithin15Minutes};
        use OutboxRowEvent::{Claim, EndDue, ReturnNotTransmitted, ReturnUnanswered};

        let not_allowed = TransitionError::NotAllowed {
            state: state.state_tag(),
            event: event.event_tag(),
        };
        let not_due = TransitionError::Domain(OutboxRowRefusal::NotDue);
        let (item_state, effect) = match (state.item_state, event) {
            (
                waiting @ (InOutbox | SentAnswerLost),
                Claim {
                    now_ms,
                    eligible_through_ms,
                    required_deadline_after_ms,
                },
            ) => {
                if state.flush_at_ms > eligible_through_ms
                    || state.deadline_ms <= required_deadline_after_ms
                {
                    return Err(not_due);
                }
                let claimed = if waiting == InOutbox { Sent } else { Resent };
                (claimed, OutboxRowEffect::Stamp { now_ms })
            }
            (Sent, ReturnNotTransmitted { now_ms }) => {
                (InOutbox, OutboxRowEffect::Stamp { now_ms })
            }
            (Resent, ReturnNotTransmitted { now_ms }) => {
                (SentAnswerLost, OutboxRowEffect::Stamp { now_ms })
            }
            (Sent | Resent, ReturnUnanswered { now_ms }) if now_ms < state.deadline_ms => {
                (SentAnswerLost, OutboxRowEffect::Stamp { now_ms })
            }
            (Sent | Resent, ReturnUnanswered { now_ms }) => {
                finish(now_ms, NotConfirmedWithin15Minutes)
            }
            (live @ (InOutbox | SentAnswerLost | Sent | Resent), EndDue { now_ms }) => {
                if now_ms < state.deadline_ms {
                    return Err(not_due);
                }
                let reason = if live == InOutbox {
                    NotSentWithin15Minutes
                } else {
                    NotConfirmedWithin15Minutes
                };
                finish(now_ms, reason)
            }
            (Sent | Resent, Claim { .. })
            | (InOutbox | SentAnswerLost, ReturnNotTransmitted { .. } | ReturnUnanswered { .. })
            | (
                Answered | Refused | NotSent | Unconfirmed,
                Claim { .. }
                | ReturnNotTransmitted { .. }
                | ReturnUnanswered { .. }
                | EndDue { .. },
            ) => return Err(not_allowed),
        };
        Ok((
            OutboxRowState {
                item_state,
                ..*state
            },
            vec![effect],
        ))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn finish(now_ms: i64, reason: OutboxCancellation) -> (OutboxItemState, OutboxRowEffect) {
    (
        OutboxItemState::ended(reason),
        OutboxRowEffect::Finish { now_ms, reason },
    )
}

#[cfg(test)]
#[path = "outbox_row_machine_test.rs"]
mod tests;
