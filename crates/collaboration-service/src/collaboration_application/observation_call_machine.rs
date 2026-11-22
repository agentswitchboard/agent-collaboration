//! The ordered effects of one bounded provider observation.
#![deny(clippy::wildcard_enum_match_arm)]

use collaboration_protocol::{ObservationCursor, ObservationEndReason};
use serde_json::Value;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ObservationState {
    Admitting,
    History,
    Live,
    Ended(ObservationEnding),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ObservationEnding {
    AttachFailed,
    Result(ObservationEndReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ObservationRecord {
    pub(super) sequence: u64,
    pub(super) value: Value,
}

#[derive(Clone, Debug)]
pub(super) enum ObservationArrival {
    Admission(Admission),
    History(HistoryArrival),
    Live(LiveArrival),
}

#[derive(Clone, Debug)]
pub(super) enum Admission {
    AttachFailed,
    Refused,
    Accepted,
}

#[derive(Clone, Debug)]
pub(super) enum HistoryArrival {
    Record(ObservationRecord),
    LimitWithMore(ObservationRecord),
    LimitAtEnd(ObservationRecord, ObservationCursor),
    DoesNotFit,
    Gap(Value),
    Exhausted(ObservationCursor),
}

#[derive(Clone, Debug)]
pub(super) enum LiveArrival {
    Record(ObservationRecord),
    Limit(ObservationRecord),
    DoesNotFit,
    Gap(Value),
    Disconnected,
    Deadline,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ObservationEffect {
    Record(ObservationRecord),
    SnapshotEnd(ObservationCursor),
    ResyncRequired(Value),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ObservationRefusal {}

impl RefusalTag for ObservationRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

impl StateTag for ObservationState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Admitting => "admitting",
            Self::History => "history",
            Self::Live => "live",
            Self::Ended(_) => "ended",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Ended(_))
    }
}

impl EventTag for ObservationArrival {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Admission(Admission::AttachFailed) => "attach_failed",
            Self::Admission(Admission::Refused) => "refused",
            Self::Admission(Admission::Accepted) => "accepted",
            Self::History(HistoryArrival::Record(_)) => "history_record",
            Self::History(HistoryArrival::LimitWithMore(_)) => "history_limit_with_more",
            Self::History(HistoryArrival::LimitAtEnd(..)) => "history_limit_at_end",
            Self::History(HistoryArrival::DoesNotFit) => "history_does_not_fit",
            Self::History(HistoryArrival::Gap(_)) => "history_gap",
            Self::History(HistoryArrival::Exhausted(_)) => "history_exhausted",
            Self::Live(LiveArrival::Record(_)) => "live_record",
            Self::Live(LiveArrival::Limit(_)) => "live_limit",
            Self::Live(LiveArrival::DoesNotFit) => "live_does_not_fit",
            Self::Live(LiveArrival::Gap(_)) => "live_gap",
            Self::Live(LiveArrival::Disconnected) => "disconnected",
            Self::Live(LiveArrival::Deadline) => "deadline",
        }
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

const TRANSITIONS: &[Transition] = &[
    edge("admitting", "attach_failed", "ended", true),
    edge("admitting", "refused", "ended", true),
    edge("admitting", "accepted", "history", false),
    edge("history", "history_record", "history", false),
    edge("history", "history_limit_with_more", "ended", true),
    edge("history", "history_limit_at_end", "ended", true),
    edge("history", "history_does_not_fit", "ended", true),
    edge("history", "history_gap", "ended", true),
    edge("history", "history_exhausted", "live", false),
    edge("live", "live_record", "live", false),
    edge("live", "live_limit", "ended", true),
    edge("live", "live_does_not_fit", "ended", true),
    edge("live", "live_gap", "ended", true),
    edge("live", "disconnected", "ended", true),
    edge("live", "deadline", "ended", true),
];

pub(super) struct ObservationCallMachine;

fn ended(reason: ObservationEndReason) -> ObservationState {
    ObservationState::Ended(ObservationEnding::Result(reason))
}

pub(super) fn bare_gap() -> Value {
    serde_json::json!({"kind":"resyncRequired"})
}

impl StateMachine for ObservationCallMachine {
    type State = ObservationState;
    type Event = ObservationArrival;
    type Effect = ObservationEffect;
    type Error = ObservationRefusal;

    fn transition(state: &Self::State, arrival: Self::Event) -> StateMachineResult<Self> {
        use ObservationArrival as Arrival;
        use ObservationEffect as Effect;
        use ObservationEndReason as Reason;
        use ObservationState as State;
        match (state, arrival) {
            (State::Admitting, Arrival::Admission(Admission::AttachFailed)) => {
                Ok((State::Ended(ObservationEnding::AttachFailed), vec![]))
            }
            (State::Admitting, Arrival::Admission(Admission::Refused)) => Ok((
                ended(Reason::ResyncRequired),
                vec![Effect::ResyncRequired(bare_gap())],
            )),
            (State::Admitting, Arrival::Admission(Admission::Accepted)) => {
                Ok((State::History, vec![]))
            }
            (State::History, Arrival::History(HistoryArrival::Record(record))) => {
                Ok((State::History, vec![Effect::Record(record)]))
            }
            (State::Live, Arrival::Live(LiveArrival::Record(record))) => {
                Ok((State::Live, vec![Effect::Record(record)]))
            }
            (State::History, Arrival::History(HistoryArrival::LimitWithMore(record)))
            | (State::Live, Arrival::Live(LiveArrival::Limit(record))) => Ok((
                ended(Reason::ResultLimitReached),
                vec![Effect::Record(record)],
            )),
            (State::History, Arrival::History(HistoryArrival::LimitAtEnd(record, cursor))) => Ok((
                ended(Reason::ResultLimitReached),
                vec![Effect::Record(record), Effect::SnapshotEnd(cursor)],
            )),
            (State::History, Arrival::History(HistoryArrival::DoesNotFit))
            | (State::Live, Arrival::Live(LiveArrival::DoesNotFit)) => {
                Ok((ended(Reason::ResultLimitReached), vec![]))
            }
            (State::History, Arrival::History(HistoryArrival::Gap(gap)))
            | (State::Live, Arrival::Live(LiveArrival::Gap(gap))) => Ok((
                ended(Reason::ResyncRequired),
                vec![Effect::ResyncRequired(gap)],
            )),
            (State::History, Arrival::History(HistoryArrival::Exhausted(cursor))) => {
                Ok((State::Live, vec![Effect::SnapshotEnd(cursor)]))
            }
            (State::Live, Arrival::Live(LiveArrival::Disconnected)) => {
                Ok((ended(Reason::BackendDisconnected), vec![]))
            }
            (State::Live, Arrival::Live(LiveArrival::Deadline)) => {
                Ok((ended(Reason::DeadlineReached), vec![]))
            }
            (State::Ended(_), _arrival) => Err(TransitionError::Stale),
            (State::Admitting, arrival @ (Arrival::History(_) | Arrival::Live(_)))
            | (State::History, arrival @ (Arrival::Admission(_) | Arrival::Live(_)))
            | (State::Live, arrival @ (Arrival::Admission(_) | Arrival::History(_))) => {
                Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: arrival.event_tag(),
                })
            }
        }
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

#[cfg(test)]
#[path = "observation_call_machine_test.rs"]
mod tests;
