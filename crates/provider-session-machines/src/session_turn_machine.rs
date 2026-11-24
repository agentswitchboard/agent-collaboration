//! A session's turn, as a pure machine (D1 state machines; design note §3.5,
//! §12.4). A steer's approver travels with its injection result, so the turn
//! keeps no record for it.

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

pub use switchboard_harness_contracts::TurnId;

/// The identity that ties a witnessed provider end to its operation custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TurnSettlement {
    pub turn: TurnId,
    pub operation: uuid::Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnRecordDisposition {
    Saved,
    AppliedUnsaved,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTurn {
    Idle,
    Running { turn: TurnId },
    Cancelling { turn: TurnId },
    Settling(TurnSettlement),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTurnEvent {
    PromptStarted {
        turn: TurnId,
    },
    SteerRequested {
        agent_steers: bool,
    },
    CancelRequested,
    TurnEnded {
        turn: TurnId,
    },
    ProviderEnded(TurnSettlement),
    SettlementCleared {
        settlement: TurnSettlement,
        record: TurnRecordDisposition,
    },
    SettleDeadlineReached(TurnSettlement),
    CloseWhileSettling(TurnSettlement),
    SettlementRetired(TurnSettlement),
    ProviderStartedSuccessor {
        previous: TurnSettlement,
        turn: TurnId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTurnEffect {
    InjectSteer {
        turn: TurnId,
    },
    WriteCancel {
        turn: TurnId,
    },
    /// A terminal fact: always published (design note §11.1).
    PublishTurnEnded {
        turn: TurnId,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTurnRefusal {
    /// "a turn is running; steer, queue, or cancel it" (M8).
    TurnRunning,
    /// "no running turn to steer; send it as a prompt".
    NoTurnToSteer,
    /// The owner's rule: "cannot steer: queue or interrupt".
    CannotSteer,
    /// "no running turn to cancel; the session is idle".
    NoTurnToCancel,
    /// The turn is already being cancelled.
    CancelInProgress,
}

impl RefusalTag for SessionTurnRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::TurnRunning => "turn_running",
            Self::NoTurnToSteer => "no_turn_to_steer",
            Self::CannotSteer => "cannot_steer",
            Self::NoTurnToCancel => "no_turn_to_cancel",
            Self::CancelInProgress => "cancel_in_progress",
        }
    }
}

impl SessionTurnRefusal {
    pub const fn human_phrase(self) -> &'static str {
        match self {
            Self::TurnRunning => "a turn is running; steer, queue, or cancel it",
            Self::NoTurnToSteer => "no running turn to steer; send it as a prompt",
            Self::CannotSteer => "cannot steer: queue or interrupt",
            Self::NoTurnToCancel => "no running turn to cancel; the session is idle",
            Self::CancelInProgress => "the turn is already being cancelled; wait for it",
        }
    }
}

impl StateTag for SessionTurn {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running { .. } => "running",
            Self::Cancelling { .. } => "cancelling",
            Self::Settling(_) => "settling",
        }
    }

    fn is_terminal(&self) -> bool {
        match self {
            Self::Idle | Self::Running { .. } | Self::Cancelling { .. } | Self::Settling(_) => {
                false
            }
        }
    }
}

impl EventTag for SessionTurnEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::PromptStarted { .. } => "prompt_started",
            Self::SteerRequested { .. } => "steer_requested",
            Self::CancelRequested => "cancel_requested",
            Self::TurnEnded { .. } => "turn_ended",
            Self::ProviderEnded(_) => "provider_ended",
            Self::SettlementCleared { record, .. } => match record {
                TurnRecordDisposition::Saved => "settlement_saved",
                TurnRecordDisposition::AppliedUnsaved => "settlement_applied_unsaved",
                TurnRecordDisposition::Uncertain => "settlement_uncertain",
            },
            Self::SettleDeadlineReached(_) => "settle_deadline_reached",
            Self::CloseWhileSettling(_) => "close_while_settling",
            Self::SettlementRetired(_) => "settlement_retired",
            Self::ProviderStartedSuccessor { .. } => "provider_started_successor",
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
    edge("idle", "prompt_started", "running"),
    edge("running", "steer_requested", "running"),
    edge("running", "cancel_requested", "cancelling"),
    edge("running", "turn_ended", "idle"),
    edge("cancelling", "turn_ended", "idle"),
    edge("running", "provider_ended", "settling"),
    edge("cancelling", "provider_ended", "settling"),
    edge("settling", "settlement_saved", "idle"),
    edge("settling", "settlement_applied_unsaved", "idle"),
    edge("settling", "settlement_uncertain", "idle"),
    edge("settling", "settle_deadline_reached", "idle"),
    edge("settling", "close_while_settling", "settling"),
    edge("settling", "settlement_retired", "idle"),
    edge("settling", "provider_started_successor", "running"),
];

pub struct SessionTurnMachine;

fn refused(refusal: SessionTurnRefusal) -> StateMachineResult<SessionTurnMachine> {
    Err(TransitionError::Domain(refusal))
}

impl StateMachine for SessionTurnMachine {
    type State = SessionTurn;
    type Event = SessionTurnEvent;
    type Effect = SessionTurnEffect;
    type Error = SessionTurnRefusal;

    fn transition(state: &SessionTurn, event: SessionTurnEvent) -> StateMachineResult<Self> {
        use SessionTurn as S;
        use SessionTurnEffect as E;
        use SessionTurnEvent as V;
        use SessionTurnRefusal as R;
        match (*state, event) {
            (S::Idle, V::PromptStarted { turn }) => Ok((S::Running { turn }, Vec::new())),
            (S::Running { .. } | S::Cancelling { .. }, V::PromptStarted { .. }) => {
                refused(R::TurnRunning)
            }
            (S::Settling(_), V::PromptStarted { .. }) => Err(TransitionError::NotAllowed {
                state: state.state_tag(),
                event: event.event_tag(),
            }),
            (S::Running { turn }, V::SteerRequested { agent_steers: true }) => {
                Ok((S::Running { turn }, vec![E::InjectSteer { turn }]))
            }
            (
                S::Running { .. } | S::Cancelling { .. },
                V::SteerRequested {
                    agent_steers: false,
                },
            ) => refused(R::CannotSteer),
            (S::Cancelling { .. }, V::SteerRequested { agent_steers: true }) => {
                refused(R::CancelInProgress)
            }
            (S::Idle, V::SteerRequested { .. }) => refused(R::NoTurnToSteer),
            (S::Running { turn }, V::CancelRequested) => {
                Ok((S::Cancelling { turn }, vec![E::WriteCancel { turn }]))
            }
            (S::Cancelling { .. }, V::CancelRequested) => refused(R::CancelInProgress),
            (S::Idle, V::CancelRequested) => refused(R::NoTurnToCancel),
            (S::Settling(_), V::SteerRequested { .. } | V::CancelRequested) => {
                Err(TransitionError::Stale)
            }
            (S::Running { turn } | S::Cancelling { turn }, V::TurnEnded { turn: ended })
                if ended == turn =>
            {
                Ok((S::Idle, vec![E::PublishTurnEnded { turn }]))
            }
            (S::Running { turn } | S::Cancelling { turn }, V::ProviderEnded(settlement))
                if settlement.turn == turn =>
            {
                Ok((S::Settling(settlement), Vec::new()))
            }
            (
                S::Settling(current),
                V::SettlementCleared { settlement, .. }
                | V::SettleDeadlineReached(settlement)
                | V::SettlementRetired(settlement),
            ) if settlement == current => Ok((
                S::Idle,
                vec![E::PublishTurnEnded {
                    turn: settlement.turn,
                }],
            )),
            (S::Settling(current), V::CloseWhileSettling(settlement)) if settlement == current => {
                Ok((S::Settling(current), Vec::new()))
            }
            (S::Settling(current), V::ProviderStartedSuccessor { previous, turn })
                if previous == current && turn != previous.turn =>
            {
                Ok((
                    S::Running { turn },
                    vec![E::PublishTurnEnded {
                        turn: previous.turn,
                    }],
                ))
            }
            (
                S::Idle | S::Running { .. } | S::Cancelling { .. } | S::Settling(_),
                V::TurnEnded { .. }
                | V::ProviderEnded(_)
                | V::SettlementCleared { .. }
                | V::SettleDeadlineReached(_)
                | V::CloseWhileSettling(_)
                | V::SettlementRetired(_)
                | V::ProviderStartedSuccessor { .. },
            ) => Err(TransitionError::Stale),
        }
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

#[cfg(test)]
#[path = "session_turn_machine_test.rs"]
mod tests;
