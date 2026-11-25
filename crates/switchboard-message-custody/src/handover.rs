//! Per-message D4 handover decisions. Custody commits transitions before running effects.

#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffChange, DirectHandoffRow, NoticeAttemptId, NoticeRefusalReason,
    SeenReceiptState,
};
use workspace_model::Timestamp;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Handoff {
    pub expires_at: Timestamp,
    pub phase: DirectHandoff,
}

impl From<&DirectHandoffRow> for Handoff {
    fn from(row: &DirectHandoffRow) -> Self {
        Self {
            expires_at: row.expires_at,
            phase: row.handoff,
        }
    }
}

impl StateTag for Handoff {
    fn state_tag(&self) -> &'static str {
        match self.phase {
            DirectHandoff::Waiting => "Waiting",
            DirectHandoff::Writing { .. } => "Writing",
            DirectHandoff::NotKnown => "NotKnown",
            DirectHandoff::NotSeen => "NotSeen",
            DirectHandoff::NoticeRefused { .. } => "NoticeRefused",
            DirectHandoff::Seen { .. } => "Seen",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            DirectHandoff::NotSeen
                | DirectHandoff::NoticeRefused { .. }
                | DirectHandoff::Seen { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeWriteOutcome {
    Written {
        taken_at: Timestamp,
    },
    /// The provider requires a new prompt and did not accept the steer.
    SteerNotInjected,
    /// The provider explicitly refused the steer.
    SteerRefused,
    DefinitelyNotWritten(NoticePreTakeRefusal),
    NotKnown(NoticeUncertainty),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticePreTakeRefusal {
    CannotSteer,
    RunChanged,
    Expired,
    Retired,
    RejectedBeforeTake,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NoticeUncertainty {
    WriterOutcomeLost,
    UnitRetiredBeforeOutcome,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffEvent {
    EligibleAtMark {
        attempt: NoticeAttemptId,
        now: Timestamp,
    },
    Cutoff {
        now: Timestamp,
    },
    WriteFinished {
        attempt: NoticeAttemptId,
        outcome: NoticeWriteOutcome,
        now: Timestamp,
    },
    RecoverWriting,
    ReadAcknowledged {
        completed_at: Timestamp,
    },
}

impl EventTag for HandoffEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::EligibleAtMark { .. } => "EligibleAtMark",
            Self::Cutoff { .. } => "Cutoff",
            Self::WriteFinished { .. } => "WriteFinished",
            Self::RecoverWriting => "RecoverWriting",
            Self::ReadAcknowledged { .. } => "ReadAcknowledged",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffEffect {
    OfferClaimedNotice,
    WakeReceiptPump,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum HandoffRefusal {
    #[error("the inbox cutoff has not been reached")]
    CutoffNotReached,
    #[error("the writer reported a notice take at or after its delivery cutoff")]
    TakeOutsideDeliveryWindow,
}

#[derive(Debug, thiserror::Error)]
pub enum HandoverError {
    #[error("inbox handover storage failed")]
    Inbox(#[from] crate::inbox_error::InboxCustodyError),
    #[error("inbox handover returned an invalid transition")]
    Transition,
    #[error("inbox handover attempted to offer an empty notice")]
    EmptyClaim(#[from] EmptyNoticeClaim),
}

#[derive(Debug, thiserror::Error)]
#[error("a notice claim must cover at least one inbox message")]
pub struct EmptyNoticeClaim;

impl RefusalTag for HandoffRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::CutoffNotReached => "CutoffNotReached",
            Self::TakeOutsideDeliveryWindow => "TakeOutsideDeliveryWindow",
        }
    }
}

pub struct DirectHandoffMachine;

impl StateMachine for DirectHandoffMachine {
    type State = Handoff;
    type Event = HandoffEvent;
    type Effect = HandoffEffect;
    type Error = HandoffRefusal;

    fn transition(state: &Handoff, event: HandoffEvent) -> StateMachineResult<Self> {
        use DirectHandoff::{NotKnown, NotSeen, NoticeRefused, Seen, Waiting, Writing};
        use HandoffEvent::{
            Cutoff, EligibleAtMark, ReadAcknowledged, RecoverWriting, WriteFinished,
        };

        let (phase, effects) = match (state.phase, event) {
            (Waiting, EligibleAtMark { attempt, now }) if now < state.expires_at => {
                (Writing { attempt }, vec![HandoffEffect::OfferClaimedNotice])
            }
            (Waiting, EligibleAtMark { .. }) => (NotSeen, vec![]),
            (Waiting | Writing { .. }, Cutoff { now }) if now < state.expires_at => {
                return Err(TransitionError::Domain(HandoffRefusal::CutoffNotReached));
            }
            (Waiting, Cutoff { .. }) => (NotSeen, vec![]),
            // A take may already have happened. The cutoff closes admission,
            // but does not replace evidence from the outstanding attempt.
            (Writing { .. }, Cutoff { .. }) => (state.phase, vec![]),
            (
                Writing { attempt: active },
                WriteFinished {
                    attempt,
                    outcome,
                    now,
                },
            ) => {
                if active != attempt {
                    return Err(TransitionError::Stale);
                }
                match outcome {
                    NoticeWriteOutcome::Written { taken_at } if taken_at < state.expires_at => {
                        seen(taken_at)
                    }
                    NoticeWriteOutcome::Written { .. } => {
                        return Err(TransitionError::Domain(
                            HandoffRefusal::TakeOutsideDeliveryWindow,
                        ));
                    }
                    NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::CannotSteer) => {
                        (
                            NoticeRefused {
                                reason: NoticeRefusalReason::CannotSteer,
                            },
                            vec![],
                        )
                    }
                    NoticeWriteOutcome::DefinitelyNotWritten(_)
                    | NoticeWriteOutcome::SteerNotInjected
                        if now < state.expires_at =>
                    {
                        (Waiting, vec![])
                    }
                    NoticeWriteOutcome::DefinitelyNotWritten(_)
                    | NoticeWriteOutcome::SteerNotInjected
                    | NoticeWriteOutcome::SteerRefused => (NotSeen, vec![]),
                    NoticeWriteOutcome::NotKnown(_) => (NotKnown, vec![]),
                }
            }
            (Writing { .. }, RecoverWriting) => (NotKnown, vec![]),
            (Waiting | NotKnown, ReadAcknowledged { completed_at }) => seen(completed_at),
            (
                Waiting | NotKnown | NotSeen | NoticeRefused { .. } | Seen { .. },
                WriteFinished { .. },
            ) => {
                return Err(TransitionError::Stale);
            }
            (Waiting, RecoverWriting)
            | (Writing { .. }, EligibleAtMark { .. } | ReadAcknowledged { .. })
            | (NotKnown, EligibleAtMark { .. } | Cutoff { .. } | RecoverWriting)
            | (
                NotSeen | NoticeRefused { .. } | Seen { .. },
                EligibleAtMark { .. } | Cutoff { .. } | RecoverWriting | ReadAcknowledged { .. },
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event.event_tag(),
                });
            }
        };
        Ok((Handoff { phase, ..*state }, effects))
    }

    fn transitions() -> &'static [Transition] {
        const TRANSITIONS: &[Transition] = &[
            edge("Waiting", "EligibleAtMark", "Writing", false),
            edge("Waiting", "EligibleAtMark", "NotSeen", true),
            edge("Waiting", "Cutoff", "NotSeen", true),
            edge("Writing", "Cutoff", "Writing", false),
            edge("Writing", "WriteFinished", "Waiting", false),
            edge("Writing", "WriteFinished", "NotSeen", true),
            edge("Writing", "WriteFinished", "NoticeRefused", true),
            edge("Writing", "WriteFinished", "Seen", true),
            edge("Writing", "WriteFinished", "NotKnown", false),
            edge("Writing", "RecoverWriting", "NotKnown", false),
            edge("Waiting", "ReadAcknowledged", "Seen", true),
            edge("NotKnown", "ReadAcknowledged", "Seen", true),
        ];
        TRANSITIONS
    }
}

fn seen(completed_at: Timestamp) -> (DirectHandoff, Vec<HandoffEffect>) {
    (
        DirectHandoff::Seen {
            completed_at,
            receipt: SeenReceiptState::Owed,
        },
        vec![HandoffEffect::WakeReceiptPump],
    )
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

pub(super) fn changes_for(
    rows: &[DirectHandoffRow],
    event: HandoffEvent,
) -> Result<(Vec<DirectHandoffChange>, bool), HandoverError> {
    let mut offer = false;
    let mut changes = Vec::with_capacity(rows.len());
    for row in rows {
        let (next, effects) = DirectHandoffMachine::transition(&Handoff::from(row), event)
            .map_err(|_| HandoverError::Transition)?;
        offer |= effects.contains(&HandoffEffect::OfferClaimedNotice);
        changes.push(DirectHandoffChange {
            message_id: row.message_id.clone(),
            expected: row.handoff,
            next: next.phase,
        });
    }
    Ok((changes, offer))
}

#[cfg(test)]
#[path = "handover_test.rs"]
mod tests;
