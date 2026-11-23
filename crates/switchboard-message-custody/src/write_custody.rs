//! The life of a write. The store remains the durable owner of each request.
#![deny(clippy::wildcard_enum_match_arm)]

use central_link_protocol::{ReplyPayload, RequestRefusal};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use workspace_ids::MessageId;
use workspace_model::central_mail::{CentralMailAnswer, CentralMailResult};

/// Custody's projection of a write as it is claimed, replayed and settled.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteCustodyState {
    InOutbox,
    FirstClaimed,
    ReplayWaiting,
    ReplayClaimed,
    Accepted(Box<ReplyPayload>),
    Refused(RequestRefusal),
    NotSent,
    NotConfirmed,
}

/// An event custody makes from a store commit, the original deadline, or the link's answer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteCustodyEvent {
    ClaimedBeforeDeadline,
    FrameNotReached,
    FrameAnswerLost,
    CentralAccepted(Box<ReplyPayload>),
    CentralRefused(RequestRefusal),
    DeadlineReached,
    /// A caller-scoped outcome proved that central accepted this direct message.
    DeliveryConfirmed {
        message_id: MessageId,
    },
}

/// A write transition refusal produced by the write policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WriteCustodyRefusal {
    #[error("central's final outcome conflicts with the one already kept")]
    ConflictingFinalOutcome,
}

impl RefusalTag for WriteCustodyRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::ConflictingFinalOutcome => "conflictingFinalOutcome",
        }
    }
}

impl StateTag for WriteCustodyState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::InOutbox => "inOutbox",
            Self::FirstClaimed => "firstClaimed",
            Self::ReplayWaiting => "replayWaiting",
            Self::ReplayClaimed => "replayClaimed",
            Self::Accepted(_) => "accepted",
            Self::Refused(_) => "refused",
            Self::NotSent => "notSent",
            Self::NotConfirmed => "notConfirmed",
        }
    }

    fn is_terminal(&self) -> bool {
        // A deadline ends retries, but later delivery evidence can establish acceptance.
        matches!(self, Self::Accepted(_) | Self::Refused(_) | Self::NotSent)
    }
}

impl EventTag for WriteCustodyEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::ClaimedBeforeDeadline => "claimed",
            Self::FrameNotReached => "notReached",
            Self::FrameAnswerLost => "answerLost",
            Self::CentralAccepted(_) => "accepted",
            Self::CentralRefused(_) => "refused",
            Self::DeadlineReached => "deadline",
            Self::DeliveryConfirmed { .. } => "deliveryConfirmed",
        }
    }
}

/// The pure write policy custody applies to observations of an individual request.
pub struct WriteCustodyMachine;

impl StateMachine for WriteCustodyMachine {
    type State = WriteCustodyState;
    type Event = WriteCustodyEvent;
    type Effect = ();
    type Error = WriteCustodyRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use WriteCustodyEvent::{
            CentralAccepted, CentralRefused, ClaimedBeforeDeadline, DeadlineReached,
            DeliveryConfirmed, FrameAnswerLost, FrameNotReached,
        };
        use WriteCustodyState::{
            Accepted, FirstClaimed, InOutbox, NotConfirmed, NotSent, Refused, ReplayClaimed,
            ReplayWaiting,
        };
        let next = match (state, event) {
            (InOutbox, ClaimedBeforeDeadline) => FirstClaimed,
            (ReplayWaiting, ClaimedBeforeDeadline) => ReplayClaimed,
            (FirstClaimed, FrameNotReached) => InOutbox,
            (ReplayClaimed, FrameNotReached) | (FirstClaimed | ReplayClaimed, FrameAnswerLost) => {
                ReplayWaiting
            }
            (FirstClaimed | ReplayClaimed, CentralAccepted(reply)) => Accepted(reply),
            (FirstClaimed | ReplayClaimed, CentralRefused(reason)) => Refused(reason),
            (InOutbox, DeadlineReached) => NotSent,
            (FirstClaimed | ReplayWaiting | ReplayClaimed, DeadlineReached) => NotConfirmed,
            (
                FirstClaimed | ReplayWaiting | ReplayClaimed | NotConfirmed,
                DeliveryConfirmed { message_id },
            ) => Accepted(Box::new(ReplyPayload::Mail(CentralMailResult::Done(
                CentralMailAnswer::Queued { message_id },
            )))),
            (Accepted(previous), CentralAccepted(reply)) if *previous == reply => {
                return Err(TransitionError::Stale);
            }
            (Refused(previous), CentralRefused(reason)) if *previous == reason => {
                return Err(TransitionError::Stale);
            }
            (Accepted(_) | Refused(_), CentralAccepted(_) | CentralRefused(_)) => {
                return Err(TransitionError::Domain(
                    WriteCustodyRefusal::ConflictingFinalOutcome,
                ));
            }
            (Accepted(_) | Refused(_) | NotSent | NotConfirmed, _) => {
                return Err(TransitionError::Stale);
            }
            (unsettled, event) => {
                return Err(TransitionError::NotAllowed {
                    state: unsettled.state_tag(),
                    event: event.event_tag(),
                });
            }
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        const DECLARED: &[Transition] = &[
            write_transition("inOutbox", "claimed", "firstClaimed", false),
            write_transition("replayWaiting", "claimed", "replayClaimed", false),
            write_transition("firstClaimed", "notReached", "inOutbox", false),
            write_transition("replayClaimed", "notReached", "replayWaiting", false),
            write_transition("firstClaimed", "answerLost", "replayWaiting", false),
            write_transition("replayClaimed", "answerLost", "replayWaiting", false),
            write_transition("firstClaimed", "accepted", "accepted", true),
            write_transition("replayClaimed", "accepted", "accepted", true),
            write_transition("firstClaimed", "refused", "refused", true),
            write_transition("replayClaimed", "refused", "refused", true),
            write_transition("inOutbox", "deadline", "notSent", true),
            write_transition("firstClaimed", "deadline", "notConfirmed", false),
            write_transition("replayWaiting", "deadline", "notConfirmed", false),
            write_transition("replayClaimed", "deadline", "notConfirmed", false),
            write_transition("firstClaimed", "deliveryConfirmed", "accepted", true),
            write_transition("replayWaiting", "deliveryConfirmed", "accepted", true),
            write_transition("replayClaimed", "deliveryConfirmed", "accepted", true),
            write_transition("notConfirmed", "deliveryConfirmed", "accepted", true),
        ];
        DECLARED
    }
}

const fn write_transition(
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
#[path = "write_custody_test.rs"]
mod tests;
