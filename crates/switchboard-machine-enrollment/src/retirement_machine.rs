//! Pure retirement admission, cleanup decisions, and ending receipts.
#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::{
    BindingGeneration, ConditionalWrite, CredentialStoreError, EnrollmentEnd, EnrollmentKey,
    EnrollmentMetadata,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RetirementRequest {
    pub(super) why: EnrollmentEnd,
    pub(super) through: Option<BindingGeneration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetirementState {
    Unfenced,
    Pending {
        request: RetirementRequest,
        queued: Option<EnrollmentEnd>,
    },
    Durable(EnrollmentEnd),
    Superseded(EnrollmentEnd),
    StaleOwnerEnding(Option<EnrollmentEnd>),
}

impl RetirementState {
    pub(super) fn pending(request: RetirementRequest) -> Self {
        Self::Pending {
            request,
            queued: None,
        }
    }

    pub(super) fn ending(self) -> Option<EnrollmentEnd> {
        match self {
            Self::Pending { request, .. } => Some(request.why),
            Self::Durable(why) | Self::Superseded(why) => Some(why),
            Self::Unfenced | Self::StaleOwnerEnding(_) => None,
        }
    }
}

impl StateTag for RetirementState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Unfenced => "unfenced",
            Self::Pending { .. } => "pending",
            Self::Durable(_) => "durable",
            Self::Superseded(_) => "superseded",
            Self::StaleOwnerEnding(_) => "staleOwnerEnding",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Durable(_) | Self::Superseded(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetirementOutcome {
    Durable,
    Superseded,
    StaleOwnerEnding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CleanupDecision {
    Remove(BindingGeneration),
    Settle(RetirementOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EndingWriteDecision {
    Finished(Result<ConditionalWrite, CredentialStoreError>),
    ReadBack,
    MarkUnusable,
}

#[derive(Clone, Copy)]
pub(super) enum RetirementEvent<'row> {
    DefinitiveRequested(EnrollmentEnd),
    OwnerEndingRequested {
        why: EnrollmentEnd,
        through: BindingGeneration,
    },
    InitialCleanupRead {
        key: &'row EnrollmentKey,
        observed: &'row EnrollmentMetadata,
    },
    RemoveReturned(Option<Result<ConditionalWrite, CredentialStoreError>>),
    MarkUnusableReturned(Option<Result<ConditionalWrite, CredentialStoreError>>),
    SubmittedWriteReadback {
        key: &'row EnrollmentKey,
        generation: BindingGeneration,
        observed: &'row EnrollmentMetadata,
    },
    DirectReplayRead {
        key: &'row EnrollmentKey,
        why: EnrollmentEnd,
        observed: &'row EnrollmentMetadata,
    },
    CleanupSettled(RetirementOutcome),
    ReleaseStaleOwnerEnding,
}

impl EventTag for RetirementEvent<'_> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::DefinitiveRequested(_) => "definitiveRequested",
            Self::OwnerEndingRequested { .. } => "ownerEndingRequested",
            Self::InitialCleanupRead { .. } => "initialCleanupRead",
            Self::RemoveReturned(_) => "removeReturned",
            Self::MarkUnusableReturned(_) => "markUnusableReturned",
            Self::SubmittedWriteReadback { .. } => "submittedWriteReadback",
            Self::DirectReplayRead { .. } => "directReplayRead",
            Self::CleanupSettled(_) => "cleanupSettled",
            Self::ReleaseStaleOwnerEnding => "releaseStaleOwnerEnding",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum RetirementEffect {
    Cleanup(CleanupDecision),
    EndingWrite(EndingWriteDecision),
    Replay(ConditionalWrite),
}

#[derive(Debug)]
pub(super) enum RetirementRefusal {}

impl RefusalTag for RetirementRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

pub(super) struct RetirementMachine<'row>(std::marker::PhantomData<&'row ()>);

const TRANSITIONS: &[Transition] = &[
    edge("unfenced", "definitiveRequested", "pending", false),
    edge("unfenced", "ownerEndingRequested", "pending", false),
    edge("pending", "definitiveRequested", "pending", false),
    edge("pending", "initialCleanupRead", "pending", false),
    edge("pending", "removeReturned", "pending", false),
    edge("pending", "markUnusableReturned", "pending", false),
    edge("pending", "submittedWriteReadback", "pending", false),
    edge("unfenced", "directReplayRead", "unfenced", false),
    edge("pending", "cleanupSettled", "durable", true),
    edge("pending", "cleanupSettled", "superseded", true),
    edge("pending", "cleanupSettled", "staleOwnerEnding", false),
    edge(
        "staleOwnerEnding",
        "releaseStaleOwnerEnding",
        "pending",
        false,
    ),
    edge(
        "staleOwnerEnding",
        "releaseStaleOwnerEnding",
        "unfenced",
        false,
    ),
];

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

impl<'row> StateMachine for RetirementMachine<'row> {
    type State = RetirementState;
    type Event = RetirementEvent<'row>;
    type Effect = RetirementEffect;
    type Error = RetirementRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use RetirementEvent as Event;
        use RetirementState as State;

        let next = match (*state, event) {
            (State::Unfenced, Event::DefinitiveRequested(why)) => {
                State::pending(RetirementRequest { why, through: None })
            }
            (State::Unfenced, Event::OwnerEndingRequested { why, through }) => {
                State::pending(RetirementRequest {
                    why,
                    through: Some(through),
                })
            }
            (
                State::Pending {
                    request,
                    queued: None,
                },
                Event::DefinitiveRequested(why),
            ) if request.through.is_some() => State::Pending {
                request,
                queued: Some(why),
            },
            (State::Pending { request, .. }, Event::InitialCleanupRead { key, observed }) => {
                let decision = match observed {
                    // Initial cleanup only asks whether this key is already ended.
                    EnrollmentMetadata::Ended { key: ended, .. } if ended == key => {
                        CleanupDecision::Settle(RetirementOutcome::Durable)
                    }
                    EnrollmentMetadata::Active {
                        key: active,
                        generation,
                        ..
                    } if active == key => {
                        if request.through.is_some_and(|ceiling| *generation > ceiling) {
                            CleanupDecision::Settle(RetirementOutcome::StaleOwnerEnding)
                        } else {
                            CleanupDecision::Remove(*generation)
                        }
                    }
                    EnrollmentMetadata::Empty
                    | EnrollmentMetadata::Ended { .. }
                    | EnrollmentMetadata::Active { .. } => {
                        CleanupDecision::Settle(RetirementOutcome::Superseded)
                    }
                };
                return Ok((*state, vec![RetirementEffect::Cleanup(decision)]));
            }
            (State::Pending { .. }, Event::RemoveReturned(reply)) => {
                return Ok((
                    *state,
                    vec![RetirementEffect::EndingWrite(write_decision(reply, true))],
                ));
            }
            (State::Pending { .. }, Event::MarkUnusableReturned(reply)) => {
                return Ok((
                    *state,
                    vec![RetirementEffect::EndingWrite(write_decision(reply, false))],
                ));
            }
            (
                State::Pending { request, .. },
                Event::SubmittedWriteReadback {
                    key,
                    generation,
                    observed,
                },
            ) => {
                // A lost write reply needs the exact key, reason, and a generation floor.
                let decision = if matches!(observed, EnrollmentMetadata::Ended {
                    key: ended, why, generation: saved,
                } if ended == key && *why == request.why && *saved >= generation)
                {
                    EndingWriteDecision::Finished(Ok(ConditionalWrite::Applied))
                } else {
                    EndingWriteDecision::MarkUnusable
                };
                return Ok((*state, vec![RetirementEffect::EndingWrite(decision)]));
            }
            (State::Unfenced, Event::DirectReplayRead { key, why, observed }) => {
                // A direct replay has no submitted generation to compare.
                let answer = if matches!(observed, EnrollmentMetadata::Ended {
                    key: ended, why: recorded, ..
                } if ended == key && *recorded == why)
                {
                    ConditionalWrite::Applied
                } else {
                    ConditionalWrite::Superseded
                };
                return Ok((*state, vec![RetirementEffect::Replay(answer)]));
            }
            (State::Pending { request, queued }, Event::CleanupSettled(outcome)) => match outcome {
                RetirementOutcome::Durable => State::Durable(request.why),
                RetirementOutcome::Superseded => State::Superseded(request.why),
                RetirementOutcome::StaleOwnerEnding => State::StaleOwnerEnding(queued),
            },
            (State::StaleOwnerEnding(queued), Event::ReleaseStaleOwnerEnding) => match queued {
                Some(why) => State::pending(RetirementRequest { why, through: None }),
                None => State::Unfenced,
            },
            (
                State::Pending { .. },
                Event::DefinitiveRequested(_) | Event::OwnerEndingRequested { .. },
            )
            | (
                State::Durable(_) | State::Superseded(_),
                Event::DefinitiveRequested(_)
                | Event::OwnerEndingRequested { .. }
                | Event::CleanupSettled(_),
            ) => return Err(TransitionError::Stale),
            (
                State::Unfenced,
                Event::InitialCleanupRead { .. }
                | Event::RemoveReturned(_)
                | Event::MarkUnusableReturned(_)
                | Event::SubmittedWriteReadback { .. }
                | Event::CleanupSettled(_)
                | Event::ReleaseStaleOwnerEnding,
            )
            | (
                State::Pending { .. },
                Event::DirectReplayRead { .. } | Event::ReleaseStaleOwnerEnding,
            )
            | (
                State::Durable(_) | State::Superseded(_),
                Event::InitialCleanupRead { .. }
                | Event::RemoveReturned(_)
                | Event::MarkUnusableReturned(_)
                | Event::SubmittedWriteReadback { .. }
                | Event::DirectReplayRead { .. }
                | Event::ReleaseStaleOwnerEnding,
            )
            | (
                State::StaleOwnerEnding(_),
                Event::DefinitiveRequested(_)
                | Event::OwnerEndingRequested { .. }
                | Event::InitialCleanupRead { .. }
                | Event::RemoveReturned(_)
                | Event::MarkUnusableReturned(_)
                | Event::SubmittedWriteReadback { .. }
                | Event::DirectReplayRead { .. }
                | Event::CleanupSettled(_),
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event.event_tag(),
                });
            }
        };
        Ok((next, Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn write_decision(
    reply: Option<Result<ConditionalWrite, CredentialStoreError>>,
    first_remove: bool,
) -> EndingWriteDecision {
    match reply {
        Some(Ok(outcome)) => EndingWriteDecision::Finished(Ok(outcome)),
        None | Some(Err(CredentialStoreError::OutcomeNotKnown)) => EndingWriteDecision::ReadBack,
        Some(Err(_)) if first_remove => EndingWriteDecision::MarkUnusable,
        Some(Err(error)) => EndingWriteDecision::Finished(Err(error)),
    }
}

pub(super) fn read_effect(
    state: RetirementState,
    event: RetirementEvent<'_>,
) -> Result<RetirementEffect, CredentialStoreError> {
    // The refused-transition mapping cannot fire because each caller feeds the
    // event its state declares. It stays because transition returns a Result.
    let (_, effects) =
        RetirementMachine::transition(&state, event).map_err(|_| CredentialStoreError::Read)?;
    // All observation and write-return events emit exactly one typed effect.
    effects.into_iter().next().ok_or(CredentialStoreError::Read)
}

#[cfg(test)]
#[path = "retirement_machine_test.rs"]
mod tests;
