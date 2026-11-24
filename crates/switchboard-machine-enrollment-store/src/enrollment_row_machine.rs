//! Pure decisions for the enrollment row under the SQLite writer reservation.
#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use switchboard_machine_core_store::{
    EnrollmentRowChange, EnrollmentRowCorruption, StoredEnrollmentRow,
};
use switchboard_machine_enrollment::{
    BindingGeneration, CredentialStoreError, EnrollmentContext, EnrollmentEnd, EnrollmentMetadata,
};

use crate::enrollment_row_mapping::{EnrollmentSnapshot, stored_end};

#[derive(Clone)]
pub(super) struct PreparedReplacement {
    pub(super) witness: EnrollmentSnapshot,
    pub(super) row: StoredEnrollmentRow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnrollmentRowPhase {
    Empty,
    Active,
    Ended,
}

impl StateTag for EnrollmentRowPhase {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Active => "active",
            Self::Ended => "ended",
        }
    }

    fn is_terminal(&self) -> bool {
        false // An ended enrollment can be replaced in its original scope.
    }
}

#[derive(Clone)]
enum EnrollmentRowEvent<'row> {
    Replace {
        expected: BindingGeneration,
        locked: &'row EnrollmentSnapshot,
        context: &'row EnrollmentContext,
        prepared: Option<Box<PreparedReplacement>>,
    },
    End {
        expected: BindingGeneration,
        locked: &'row EnrollmentMetadata,
        why: EnrollmentEnd,
    },
}

impl EnrollmentRowEvent<'_> {
    fn phase(&self) -> EnrollmentRowPhase {
        match self {
            Self::Replace { locked, .. } => match locked {
                EnrollmentSnapshot::Empty => EnrollmentRowPhase::Empty,
                EnrollmentSnapshot::Active { .. } => EnrollmentRowPhase::Active,
                EnrollmentSnapshot::Ended { .. } => EnrollmentRowPhase::Ended,
            },
            Self::End { locked, .. } => match locked {
                EnrollmentMetadata::Empty => EnrollmentRowPhase::Empty,
                EnrollmentMetadata::Active { .. } => EnrollmentRowPhase::Active,
                EnrollmentMetadata::Ended { .. } => EnrollmentRowPhase::Ended,
            },
        }
    }

    fn generation_matches(&self) -> bool {
        match self {
            Self::Replace {
                expected, locked, ..
            } => *expected == locked.generation(),
            Self::End {
                expected, locked, ..
            } => {
                let generation = match locked {
                    EnrollmentMetadata::Empty => BindingGeneration::EMPTY,
                    EnrollmentMetadata::Active { generation, .. }
                    | EnrollmentMetadata::Ended { generation, .. } => *generation,
                };
                *expected == generation
            }
        }
    }

    fn refusal(&self) -> CredentialStoreError {
        match self {
            Self::Replace { .. } => CredentialStoreError::Replace,
            Self::End { .. } => CredentialStoreError::Remove,
        }
    }
}

impl EventTag for EnrollmentRowEvent<'_> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Replace { .. } => "replace",
            Self::End { .. } => "end",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EnrollmentRowRefusal {
    ScopeMismatch,
    ReplacementNotPrepared,
    EndingGenerationExhausted,
}

impl RefusalTag for EnrollmentRowRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::ScopeMismatch => "scopeMismatch",
            Self::ReplacementNotPrepared => "replacementNotPrepared",
            Self::EndingGenerationExhausted => "endingGenerationExhausted",
        }
    }
}

struct EnrollmentRowMachine<'row>(std::marker::PhantomData<&'row ()>);

const TRANSITIONS: &[Transition] = &[
    Transition {
        from: "empty",
        event: "replace",
        to: "active",
        terminal: false,
    },
    Transition {
        from: "active",
        event: "replace",
        to: "active",
        terminal: false,
    },
    Transition {
        from: "ended",
        event: "replace",
        to: "active",
        terminal: false,
    },
    Transition {
        from: "active",
        event: "end",
        to: "ended",
        terminal: false,
    },
];

// Decision order for both writes. The first two rows still fence the SQL write
// even when the reducer refuses it, and SQL validates any proposed successor.
//
// | Locked input / event                   | Decision                         |
// |----------------------------------------|----------------------------------|
// | Corrupt row or corrupt domain snapshot | Report corruption                |
// | Valid snapshot, generation mismatch    | Superseded                       |
// | Current generation, refused event     | Report the typed refusal         |
// | Current generation, permitted event   | Validate and write the successor |
impl<'row> StateMachine for EnrollmentRowMachine<'row> {
    type State = EnrollmentRowPhase;
    type Event = EnrollmentRowEvent<'row>;
    type Effect = StoredEnrollmentRow;
    type Error = EnrollmentRowRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        if *state != event.phase() || !event.generation_matches() {
            return Err(TransitionError::Stale);
        }
        match (state, event) {
            (
                EnrollmentRowPhase::Empty | EnrollmentRowPhase::Active | EnrollmentRowPhase::Ended,
                EnrollmentRowEvent::Replace {
                    locked,
                    context,
                    prepared,
                    ..
                },
            ) => {
                if !locked.matches_scope(context) {
                    return Err(TransitionError::Domain(EnrollmentRowRefusal::ScopeMismatch));
                }
                let replacement = prepared
                    .filter(|replacement| *locked == replacement.witness)
                    .ok_or(TransitionError::Domain(
                        EnrollmentRowRefusal::ReplacementNotPrepared,
                    ))?;
                Ok((EnrollmentRowPhase::Active, vec![replacement.row]))
            }
            (
                EnrollmentRowPhase::Active,
                EnrollmentRowEvent::End {
                    locked:
                        EnrollmentMetadata::Active {
                            key,
                            deployment,
                            generation,
                        },
                    why,
                    ..
                },
            ) => {
                let generation = generation
                    .successor()
                    .and_then(|next| i64::try_from(next.get()).ok())
                    .ok_or(TransitionError::Domain(
                        EnrollmentRowRefusal::EndingGenerationExhausted,
                    ))?;
                Ok((
                    EnrollmentRowPhase::Ended,
                    vec![StoredEnrollmentRow::Ended {
                        generation,
                        organization_id: key.organization_id.to_string(),
                        deployment: deployment.origin().as_str().to_owned(),
                        enrollment_id: key.enrollment_id.to_string(),
                        why: stored_end(why),
                    }],
                ))
            }
            (
                EnrollmentRowPhase::Empty | EnrollmentRowPhase::Ended,
                EnrollmentRowEvent::End { .. },
            )
            | (
                EnrollmentRowPhase::Active,
                EnrollmentRowEvent::End {
                    locked: EnrollmentMetadata::Empty | EnrollmentMetadata::Ended { .. },
                    ..
                },
            ) => Err(TransitionError::NotAllowed {
                state: state.state_tag(),
                event: "end",
            }),
        }
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

pub(super) fn replace_locked(
    locked: Result<EnrollmentSnapshot, EnrollmentRowCorruption>,
    expected: BindingGeneration,
    context: &EnrollmentContext,
    prepared: Option<Box<PreparedReplacement>>,
) -> EnrollmentRowChange<CredentialStoreError> {
    match locked {
        Ok(locked) => decide(EnrollmentRowEvent::Replace {
            expected,
            locked: &locked,
            context,
            prepared,
        }),
        Err(reason) => EnrollmentRowChange::Corrupt(reason),
    }
}

pub(super) fn end_locked(
    locked: Result<EnrollmentMetadata, EnrollmentRowCorruption>,
    expected: BindingGeneration,
    why: EnrollmentEnd,
) -> EnrollmentRowChange<CredentialStoreError> {
    match locked {
        Ok(locked) => decide(EnrollmentRowEvent::End {
            expected,
            locked: &locked,
            why,
        }),
        Err(reason) => EnrollmentRowChange::Corrupt(reason),
    }
}

fn decide(event: EnrollmentRowEvent<'_>) -> EnrollmentRowChange<CredentialStoreError> {
    let refusal = event.refusal();
    match EnrollmentRowMachine::transition(&event.phase(), event) {
        Ok((_, mut rows)) => match rows.pop() {
            Some(row) => EnrollmentRowChange::Write(row),
            None => EnrollmentRowChange::Abort(refusal),
        },
        Err(TransitionError::Domain(EnrollmentRowRefusal::ScopeMismatch)) => {
            EnrollmentRowChange::Abort(CredentialStoreError::EnrollmentScopeMismatch)
        }
        // The enclosing transaction checks the same locked generation before
        // returning an abort, and reports Superseded on mismatch.
        Err(
            TransitionError::Stale
            | TransitionError::NotAllowed { .. }
            | TransitionError::Domain(
                EnrollmentRowRefusal::ReplacementNotPrepared
                | EnrollmentRowRefusal::EndingGenerationExhausted,
            ),
        ) => EnrollmentRowChange::Abort(refusal),
    }
}

#[cfg(test)]
#[path = "enrollment_row_machine_test.rs"]
mod tests;
