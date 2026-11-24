//! Durable save and readback of a grant already approved by sign-in.
#![deny(clippy::wildcard_enum_match_arm)]

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::{
    BindingGeneration, ConditionalWrite, CredentialStore, CredentialStoreError, EnrollmentBinding,
    EnrollmentContext, EnrollmentKey, EnrollmentReceipt, RefreshCredential, StoredEnrollment,
};

/// Sign-in approved the attempt, but the Mac cannot report a durable save.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ApprovedEnrollmentSaveFailure {
    #[error("sign-in approved the request, but the local save failed")]
    NotSaved(#[source] EnrollmentSaveFailure),
    #[error("sign-in approved the request, but the local save outcome is unknown")]
    OutcomeUnknown(#[source] EnrollmentSaveFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnrollmentSaveFailure {
    #[error(transparent)]
    Store(#[from] CredentialStoreError),
    #[error("another enrollment replaced the preceding binding")]
    ReplacementConflict,
    #[error("the enrollment generation cannot be advanced")]
    GenerationExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitKnowledge {
    Known,
    PossiblyCommitted,
}

impl CommitKnowledge {
    fn failure(self, cause: EnrollmentSaveFailure) -> SaveState {
        SaveState::Failed(match self {
            Self::Known => ApprovedEnrollmentSaveFailure::NotSaved(cause),
            Self::PossiblyCommitted => ApprovedEnrollmentSaveFailure::OutcomeUnknown(cause),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SaveAttempt {
    expected: BindingGeneration,
    replacement: BindingGeneration,
    knowledge: CommitKnowledge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadBack {
    Superseded,
    UnknownWrite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SaveState {
    Write(SaveAttempt),
    Reconcile {
        attempt: SaveAttempt,
        cause: ReadBack,
    },
    Saved(BindingGeneration),
    Failed(ApprovedEnrollmentSaveFailure),
}

impl StateTag for SaveState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Write(_) => "write",
            Self::Reconcile { .. } => "reconcile",
            Self::Saved(_) => "saved",
            Self::Failed(_) => "failed",
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self, Self::Saved(_) | Self::Failed(_))
    }
}

#[derive(Clone, Copy)]
enum SaveEvent<'read> {
    Wrote(Result<ConditionalWrite, CredentialStoreError>),
    Observed {
        current: Result<&'read StoredEnrollment, CredentialStoreError>,
        original_key: Option<&'read EnrollmentKey>,
        submitted: &'read EnrollmentBinding,
    },
}

impl EventTag for SaveEvent<'_> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Wrote(_) => "wrote",
            Self::Observed { .. } => "observed",
        }
    }
}

#[derive(Debug)]
enum SaveRefusal {}

impl RefusalTag for SaveRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

struct ApprovedEnrollmentSaveMachine<'read>(std::marker::PhantomData<&'read ()>);

const TRANSITIONS: &[Transition] = &[
    edge("write", "wrote", "saved", true),
    edge("write", "wrote", "reconcile", false),
    edge("write", "wrote", "failed", true),
    edge("reconcile", "observed", "saved", true),
    edge("reconcile", "observed", "write", false),
    edge("reconcile", "observed", "failed", true),
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

impl<'read> StateMachine for ApprovedEnrollmentSaveMachine<'read> {
    type State = SaveState;
    type Event = SaveEvent<'read>;
    type Effect = ();
    type Error = SaveRefusal;

    fn transition(state: &SaveState, event: Self::Event) -> StateMachineResult<Self> {
        let next = match (*state, event) {
            (SaveState::Write(attempt), SaveEvent::Wrote(result)) => match result {
                Ok(ConditionalWrite::Applied) => SaveState::Saved(attempt.replacement),
                Ok(ConditionalWrite::Superseded) => SaveState::Reconcile {
                    attempt,
                    cause: ReadBack::Superseded,
                },
                Err(CredentialStoreError::OutcomeNotKnown) => SaveState::Reconcile {
                    attempt,
                    cause: ReadBack::UnknownWrite,
                },
                Err(
                    error @ (CredentialStoreError::Read
                    | CredentialStoreError::Replace
                    | CredentialStoreError::EnrollmentScopeMismatch
                    | CredentialStoreError::Remove
                    | CredentialStoreError::CleanupTimedOut),
                ) => attempt.knowledge.failure(error.into()),
            },
            (
                SaveState::Reconcile { attempt, cause },
                SaveEvent::Observed {
                    current,
                    original_key,
                    submitted,
                },
            ) => reconcile(attempt, cause, current, original_key, submitted),
            (
                SaveState::Saved(_) | SaveState::Failed(_),
                SaveEvent::Wrote(_) | SaveEvent::Observed { .. },
            ) => {
                return Err(TransitionError::Stale);
            }
            (SaveState::Write(_), SaveEvent::Observed { .. })
            | (SaveState::Reconcile { .. }, SaveEvent::Wrote(_)) => {
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

fn reconcile(
    mut attempt: SaveAttempt,
    cause: ReadBack,
    current: Result<&StoredEnrollment, CredentialStoreError>,
    original_key: Option<&EnrollmentKey>,
    submitted: &EnrollmentBinding,
) -> SaveState {
    let unknown_write = cause == ReadBack::UnknownWrite;
    let current = match current {
        Ok(current) => current,
        Err(error) => {
            if unknown_write {
                attempt.knowledge = CommitKnowledge::PossiblyCommitted;
            }
            return attempt.knowledge.failure(error.into());
        }
    };
    let observed = current.generation();
    if unknown_write {
        if matches!(current, StoredEnrollment::Active(stored) if submitted.matches_replacement(stored))
        {
            return SaveState::Saved(attempt.replacement);
        }
        if observed == attempt.expected {
            return SaveState::Write(attempt);
        }
        // A later generation can hide a write that committed and was replaced.
        if observed > attempt.replacement {
            attempt.knowledge = CommitKnowledge::PossiblyCommitted;
        }
    }
    if current.key().as_ref() != original_key || observed <= attempt.expected {
        return attempt
            .knowledge
            .failure(EnrollmentSaveFailure::ReplacementConflict);
    }
    let Some(replacement) = observed.binding_successor() else {
        return attempt
            .knowledge
            .failure(EnrollmentSaveFailure::GenerationExhausted);
    };
    SaveState::Write(SaveAttempt {
        expected: observed,
        replacement,
        ..attempt
    })
}

pub(super) async fn install_approved_binding(
    store: &impl CredentialStore,
    before: &StoredEnrollment,
    context: &EnrollmentContext,
    credential: RefreshCredential,
) -> Result<EnrollmentReceipt, ApprovedEnrollmentSaveFailure> {
    let original_key = before.key();
    let expected = before.generation();
    let replacement =
        expected
            .binding_successor()
            .ok_or(ApprovedEnrollmentSaveFailure::NotSaved(
                EnrollmentSaveFailure::GenerationExhausted,
            ))?;
    let mut binding = EnrollmentBinding {
        generation: replacement,
        context: context.clone(),
        credential,
    };
    let mut state = SaveState::Write(SaveAttempt {
        expected,
        replacement,
        knowledge: CommitKnowledge::Known,
    });

    loop {
        let next = match state {
            SaveState::Write(attempt) => {
                binding.generation = attempt.replacement;
                ApprovedEnrollmentSaveMachine::transition(
                    &state,
                    SaveEvent::Wrote(store.replace(attempt.expected, &binding).await),
                )
            }
            SaveState::Reconcile { .. } => {
                let current = store.read().await;
                ApprovedEnrollmentSaveMachine::transition(
                    &state,
                    SaveEvent::Observed {
                        current: current.as_ref().map_err(|error| *error),
                        original_key: original_key.as_ref(),
                        submitted: &binding,
                    },
                )
            }
            SaveState::Saved(generation) => {
                return Ok(EnrollmentReceipt {
                    generation,
                    enrollment_id: binding.credential.enrollment_id,
                });
            }
            SaveState::Failed(failure) => return Err(failure),
        };
        // Each driver branch supplies the event declared for that state.
        state = next
            .map_err(|_| {
                ApprovedEnrollmentSaveFailure::OutcomeUnknown(EnrollmentSaveFailure::Store(
                    CredentialStoreError::OutcomeNotKnown,
                ))
            })?
            .0;
    }
}

#[cfg(test)]
#[path = "approved_enrollment_save_test.rs"]
mod tests;
