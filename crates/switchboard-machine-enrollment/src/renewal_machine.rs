//! Pure renewal decisions. The owner retains the shared future and secret leases.
#![deny(clippy::wildcard_enum_match_arm)]

use std::time::Duration;

use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::{
    AccessRequest, BindingGeneration, ConditionalWrite, CredentialStoreError, EnrollmentBinding,
    EnrollmentEnd, EnrollmentId, InvalidReason, KeyedEnrollment, LeaseConfigurationFault,
    LeaseUnavailable, RefreshAnswer, StoredEnrollment,
    renewal_admission::{
        Admission, LeaseAdmission, LeaseSettlement, RenewalFacts, Settlement, admit_binding,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlightOutcome {
    Committed,
    Unavailable(Option<Duration>),
    Invalid(LeaseConfigurationFault),
    OversizedReply,
    OutcomeNotKnown,
    Recheck,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FlightPhase {
    Dispatch,
    Requesting,
    Writing,
    Reconciling,
    BackingOff,
    Retiring(EnrollmentEnd),
    Finished(FlightOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WriteAdmission {
    Admitted,
    FencedOrStale,
    BudgetElapsed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RenewalState {
    Owner(RenewalFacts),
    Flight(FlightPhase),
}

impl StateTag for RenewalState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::Owner(_) => "owner",
            Self::Flight(phase) => match phase {
                FlightPhase::Dispatch => "dispatch",
                FlightPhase::Requesting => "requesting",
                FlightPhase::Writing => "writing",
                FlightPhase::Reconciling => "reconciling",
                FlightPhase::BackingOff => "backingOff",
                FlightPhase::Retiring(_) => "retiring",
                FlightPhase::Finished(_) => "finished",
            },
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Flight(FlightPhase::Retiring(_) | FlightPhase::Finished(_))
        )
    }
}

#[derive(Clone, Copy)]
pub(super) enum RenewalEvent<'observation> {
    BindingChecked {
        ending: Option<EnrollmentEnd>,
        generation: BindingGeneration,
        request: AccessRequest,
    },
    LeaseRequested(LeaseAdmission),
    LeaseSettled(LeaseSettlement),
    DispatchChecked {
        fenced: bool,
    },
    RemoteReturned {
        expected: EnrollmentId,
        answer: &'observation RefreshAnswer,
    },
    WriteAdmitted(WriteAdmission),
    WriteReturned(Option<Result<ConditionalWrite, CredentialStoreError>>),
    ReconciliationRead {
        observed: Option<&'observation KeyedEnrollment>,
        previous: BindingGeneration,
        replacement: &'observation EnrollmentBinding,
    },
    RetryElapsed,
}

impl EventTag for RenewalEvent<'_> {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::BindingChecked { .. } => "bindingChecked",
            Self::LeaseRequested(_) => "leaseRequested",
            Self::LeaseSettled(_) => "leaseSettled",
            Self::DispatchChecked { .. } => "dispatchChecked",
            Self::RemoteReturned { .. } => "remoteReturned",
            Self::WriteAdmitted(_) => "writeAdmitted",
            Self::WriteReturned(_) => "writeReturned",
            Self::ReconciliationRead { .. } => "reconciliationRead",
            Self::RetryElapsed => "retryElapsed",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum RenewalEffect {
    BindingAdmission(Result<(), LeaseUnavailable>),
    Admission(Admission),
    Settlement(Settlement),
}

#[derive(Debug)]
pub(super) enum RenewalRefusal {}

impl RefusalTag for RenewalRefusal {
    fn refusal_tag(&self) -> &'static str {
        match *self {}
    }
}

pub(super) struct RenewalMachine<'observation>(std::marker::PhantomData<&'observation ()>);

const TRANSITIONS: &[Transition] = &[
    edge("owner", "bindingChecked", "owner", false),
    edge("owner", "leaseRequested", "owner", false),
    edge("owner", "leaseSettled", "owner", false),
    edge("dispatch", "dispatchChecked", "requesting", false),
    edge("dispatch", "dispatchChecked", "finished", true),
    edge("requesting", "remoteReturned", "writing", false),
    edge("requesting", "remoteReturned", "retiring", true),
    edge("requesting", "remoteReturned", "finished", true),
    edge("writing", "writeAdmitted", "writing", false),
    edge("writing", "writeAdmitted", "finished", true),
    edge("writing", "writeAdmitted", "retiring", true),
    edge("writing", "writeReturned", "finished", true),
    edge("writing", "writeReturned", "reconciling", false),
    edge("writing", "writeReturned", "backingOff", false),
    edge("reconciling", "reconciliationRead", "finished", true),
    edge("reconciling", "reconciliationRead", "backingOff", false),
    edge("backingOff", "retryElapsed", "writing", false),
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

impl<'observation> StateMachine for RenewalMachine<'observation> {
    type State = RenewalState;
    type Event = RenewalEvent<'observation>;
    type Effect = RenewalEffect;
    type Error = RenewalRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use FlightPhase as Phase;
        use RenewalEvent as Event;
        use RenewalState as State;

        let phase = match (*state, event) {
            (
                State::Owner(_),
                Event::BindingChecked {
                    ending,
                    generation,
                    request,
                },
            ) => {
                return Ok((
                    *state,
                    vec![RenewalEffect::BindingAdmission(admit_binding(
                        ending, generation, request,
                    ))],
                ));
            }
            (State::Owner(facts), Event::LeaseRequested(request)) => {
                return Ok((*state, vec![RenewalEffect::Admission(facts.admit(request))]));
            }
            (State::Owner(mut facts), Event::LeaseSettled(result)) => {
                let settlement = facts.settle(result);
                return Ok((
                    State::Owner(facts),
                    vec![RenewalEffect::Settlement(settlement)],
                ));
            }
            (State::Flight(Phase::Dispatch), Event::DispatchChecked { fenced }) => {
                if fenced {
                    Phase::Finished(FlightOutcome::Recheck)
                } else {
                    Phase::Requesting
                }
            }
            (State::Flight(Phase::Requesting), Event::RemoteReturned { expected, answer }) => {
                remote_result(expected, answer)
            }
            (State::Flight(Phase::Writing), Event::WriteAdmitted(admission)) => match admission {
                WriteAdmission::Admitted => Phase::Writing,
                WriteAdmission::FencedOrStale => Phase::Finished(FlightOutcome::Recheck),
                WriteAdmission::BudgetElapsed => {
                    Phase::Retiring(EnrollmentEnd::LocalCredentialLost)
                }
            },
            (State::Flight(Phase::Writing), Event::WriteReturned(result)) => match result {
                Some(Ok(ConditionalWrite::Applied)) => Phase::Finished(FlightOutcome::Committed),
                Some(Ok(ConditionalWrite::Superseded)) => Phase::Finished(FlightOutcome::Recheck),
                None | Some(Err(CredentialStoreError::OutcomeNotKnown)) => Phase::Reconciling,
                Some(Err(_)) => Phase::BackingOff,
            },
            (
                State::Flight(Phase::Reconciling),
                Event::ReconciliationRead {
                    observed,
                    previous,
                    replacement,
                },
            ) => reconcile(observed, previous, replacement),
            (State::Flight(Phase::BackingOff), Event::RetryElapsed) => Phase::Writing,
            (
                State::Flight(Phase::Retiring(_) | Phase::Finished(_)),
                Event::BindingChecked { .. }
                | Event::LeaseRequested(_)
                | Event::LeaseSettled(_)
                | Event::DispatchChecked { .. }
                | Event::RemoteReturned { .. }
                | Event::WriteAdmitted(_)
                | Event::WriteReturned(_)
                | Event::ReconciliationRead { .. }
                | Event::RetryElapsed,
            ) => return Err(TransitionError::Stale),
            (
                State::Owner(_),
                Event::DispatchChecked { .. }
                | Event::RemoteReturned { .. }
                | Event::WriteAdmitted(_)
                | Event::WriteReturned(_)
                | Event::ReconciliationRead { .. }
                | Event::RetryElapsed,
            )
            | (
                State::Flight(Phase::Dispatch),
                Event::BindingChecked { .. }
                | Event::LeaseRequested(_)
                | Event::LeaseSettled(_)
                | Event::RemoteReturned { .. }
                | Event::WriteAdmitted(_)
                | Event::WriteReturned(_)
                | Event::ReconciliationRead { .. }
                | Event::RetryElapsed,
            )
            | (
                State::Flight(Phase::Requesting),
                Event::BindingChecked { .. }
                | Event::LeaseRequested(_)
                | Event::LeaseSettled(_)
                | Event::DispatchChecked { .. }
                | Event::WriteAdmitted(_)
                | Event::WriteReturned(_)
                | Event::ReconciliationRead { .. }
                | Event::RetryElapsed,
            )
            | (
                State::Flight(Phase::Writing),
                Event::BindingChecked { .. }
                | Event::LeaseRequested(_)
                | Event::LeaseSettled(_)
                | Event::DispatchChecked { .. }
                | Event::RemoteReturned { .. }
                | Event::ReconciliationRead { .. }
                | Event::RetryElapsed,
            )
            | (
                State::Flight(Phase::Reconciling),
                Event::BindingChecked { .. }
                | Event::LeaseRequested(_)
                | Event::LeaseSettled(_)
                | Event::DispatchChecked { .. }
                | Event::RemoteReturned { .. }
                | Event::WriteAdmitted(_)
                | Event::WriteReturned(_)
                | Event::RetryElapsed,
            )
            | (
                State::Flight(Phase::BackingOff),
                Event::BindingChecked { .. }
                | Event::LeaseRequested(_)
                | Event::LeaseSettled(_)
                | Event::DispatchChecked { .. }
                | Event::RemoteReturned { .. }
                | Event::WriteAdmitted(_)
                | Event::WriteReturned(_)
                | Event::ReconciliationRead { .. },
            ) => {
                return Err(TransitionError::NotAllowed {
                    state: state.state_tag(),
                    event: event.event_tag(),
                });
            }
        };
        Ok((State::Flight(phase), Vec::new()))
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn remote_result(expected: EnrollmentId, answer: &RefreshAnswer) -> FlightPhase {
    use FlightPhase as Phase;

    match answer {
        RefreshAnswer::Rotated(grant) if grant.credential.enrollment_id == expected => {
            Phase::Writing
        }
        RefreshAnswer::Rotated(_) => Phase::Finished(FlightOutcome::Invalid(
            LeaseConfigurationFault::InvalidAnswer(InvalidReason::WrongEnrollment),
        )),
        RefreshAnswer::OversizedReply => Phase::Finished(FlightOutcome::OversizedReply),
        RefreshAnswer::Unavailable { retry_after } => {
            Phase::Finished(FlightOutcome::Unavailable(*retry_after))
        }
        RefreshAnswer::Refused {
            reason: InvalidReason::GrantRefused,
        } => Phase::Retiring(EnrollmentEnd::RefreshRefused),
        RefreshAnswer::Refused {
            reason: InvalidReason::InvalidClient,
        } => Phase::Retiring(EnrollmentEnd::RefreshClientDisabled),
        RefreshAnswer::Refused { reason } => Phase::Finished(FlightOutcome::Invalid(
            LeaseConfigurationFault::InvalidAnswer(*reason),
        )),
    }
}

fn reconcile(
    observed: Option<&KeyedEnrollment>,
    previous: BindingGeneration,
    replacement: &EnrollmentBinding,
) -> FlightPhase {
    use FlightPhase as Phase;

    match observed {
        Some(KeyedEnrollment::Snapshot(StoredEnrollment::Active(stored)))
            if replacement.matches_replacement(stored) =>
        {
            Phase::Finished(FlightOutcome::Committed)
        }
        Some(KeyedEnrollment::Snapshot(stored)) if stored.generation() == previous => {
            Phase::BackingOff
        }
        Some(KeyedEnrollment::Snapshot(_) | KeyedEnrollment::DifferentEnrollment) => {
            Phase::Finished(FlightOutcome::Recheck)
        }
        None => Phase::Finished(FlightOutcome::OutcomeNotKnown),
    }
}

pub(super) fn advance_flight(phase: FlightPhase, event: RenewalEvent<'_>) -> FlightPhase {
    match RenewalMachine::transition(&RenewalState::Flight(phase), event) {
        Ok((RenewalState::Flight(next), _)) => next,
        // The driver feeds only the event declared for each phase.
        Ok((RenewalState::Owner(_), _)) | Err(_) => FlightPhase::Finished(FlightOutcome::Recheck),
    }
}

#[cfg(test)]
#[path = "renewal_machine_test.rs"]
mod tests;
