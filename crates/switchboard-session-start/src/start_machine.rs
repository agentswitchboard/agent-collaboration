//! Make a harness conversation once, register it, then release its held owner.

use fleet_identity_protocol::CreateSessionAttempt;
use state_machine_core::{StateMachine, StateMachineResult, Transition, TransitionError};
use workspace_ids::SessionId;

use crate::{
    BindObservation, CentralCloseProgress, CentralEndPurpose, CentralEndUnavailable, ClosingReason,
    CreateObservation, EndCallCertainty, LaunchProgress, MakePlan, ObservedSettlement,
    RegistrationContradiction, RegistrationProgress, SealedSessionSecretRef, StartEffect,
    StartEvent, StartProgress, StartReadiness, StartRefusal, StartSnapshot, StartTransitionRefusal,
};

use super::{
    make_transitions::{advance_make, first_input_answered, launched, retire},
    start_cleanup::{begin_closing, central_ended, harness_closed, resume_closing},
    start_events::{MachineResult, move_to, wrong_phase},
    start_transition_table::TRANSITIONS,
};

/// Pure lifecycle policy shared by the ACP and native harness drivers.
pub struct StartMachine;

impl StateMachine for StartMachine {
    type State = StartSnapshot;
    type Event = StartEvent;
    type Effect = StartEffect;
    type Error = StartTransitionRefusal;

    fn transition(snapshot: &StartSnapshot, event: StartEvent) -> StateMachineResult<Self> {
        match event {
            StartEvent::Advance(readiness) => advance(snapshot, readiness, false),
            StartEvent::Recover(readiness) => advance(snapshot, readiness, true),
            StartEvent::Launched(observation) => launched(snapshot, observation),
            StartEvent::FirstInputAnswered {
                harness,
                input,
                observation,
            } => first_input_answered(snapshot, harness, input, observation),
            StartEvent::CreateAnswered {
                attempt,
                observation,
            } => created(snapshot, attempt, observation),
            StartEvent::BindAnswered { issue, observation } => bound(snapshot, &issue, observation),
            StartEvent::EndForGood => end_open(snapshot),
            StartEvent::HarnessCloseAnswered(observation) => harness_closed(snapshot, observation),
            StartEvent::CentralEndAnswered {
                target,
                observation,
            } => central_ended(snapshot, &target, observation),
        }
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn advance(snapshot: &StartSnapshot, readiness: StartReadiness, recovered: bool) -> MachineResult {
    match snapshot.progress() {
        StartProgress::Asked
            if recovered && matches!(snapshot.make_plan(), MakePlan::FirstInputRequired(_)) =>
        {
            refuse_start(snapshot, StartRefusal::InitialInputUnavailable)
        }
        StartProgress::Asked => match readiness {
            StartReadiness::Ready => move_to(
                snapshot,
                StartProgress::Launching(LaunchProgress::Intent),
                vec![StartEffect::LaunchHeld],
            ),
            StartReadiness::LinkUnavailable => {
                refuse_start(snapshot, StartRefusal::LinkUnavailable)
            }
            StartReadiness::EnrollmentEnded => {
                refuse_start(snapshot, StartRefusal::EnrollmentEnded)
            }
        },
        StartProgress::Launching(progress) => {
            advance_make(snapshot, progress, readiness, recovered)
        }
        StartProgress::Registering {
            harness,
            registration,
        } => match readiness {
            StartReadiness::Ready => register(snapshot, harness, registration),
            StartReadiness::LinkUnavailable => wait(snapshot, StartEffect::AwaitReadiness),
            StartReadiness::EnrollmentEnded => {
                close_on_enrollment_end(snapshot, harness, registration)
            }
        },
        StartProgress::Open { harness, secret } => match readiness {
            StartReadiness::Ready => wait(snapshot, StartEffect::RestoreExact(harness.clone())),
            StartReadiness::LinkUnavailable => wait(snapshot, StartEffect::AwaitReadiness),
            StartReadiness::EnrollmentEnded => begin_closing(
                snapshot,
                harness,
                ClosingReason::EnrollmentEnded,
                unavailable_end(secret),
            ),
        },
        StartProgress::Closing(closing) => resume_closing(snapshot, closing, readiness),
        StartProgress::Ended(_) | StartProgress::Refused(_) | StartProgress::LaunchUnknown(_) => {
            Ok((snapshot.clone(), Vec::new()))
        }
    }
}

fn register(
    snapshot: &StartSnapshot,
    harness: &SessionId,
    step: &RegistrationProgress,
) -> MachineResult {
    let attempt = match step {
        RegistrationProgress::NeedCreate => CreateSessionAttempt::try_from(1)
            .map_err(|_| TransitionError::Domain(StartTransitionRefusal::InvalidAttempt))?,
        RegistrationProgress::CreateIntended(previous) => {
            let Some(next) = previous.checked_next() else {
                return block_registration(
                    snapshot,
                    harness,
                    RegistrationContradiction::AttemptLimit,
                );
            };
            next
        }
        RegistrationProgress::BindIntended(issued) => {
            return wait(snapshot, StartEffect::BindCentral(issued.clone()));
        }
        RegistrationProgress::Blocked(_) => return wait(snapshot, StartEffect::ReportOutcome),
    };
    move_to(
        snapshot,
        StartProgress::Registering {
            harness: harness.clone(),
            registration: RegistrationProgress::CreateIntended(attempt),
        },
        vec![StartEffect::CreateCentral(attempt)],
    )
}

fn created(
    snapshot: &StartSnapshot,
    attempt: CreateSessionAttempt,
    observation: CreateObservation,
) -> MachineResult {
    let StartProgress::Registering {
        harness,
        registration: RegistrationProgress::CreateIntended(expected),
    } = snapshot.progress()
    else {
        return wrong_phase();
    };
    if *expected != attempt {
        return Err(TransitionError::Stale);
    }
    match observation {
        CreateObservation::Reserved(issued) => {
            if issued.secret.attempt != attempt {
                return Err(TransitionError::Stale);
            }
            move_to(
                snapshot,
                StartProgress::Registering {
                    harness: harness.clone(),
                    registration: RegistrationProgress::BindIntended(issued.clone()),
                },
                vec![StartEffect::BindCentral(issued)],
            )
        }
        CreateObservation::Active {
            central_id,
            address,
        } => block_registration(
            snapshot,
            harness,
            RegistrationContradiction::UnexpectedActive {
                central_id,
                address,
            },
        ),
        CreateObservation::Superseded
        | CreateObservation::AnswerLost
        | CreateObservation::NotSent => wait(snapshot, StartEffect::RetryAfterBackoff),
        CreateObservation::Refused(reason) => begin_closing(
            snapshot,
            harness,
            ClosingReason::CreateRefused(reason),
            CentralCloseProgress::NotOwed,
        ),
        CreateObservation::Settled(settlement) => begin_closing(
            snapshot,
            harness,
            ClosingReason::RegistrationSettled,
            CentralCloseProgress::SettledWithoutIssuingHere(settlement),
        ),
        CreateObservation::Contradicted => {
            block_registration(snapshot, harness, RegistrationContradiction::CreateReply)
        }
    }
}

fn bound(
    snapshot: &StartSnapshot,
    issue: &SealedSessionSecretRef,
    observation: BindObservation,
) -> MachineResult {
    let StartProgress::Registering {
        harness,
        registration: RegistrationProgress::BindIntended(issued),
    } = snapshot.progress()
    else {
        return wrong_phase();
    };
    if &issued.secret != issue {
        return Err(TransitionError::Stale);
    }
    match observation {
        BindObservation::Active(address)
            if address.machine_id == snapshot.scope().machine && &address.session_id == harness =>
        {
            move_to(
                snapshot,
                StartProgress::Open {
                    harness: harness.clone(),
                    secret: issue.clone(),
                },
                vec![StartEffect::PublishHeld(harness.clone())],
            )
        }
        BindObservation::Active(address) => block_registration(
            snapshot,
            harness,
            RegistrationContradiction::BindAddress {
                issued: issued.clone(),
                address,
            },
        ),
        BindObservation::Retry => wait(snapshot, StartEffect::RetryAfterBackoff),
        BindObservation::Refused(reason) => begin_closing(
            snapshot,
            harness,
            ClosingReason::BindRefused(reason),
            owed_end(issue, CentralEndPurpose::AbandonReservation),
        ),
        BindObservation::Settled(settlement) => begin_closing(
            snapshot,
            harness,
            ClosingReason::RegistrationSettled,
            CentralCloseProgress::Settled {
                secret: issue.clone(),
                settlement: ObservedSettlement::Existing(settlement),
            },
        ),
        BindObservation::Contradicted => block_registration(
            snapshot,
            harness,
            RegistrationContradiction::BindReply {
                issued: issued.clone(),
            },
        ),
    }
}

fn end_open(snapshot: &StartSnapshot) -> MachineResult {
    let StartProgress::Open { harness, secret } = snapshot.progress() else {
        return wrong_phase();
    };
    begin_closing(
        snapshot,
        harness,
        ClosingReason::EndedForGood,
        owed_end(secret, CentralEndPurpose::EndBoundSession),
    )
}

fn close_on_enrollment_end(
    snapshot: &StartSnapshot,
    harness: &SessionId,
    registration: &RegistrationProgress,
) -> MachineResult {
    let central = match registration {
        RegistrationProgress::BindIntended(issued)
        | RegistrationProgress::Blocked(
            RegistrationContradiction::BindAddress { issued, .. }
            | RegistrationContradiction::BindReply { issued },
        ) => unavailable_end(&issued.secret),
        RegistrationProgress::NeedCreate => CentralCloseProgress::NotOwed,
        RegistrationProgress::CreateIntended(_)
        | RegistrationProgress::Blocked(
            RegistrationContradiction::UnexpectedActive { .. }
            | RegistrationContradiction::CreateReply
            | RegistrationContradiction::AttemptLimit,
        ) => CentralCloseProgress::UnverifiedCreate,
    };
    begin_closing(snapshot, harness, ClosingReason::EnrollmentEnded, central)
}

fn owed_end(secret: &SealedSessionSecretRef, purpose: CentralEndPurpose) -> CentralCloseProgress {
    CentralCloseProgress::Owed {
        secret: secret.clone(),
        purpose,
        certainty: EndCallCertainty::Pending,
    }
}

fn unavailable_end(secret: &SealedSessionSecretRef) -> CentralCloseProgress {
    CentralCloseProgress::Unavailable {
        secret: secret.clone(),
        reason: CentralEndUnavailable::EnrollmentEnded,
    }
}

fn block_registration(
    snapshot: &StartSnapshot,
    harness: &SessionId,
    reason: RegistrationContradiction,
) -> MachineResult {
    move_to(
        snapshot,
        StartProgress::Registering {
            harness: harness.clone(),
            registration: RegistrationProgress::Blocked(reason),
        },
        vec![StartEffect::ReportOutcome],
    )
}

fn refuse_start(snapshot: &StartSnapshot, refusal: StartRefusal) -> MachineResult {
    retire(snapshot, StartProgress::Refused(refusal))
}

fn wait(snapshot: &StartSnapshot, effect: StartEffect) -> MachineResult {
    Ok((snapshot.clone(), vec![effect]))
}
