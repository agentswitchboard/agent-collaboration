//! Exactly one harness allocation and, where required, one private initial input.

use state_machine_core::TransitionError;
use workspace_ids::SessionId;

use crate::{
    CentralCloseProgress, ClosingReason, DurableFirstInput, EndOutcome, FirstInputIntent,
    FirstInputObservation, HarnessLaunchObservation, LaunchProgress, LaunchUncertainty, MakePlan,
    RegistrationProgress, StartEffect, StartProgress, StartReadiness, StartRefusal, StartSnapshot,
};

use super::{
    start_cleanup::begin_closing,
    start_events::{MachineResult, move_to, snapshot_refusal, wrong_phase},
};

pub(super) fn advance_make(
    snapshot: &StartSnapshot,
    progress: &LaunchProgress,
    readiness: StartReadiness,
    recovered: bool,
) -> MachineResult {
    match progress {
        LaunchProgress::Intent if recovered => retire(
            snapshot,
            StartProgress::LaunchUnknown(LaunchUncertainty::RouterRestarted),
        ),
        LaunchProgress::Intent => wrong_phase(),
        LaunchProgress::Allocated { harness } if recovered => retire(
            snapshot,
            StartProgress::LaunchUnknown(LaunchUncertainty::RecoveredAllocation {
                harness: harness.clone(),
            }),
        ),
        LaunchProgress::Allocated { harness } => match readiness {
            StartReadiness::Ready => intend_first_input(snapshot, harness),
            StartReadiness::LinkUnavailable => {
                Ok((snapshot.clone(), vec![StartEffect::AwaitReadiness]))
            }
            StartReadiness::EnrollmentEnded => {
                close_make(snapshot, harness, ClosingReason::EnrollmentEnded)
            }
        },
        LaunchProgress::FirstInputIntended { harness, input } => match readiness {
            StartReadiness::Ready | StartReadiness::LinkUnavailable => {
                Ok((snapshot.clone(), vec![verify_input(harness, input)]))
            }
            StartReadiness::EnrollmentEnded => {
                close_make(snapshot, harness, ClosingReason::EnrollmentEnded)
            }
        },
    }
}

pub(super) fn launched(
    snapshot: &StartSnapshot,
    observation: HarnessLaunchObservation,
) -> MachineResult {
    if !matches!(
        snapshot.progress(),
        StartProgress::Launching(LaunchProgress::Intent)
    ) {
        return wrong_phase();
    }
    match observation {
        HarnessLaunchObservation::Held(harness)
            if snapshot.make_plan() == &MakePlan::HeldOnLaunch =>
        {
            made(snapshot, harness, None)
        }
        HarnessLaunchObservation::Allocated(harness)
            if matches!(snapshot.make_plan(), MakePlan::FirstInputRequired(_)) =>
        {
            move_to(
                snapshot,
                StartProgress::Launching(LaunchProgress::Allocated { harness }),
                vec![StartEffect::AwaitReadiness],
            )
        }
        HarnessLaunchObservation::Held(_) | HarnessLaunchObservation::Allocated(_) => wrong_phase(),
        HarnessLaunchObservation::SetupFailed(harness) => {
            close_make(snapshot, &harness, ClosingReason::SetupFailed)
        }
        HarnessLaunchObservation::ProviderIdCollision(reported) => retire(
            snapshot,
            StartProgress::Ended(EndOutcome::ProviderIdCollision { reported }),
        ),
        HarnessLaunchObservation::Rejected => {
            retire(snapshot, StartProgress::Ended(EndOutcome::LaunchRejected))
        }
        HarnessLaunchObservation::ProvenNotSent => retire(
            snapshot,
            StartProgress::Refused(StartRefusal::HarnessNotSent),
        ),
        HarnessLaunchObservation::AnswerLost => retire(
            snapshot,
            StartProgress::LaunchUnknown(LaunchUncertainty::AnswerLost),
        ),
    }
}

pub(super) fn first_input_answered(
    snapshot: &StartSnapshot,
    harness: SessionId,
    input: FirstInputIntent,
    observation: FirstInputObservation,
) -> MachineResult {
    let StartProgress::Launching(LaunchProgress::FirstInputIntended {
        harness: expected_harness,
        input: expected_input,
    }) = snapshot.progress()
    else {
        return wrong_phase();
    };
    if &harness != expected_harness || &input != expected_input {
        return Err(TransitionError::Stale);
    }
    match observation {
        FirstInputObservation::DurablyTaken => made(
            snapshot,
            harness.clone(),
            Some(DurableFirstInput { harness, input }),
        ),
        FirstInputObservation::NeedsRead => {
            Ok((snapshot.clone(), vec![verify_input(&harness, &input)]))
        }
        FirstInputObservation::Unconfirmed => retire(
            snapshot,
            StartProgress::LaunchUnknown(LaunchUncertainty::FirstInputUnconfirmed {
                harness,
                input,
            }),
        ),
        FirstInputObservation::SetupFailed => {
            close_make(snapshot, &harness, ClosingReason::SetupFailed)
        }
    }
}

fn intend_first_input(snapshot: &StartSnapshot, harness: &SessionId) -> MachineResult {
    let MakePlan::FirstInputRequired(input) = snapshot.make_plan() else {
        return wrong_phase();
    };
    move_to(
        snapshot,
        StartProgress::Launching(LaunchProgress::FirstInputIntended {
            harness: harness.clone(),
            input: input.clone(),
        }),
        vec![StartEffect::SubmitFirstInput {
            harness: harness.clone(),
            input: input.clone(),
        }],
    )
}

fn verify_input(harness: &SessionId, input: &FirstInputIntent) -> StartEffect {
    StartEffect::VerifyFirstInput {
        harness: harness.clone(),
        input: input.clone(),
    }
}

fn made(
    snapshot: &StartSnapshot,
    harness: SessionId,
    proof: Option<DurableFirstInput>,
) -> MachineResult {
    let next = snapshot
        .advance_with_input(
            StartProgress::Registering {
                harness,
                registration: RegistrationProgress::NeedCreate,
            },
            proof,
        )
        .map_err(snapshot_refusal)?;
    Ok((next, vec![StartEffect::AwaitReadiness]))
}

fn close_make(
    snapshot: &StartSnapshot,
    harness: &SessionId,
    reason: ClosingReason,
) -> MachineResult {
    begin_closing(snapshot, harness, reason, CentralCloseProgress::NotOwed)
}

pub(super) fn retire(snapshot: &StartSnapshot, progress: StartProgress) -> MachineResult {
    move_to(
        snapshot,
        progress,
        vec![StartEffect::RetireAdmission, StartEffect::ReportOutcome],
    )
}
