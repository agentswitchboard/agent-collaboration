use state_machine_core::{StateMachine, TransitionError};
use switchboard_session_start::{
    DurableFirstInput, FirstInputObservation, HarnessLaunchObservation, InvalidStartSnapshot,
    LaunchProgress, LaunchUncertainty, MakePlan, RegistrationProgress, StartEffect, StartEvent,
    StartMachine, StartProgress, StartReadiness, StartRefusal, StartSnapshot,
};

use super::start_fixture::{attempt, first_input, harness_id, native_snapshot, scope, step};

#[test]
fn native_make_rejects_registering_on_allocation_and_replaying_first_input_on_restart() {
    let asked = native_snapshot(StartProgress::Asked);
    let launch = step(
        &asked,
        StartEvent::Advance(StartReadiness::Ready),
        &[StartEffect::LaunchHeld],
    );
    assert!(
        StartMachine::transition(
            &launch,
            StartEvent::Launched(HarnessLaunchObservation::Held(harness_id()))
        )
        .is_err()
    );
    let allocated = step(
        &launch,
        StartEvent::Launched(HarnessLaunchObservation::Allocated(harness_id())),
        &[StartEffect::AwaitReadiness],
    );
    assert_eq!(
        allocated.progress(),
        &StartProgress::Launching(LaunchProgress::Allocated {
            harness: harness_id()
        })
    );
    let prime = step(
        &allocated,
        StartEvent::Advance(StartReadiness::Ready),
        &[StartEffect::SubmitFirstInput {
            harness: harness_id(),
            input: first_input(),
        }],
    );
    assert_eq!(
        prime.progress(),
        &StartProgress::Launching(LaunchProgress::FirstInputIntended {
            harness: harness_id(),
            input: first_input()
        })
    );
    assert_eq!(
        step(
            &prime,
            StartEvent::Recover(StartReadiness::Ready),
            &[StartEffect::VerifyFirstInput {
                harness: harness_id(),
                input: first_input()
            }]
        ),
        prime
    );

    let mut wrong = first_input();
    wrong.fingerprint = [6; 32];
    let observation = FirstInputObservation::DurablyTaken;
    assert_eq!(
        StartMachine::transition(
            &prime,
            StartEvent::FirstInputAnswered {
                harness: harness_id(),
                input: wrong,
                observation
            }
        ),
        Err(TransitionError::Stale)
    );
    let made = step(
        &prime,
        StartEvent::FirstInputAnswered {
            harness: harness_id(),
            input: first_input(),
            observation,
        },
        &[StartEffect::AwaitReadiness],
    );
    assert_eq!(
        made.progress(),
        &StartProgress::Registering {
            harness: harness_id(),
            registration: RegistrationProgress::NeedCreate
        }
    );
    assert_eq!(
        made.first_input(),
        Some(&DurableFirstInput {
            harness: harness_id(),
            input: first_input()
        })
    );
    let create = step(
        &made,
        StartEvent::Advance(StartReadiness::Ready),
        &[StartEffect::CreateCentral(attempt(1))],
    );
    assert_eq!(create.first_input(), made.first_input());
}

#[test]
fn native_make_restart_with_an_unsaved_body_and_without_proof_cannot_dispatch() {
    let asked = native_snapshot(StartProgress::Asked);
    let refusal = step(
        &asked,
        StartEvent::Recover(StartReadiness::Ready),
        &[StartEffect::RetireAdmission, StartEffect::ReportOutcome],
    );
    assert_eq!(
        refusal.progress(),
        &StartProgress::Refused(StartRefusal::InitialInputUnavailable)
    );
    let allocated = native_snapshot(StartProgress::Launching(LaunchProgress::Allocated {
        harness: harness_id(),
    }));
    let unknown = step(
        &allocated,
        StartEvent::Recover(StartReadiness::Ready),
        &[StartEffect::RetireAdmission, StartEffect::ReportOutcome],
    );
    assert_eq!(
        unknown.progress(),
        &StartProgress::LaunchUnknown(LaunchUncertainty::RecoveredAllocation {
            harness: harness_id()
        })
    );
    let intended = native_snapshot(StartProgress::Launching(
        LaunchProgress::FirstInputIntended {
            harness: harness_id(),
            input: first_input(),
        },
    ));
    let unknown = step(
        &intended,
        StartEvent::FirstInputAnswered {
            harness: harness_id(),
            input: first_input(),
            observation: FirstInputObservation::Unconfirmed,
        },
        &[StartEffect::RetireAdmission, StartEffect::ReportOutcome],
    );
    assert_eq!(
        unknown.progress(),
        &StartProgress::LaunchUnknown(LaunchUncertainty::FirstInputUnconfirmed {
            harness: harness_id(),
            input: first_input()
        })
    );
    assert_eq!(
        step(&unknown, StartEvent::Recover(StartReadiness::Ready), &[]),
        unknown
    );
}

#[test]
fn native_reopen_rejects_a_registering_snapshot_without_exact_input_evidence() {
    let snapshot = StartSnapshot::restore(
        scope(),
        fleet_identity_protocol::HarnessKind::Codex,
        MakePlan::FirstInputRequired(first_input()),
        None,
        StartProgress::Registering {
            harness: harness_id(),
            registration: RegistrationProgress::NeedCreate,
        },
    );
    assert_eq!(snapshot, Err(InvalidStartSnapshot::WrongFirstInput));
}
