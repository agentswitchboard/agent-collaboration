use state_machine_core::{StateMachine, check_state_machine_transitions};
use switchboard_session_start::{
    BindObservation, BindRefusal, CentralEndObservation, CreateObservation, CreateRefusal,
    EndOutcome, FirstInputObservation, HarnessCloseObservation, HarnessLaunchObservation,
    InvalidStartSnapshot, LaunchProgress, LaunchUncertainty, MakePlan, ObservedSettlement,
    RegistrationContradiction, RegistrationProgress, StartEvent, StartMachine, StartProgress,
    StartReadiness, StartRefusal, StartSnapshot,
};

use super::start_fixture::{
    address, attempt, central_id, first_input, from_progress, harness_id, issue, native_snapshot,
    scope, settlement,
};

#[test]
fn declared_table_detects_missing_or_undeclared_start_transitions() {
    let cases = snapshots().into_iter().flat_map(|snapshot| {
        observations()
            .into_iter()
            .map(move |observation| (snapshot.clone(), observation))
    });
    check_state_machine_transitions::<StartMachine>(cases).unwrap();
}

#[test]
fn restored_open_rejects_a_sealed_proof_from_another_start() {
    let mut proof = issue(1).secret;
    proof.start_id = "019c0000-0000-7000-8000-000000000079".parse().unwrap();
    let restored = StartSnapshot::restore(
        scope(),
        fleet_identity_protocol::HarnessKind::Cursor,
        MakePlan::HeldOnLaunch,
        None,
        StartProgress::Open {
            harness: harness_id(),
            secret: proof,
        },
    );
    assert_eq!(restored, Err(InvalidStartSnapshot::WrongProofOwner));
}

fn snapshots() -> Vec<StartSnapshot> {
    let claiming = |registration| StartProgress::Registering {
        harness: harness_id(),
        registration,
    };
    let open = from_progress(StartProgress::Open {
        harness: harness_id(),
        secret: issue(1).secret,
    });
    let closing = StartMachine::transition(&open, StartEvent::EndForGood)
        .unwrap()
        .0;
    let waiting_for_end = StartMachine::transition(
        &closing,
        StartEvent::HarnessCloseAnswered(HarnessCloseObservation::Closed),
    )
    .unwrap()
    .0;
    let local_only = StartMachine::transition(
        &from_progress(StartProgress::Launching(LaunchProgress::Intent)),
        StartEvent::Launched(HarnessLaunchObservation::SetupFailed(harness_id())),
    )
    .unwrap()
    .0;
    let mut fixtures: Vec<_> = [
        StartProgress::Asked,
        StartProgress::Launching(LaunchProgress::Intent),
        claiming(RegistrationProgress::NeedCreate),
        claiming(RegistrationProgress::CreateIntended(attempt(1))),
        claiming(RegistrationProgress::CreateIntended(attempt(
            9_007_199_254_740_991,
        ))),
        claiming(RegistrationProgress::BindIntended(issue(1))),
        claiming(RegistrationProgress::Blocked(
            RegistrationContradiction::AttemptLimit,
        )),
        StartProgress::Ended(EndOutcome::LaunchRejected),
        StartProgress::Refused(StartRefusal::LinkUnavailable),
        StartProgress::LaunchUnknown(LaunchUncertainty::AnswerLost),
    ]
    .into_iter()
    .map(from_progress)
    .collect();
    fixtures.extend([
        open,
        closing,
        waiting_for_end,
        local_only,
        native_snapshot(StartProgress::Launching(LaunchProgress::Intent)),
        native_snapshot(StartProgress::Launching(LaunchProgress::Allocated {
            harness: harness_id(),
        })),
        native_snapshot(StartProgress::Launching(
            LaunchProgress::FirstInputIntended {
                harness: harness_id(),
                input: first_input(),
            },
        )),
    ]);
    fixtures
}

fn observations() -> Vec<StartEvent> {
    let create = |observation| StartEvent::CreateAnswered {
        attempt: attempt(1),
        observation,
    };
    let bind = |observation| StartEvent::BindAnswered {
        issue: issue(1).secret,
        observation,
    };
    let end = |observation| StartEvent::CentralEndAnswered {
        target: central_id(),
        observation,
    };
    let mut events = vec![
        StartEvent::Launched(HarnessLaunchObservation::Held(harness_id())),
        StartEvent::Launched(HarnessLaunchObservation::Allocated(harness_id())),
        StartEvent::Launched(HarnessLaunchObservation::SetupFailed(harness_id())),
        StartEvent::Launched(HarnessLaunchObservation::Rejected),
        StartEvent::Launched(HarnessLaunchObservation::ProvenNotSent),
        StartEvent::Launched(HarnessLaunchObservation::AnswerLost),
        create(CreateObservation::AnswerLost),
        create(CreateObservation::Reserved(issue(1))),
        create(CreateObservation::Contradicted),
        create(CreateObservation::Refused(CreateRefusal::OperationConflict)),
        bind(BindObservation::Retry),
        bind(BindObservation::Active(address())),
        bind(BindObservation::Contradicted),
        bind(BindObservation::Refused(BindRefusal::WrongSecret)),
        StartEvent::EndForGood,
        StartEvent::HarnessCloseAnswered(HarnessCloseObservation::Closed),
        StartEvent::HarnessCloseAnswered(HarnessCloseObservation::AnswerLost),
        end(CentralEndObservation::Retry),
        end(CentralEndObservation::Settled(
            ObservedSettlement::Existing(settlement()),
        )),
    ];
    for observation in [
        FirstInputObservation::DurablyTaken,
        FirstInputObservation::NeedsRead,
        FirstInputObservation::Unconfirmed,
        FirstInputObservation::SetupFailed,
    ] {
        events.push(StartEvent::FirstInputAnswered {
            harness: harness_id(),
            input: first_input(),
            observation,
        });
    }
    for readiness in [
        StartReadiness::Ready,
        StartReadiness::LinkUnavailable,
        StartReadiness::EnrollmentEnded,
    ] {
        events.extend([
            StartEvent::Advance(readiness),
            StartEvent::Recover(readiness),
        ]);
    }
    events
}
