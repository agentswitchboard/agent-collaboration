use fleet_identity_protocol::HarnessKind;
use hegel::generators as gs;
use state_machine_core::{StateMachine, StateTag, TransitionError};
use switchboard_session_start::{
    BindObservation, BindRefusal, CentralCloseProgress, CentralEndObservation, CentralEndPurpose,
    CreateObservation, EndOutcome, EndedCentralSession, HarnessCloseObservation,
    HarnessCloseProgress, HarnessLaunchObservation, LaunchProgress, LaunchUncertainty, MakePlan,
    ObservedSettlement, RegistrationContradiction, RegistrationProgress, StartEffect, StartEvent,
    StartMachine, StartProgress, StartReadiness, StartRefusal, StartSnapshot,
};

use super::start_fixture::{
    address, attempt, central_id, from_progress, harness_id, issue, scope, settlement, step,
};

#[hegel::test(test_cases = 160)]
fn make_then_register_model_rejects_reserve_first_repeat_launch_and_early_publication(
    case: hegel::TestCase,
) {
    let harness = if case.draw(gs::booleans()) {
        HarnessKind::Claude
    } else {
        HarnessKind::Cursor
    };
    let asked = StartSnapshot::asked(scope(), harness.clone(), MakePlan::HeldOnLaunch);
    if case.draw(gs::booleans()) {
        let refused = step(
            &asked,
            StartEvent::Advance(StartReadiness::LinkUnavailable),
            &[StartEffect::RetireAdmission, StartEffect::ReportOutcome],
        );
        assert_eq!(
            refused.progress(),
            &StartProgress::Refused(StartRefusal::LinkUnavailable)
        );
        return;
    }
    let launching = step(
        &asked,
        StartEvent::Advance(StartReadiness::Ready),
        &[StartEffect::LaunchHeld],
    );
    assert_eq!(
        launching.progress(),
        &StartProgress::Launching(LaunchProgress::Intent)
    );
    if case.draw(gs::booleans()) {
        let unknown = step(
            &launching,
            StartEvent::Recover(StartReadiness::Ready),
            &[StartEffect::RetireAdmission, StartEffect::ReportOutcome],
        );
        assert_eq!(
            unknown.progress(),
            &StartProgress::LaunchUnknown(LaunchUncertainty::RouterRestarted)
        );
        assert_eq!(
            step(&unknown, StartEvent::Advance(StartReadiness::Ready), &[]),
            unknown
        );
        assert!(
            StartMachine::transition(
                &unknown,
                StartEvent::Launched(HarnessLaunchObservation::Held(harness_id()))
            )
            .is_err()
        );
        return;
    }
    let held = step(
        &launching,
        StartEvent::Launched(HarnessLaunchObservation::Held(harness_id())),
        &[StartEffect::AwaitReadiness],
    );
    assert_eq!(held.state_tag(), "registering_need_create");

    let mut registering = step(
        &held,
        StartEvent::Recover(StartReadiness::Ready),
        &[StartEffect::CreateCentral(attempt(1))],
    );
    let highest = case.draw(gs::integers::<u64>().min_value(1).max_value(5));
    for prior in 1..highest {
        let lost = step(
            &registering,
            StartEvent::CreateAnswered {
                attempt: attempt(prior),
                observation: CreateObservation::AnswerLost,
            },
            &[StartEffect::RetryAfterBackoff],
        );
        registering = step(
            &lost,
            StartEvent::Recover(StartReadiness::Ready),
            &[StartEffect::CreateCentral(attempt(prior + 1))],
        );
    }
    let request = registering.create_request(attempt(highest));
    assert_eq!(request.operation_id, scope().operation_id);
    assert_eq!(request.agent_program, harness);
    let issued = issue(highest);
    let binding = step(
        &registering,
        StartEvent::CreateAnswered {
            attempt: attempt(highest),
            observation: CreateObservation::Reserved(issued.clone()),
        },
        &[StartEffect::BindCentral(issued.clone())],
    );
    assert_eq!(
        binding.progress(),
        &StartProgress::Registering {
            harness: harness_id(),
            registration: RegistrationProgress::BindIntended(issued.clone())
        }
    );
    if case.draw(gs::booleans()) {
        assert_eq!(
            step(
                &binding,
                StartEvent::Recover(StartReadiness::Ready),
                &[StartEffect::BindCentral(issued.clone())]
            ),
            binding
        );
    }
    let open = step(
        &binding,
        StartEvent::BindAnswered {
            issue: issued.secret.clone(),
            observation: BindObservation::Active(address()),
        },
        &[StartEffect::PublishHeld(harness_id())],
    );
    assert_eq!(
        open.progress(),
        &StartProgress::Open {
            harness: harness_id(),
            secret: issued.secret
        }
    );
    assert_eq!(
        step(
            &open,
            StartEvent::Recover(StartReadiness::Ready),
            &[StartEffect::RestoreExact(harness_id())]
        ),
        open
    );
}

#[test]
fn stale_and_wrong_address_bind_replies_cannot_open_another_provider() {
    let issued = issue(2);
    let binding = from_progress(StartProgress::Registering {
        harness: harness_id(),
        registration: RegistrationProgress::BindIntended(issued.clone()),
    });
    assert_eq!(
        StartMachine::transition(
            &binding,
            StartEvent::BindAnswered {
                issue: issue(1).secret,
                observation: BindObservation::Active(address()),
            }
        ),
        Err(TransitionError::Stale)
    );
    let wrong = workspace_ids::SessionRef {
        session_id: "someone-else".parse().unwrap(),
        ..address()
    };
    let blocked = step(
        &binding,
        StartEvent::BindAnswered {
            issue: issued.secret.clone(),
            observation: BindObservation::Active(wrong.clone()),
        },
        &[StartEffect::ReportOutcome],
    );
    assert_eq!(
        blocked.progress(),
        &StartProgress::Registering {
            harness: harness_id(),
            registration: RegistrationProgress::Blocked(RegistrationContradiction::BindAddress {
                issued,
                address: wrong
            })
        }
    );
    assert_eq!(
        step(
            &blocked,
            StartEvent::Recover(StartReadiness::Ready),
            &[StartEffect::ReportOutcome]
        ),
        blocked
    );
}

#[test]
fn failed_bind_abandons_only_the_reservation_and_cannot_end_an_active_address() {
    let issued = issue(1);
    let binding = from_progress(StartProgress::Registering {
        harness: harness_id(),
        registration: RegistrationProgress::BindIntended(issued.clone()),
    });
    let closing = step(
        &binding,
        StartEvent::BindAnswered {
            issue: issued.secret,
            observation: BindObservation::Refused(BindRefusal::AddressTaken),
        },
        &[
            StartEffect::RetireAdmission,
            StartEffect::CloseExact(harness_id()),
        ],
    );
    let still_closing = step(
        &closing,
        StartEvent::HarnessCloseAnswered(HarnessCloseObservation::Closed),
        &[StartEffect::EndCentral {
            id: central_id(),
            purpose: CentralEndPurpose::AbandonReservation,
        }],
    );
    let StartProgress::Closing(closing) = still_closing.progress() else {
        panic!("cleanup must remain pending")
    };
    assert!(matches!(
        closing.central_close,
        CentralCloseProgress::Owed {
            purpose: CentralEndPurpose::AbandonReservation,
            ..
        }
    ));
}

#[test]
fn permanent_end_fences_tools_while_a_lost_close_and_end_answer_are_reconciled() {
    let open = from_progress(StartProgress::Open {
        harness: harness_id(),
        secret: issue(1).secret,
    });
    let closing = step(
        &open,
        StartEvent::EndForGood,
        &[
            StartEffect::RetireAdmission,
            StartEffect::CloseExact(harness_id()),
        ],
    );
    let central_pending = step(
        &closing,
        StartEvent::HarnessCloseAnswered(HarnessCloseObservation::AnswerLost),
        &[StartEffect::EndCentral {
            id: central_id(),
            purpose: CentralEndPurpose::EndBoundSession,
        }],
    );
    let both_pending = step(
        &central_pending,
        StartEvent::CentralEndAnswered {
            target: central_id(),
            observation: CentralEndObservation::Retry,
        },
        &[StartEffect::RetryAfterBackoff],
    );
    let closing_again = step(
        &both_pending,
        StartEvent::Recover(StartReadiness::Ready),
        &[StartEffect::CloseExact(harness_id())],
    );
    let central_again = step(
        &closing_again,
        StartEvent::HarnessCloseAnswered(HarnessCloseObservation::Closed),
        &[StartEffect::EndCentral {
            id: central_id(),
            purpose: CentralEndPurpose::EndBoundSession,
        }],
    );
    let evidence = ObservedSettlement::Existing(settlement());
    let ended = step(
        &central_again,
        StartEvent::CentralEndAnswered {
            target: central_id(),
            observation: CentralEndObservation::Settled(evidence.clone()),
        },
        &[StartEffect::ReportOutcome],
    );
    assert_eq!(
        ended.progress(),
        &StartProgress::Ended(EndOutcome::Closed {
            harness: harness_id(),
            reason: switchboard_session_start::ClosingReason::EndedForGood,
            harness_close: HarnessCloseProgress::Closed,
            central: EndedCentralSession::Settled {
                id: central_id(),
                evidence
            },
        })
    );
    assert_eq!(
        step(&ended, StartEvent::Recover(StartReadiness::Ready), &[]),
        ended
    );
}
