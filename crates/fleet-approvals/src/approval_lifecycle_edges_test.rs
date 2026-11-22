use approval_request_protocol::{Ending, EndingView, HolderReport, Refusal};
use central_engine_model::{EventTag, Machine, StateTag, Transition, TransitionError};

use crate::{ApprovalEvent, ApprovalLifecycle, ApprovalRefusal, ApprovalState};

use super::erasure::SIXTY_DAYS;
use super::fixtures::{
    DEADLINE, apply, holder_report, parent_pass_on, raise_event, router_run, step, stop_event,
    time, waiting,
};
use super::{allow_from_parent, closed, refused};

#[derive(Clone, Copy)]
enum HolderRunAdmission {
    Registered,
    Retired,
}

fn admits_fixture_event(run: HolderRunAdmission, event: &ApprovalEvent) -> bool {
    match run {
        HolderRunAdmission::Registered => true,
        HolderRunAdmission::Retired => !matches!(
            event,
            ApprovalEvent::Raise { .. }
                | ApprovalEvent::Stop(_)
                | ApprovalEvent::Report(_)
                | ApprovalEvent::OfferAgain { .. }
        ),
    }
}

fn authority_ended_state(state: &str) -> bool {
    matches!(state, "noHandoverOwed" | "receiptUnknown")
}

#[test]
fn edge_matrix_fences_retired_runs_and_checks_admitted_transitions() {
    let pending = waiting();
    let (passed, _) = apply(&pending, parent_pass_on(), 1);
    let (pending_fresh, _) = apply(&ApprovalState::Unraised, raise_event(), DEADLINE - 1);
    let (passed_fresh, _) = apply(&pending_fresh, parent_pass_on(), DEADLINE - 1);
    let (awaiting, _) = apply(&pending, allow_from_parent(), 1);
    let (reported, _) = apply(
        &awaiting,
        holder_report(router_run(), HolderReport::Applied),
        1,
    );
    let (no_handover, _) = apply(&pending, ApprovalEvent::RunRetired, 1);
    let (unknown, _) = apply(&awaiting, ApprovalEvent::RunRetired, 1);
    let (marker, _) = apply(&ApprovalState::Unraised, stop_event(), 1);

    let old_ending_at = DEADLINE - SIXTY_DAYS;
    let (old_pending, _) = apply(&ApprovalState::Unraised, raise_event(), old_ending_at - 1);
    let (old_awaiting, _) = apply(&old_pending, allow_from_parent(), old_ending_at);
    let (old_reported, _) = apply(
        &old_awaiting,
        holder_report(router_run(), HolderReport::Applied),
        old_ending_at,
    );
    let (old_unowed, _) = apply(&old_pending, ApprovalEvent::RunRetired, old_ending_at);
    let (old_unknown, _) = apply(&old_awaiting, ApprovalEvent::RunRetired, old_ending_at);
    let (erased, _) = apply(&old_awaiting, ApprovalEvent::Erase, DEADLINE);
    let scenarios = [
        (ApprovalState::Unraised, HolderRunAdmission::Registered),
        (marker, HolderRunAdmission::Registered),
        (pending_fresh, HolderRunAdmission::Registered),
        (passed_fresh, HolderRunAdmission::Registered),
        (pending, HolderRunAdmission::Registered),
        (passed, HolderRunAdmission::Registered),
        (awaiting, HolderRunAdmission::Registered),
        (reported, HolderRunAdmission::Registered),
        (no_handover, HolderRunAdmission::Retired),
        (unknown, HolderRunAdmission::Retired),
        (old_awaiting, HolderRunAdmission::Registered),
        (old_reported, HolderRunAdmission::Registered),
        (old_unowed, HolderRunAdmission::Retired),
        (old_unknown, HolderRunAdmission::Retired),
        (erased, HolderRunAdmission::Registered),
    ];
    let events = [
        raise_event(),
        allow_from_parent(),
        parent_pass_on(),
        stop_event(),
        ApprovalEvent::ClockReached,
        ApprovalEvent::RunRetired,
        ApprovalEvent::SessionEnded,
        ApprovalEvent::EnrollmentEnded,
        ApprovalEvent::OfferAgain {
            router_run: router_run(),
        },
        holder_report(router_run(), HolderReport::Applied),
        ApprovalEvent::Erase,
    ];
    let declared = ApprovalLifecycle::transitions();
    let mut observed = Vec::new();
    for (state, run_admission) in scenarios {
        for event in &events {
            let state_tag = state.state_tag();
            let event_tag = event.event_tag();
            let admitted = admits_fixture_event(run_admission, event);
            let expected_held = authority_ended_state(state_tag)
                && matches!(event_tag, "raise" | "stop" | "report" | "offerAgain");
            assert_eq!(
                !admitted, expected_held,
                "caller admission for {state_tag} + {event_tag}"
            );
            if !admitted {
                continue;
            }

            let refusal = match (state_tag, event_tag) {
                ("unraised" | "stoppedBeforeRaise", "raise" | "stop") => None,
                ("unraised" | "stoppedBeforeRaise", _)
                | ("pending" | "passedOn", "offerAgain" | "report" | "erase")
                | ("erased", "clockReached" | "runRetired" | "sessionEnded" | "enrollmentEnded") => {
                    Some(TransitionError::NotAllowed {
                        state: state.state_tag(),
                        event: event.event_tag(),
                    })
                }
                ("pending" | "passedOn", "clockReached")
                    if ApprovalLifecycle::next_due(&state) > Some(time(DEADLINE)) =>
                {
                    Some(TransitionError::Stale)
                }
                (
                    "awaitingReport" | "reported" | "noHandoverOwed" | "receiptUnknown",
                    "answer" | "passOn",
                ) => {
                    let ending = match &closed(&state).ending {
                        Ending::Decided { answer, by } => EndingView::DecidedWithAnswer {
                            answer: answer.clone(),
                            by: by.clone(),
                        },
                        Ending::Cancelled { reason } => EndingView::Cancelled { reason: *reason },
                        Ending::TimedOut {} => EndingView::TimedOut {},
                    };
                    Some(refused(Refusal::Ended { ending }))
                }
                ("erased", "erase") => Some(TransitionError::Stale),
                ("erased", "answer" | "passOn") => Some(refused(Refusal::NotVisible {})),
                ("awaitingReport" | "reported" | "noHandoverOwed" | "receiptUnknown", "erase")
                    if closed(&state).ended_at.timestamp_millis() > old_ending_at =>
                {
                    Some(TransitionError::Stale)
                }
                ("reported" | "erased", "offerAgain") => {
                    Some(TransitionError::Domain(ApprovalRefusal::NoHandoverOwed))
                }
                _ => None,
            };
            let result = step(&state, event.clone(), DEADLINE);
            assert_eq!(
                result.as_ref().err(),
                refusal.as_ref(),
                "{state:?}, {event:?}"
            );
            if let Ok((next, _)) = result {
                let transition = Transition {
                    from: state_tag,
                    event: event_tag,
                    to: next.state_tag(),
                    terminal: next.is_terminal(),
                };
                assert!(
                    declared.contains(&transition),
                    "undeclared transition {transition:?}"
                );
                observed.push(transition);
            }
        }
    }

    for transition in declared {
        // The raw table describes the reducer. These replay edges have no
        // admitted witness once their holder's run has retired.
        let behind_caller_fence =
            authority_ended_state(transition.from) && transition.event == "raise";
        assert_eq!(
            observed.contains(transition),
            !behind_caller_fence,
            "declared edge coverage for {transition:?}"
        );
    }
}
