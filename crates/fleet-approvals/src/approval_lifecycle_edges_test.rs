use approval_request_protocol::{DecideRequest, DeliverEndingReply};
use central_engine_model::{EventTag, Machine, StateTag, Transition, TransitionError};

use crate::{
    ApprovalApply, ApprovalEvent, ApprovalLifecycle, ApprovalState, FactorStanding,
    ProviderSessionAvailability, RaiseAdmission,
};

use super::fixtures::{
    HOUR, answer, decide_event, key, offered_frame, operation_id, person, person_id, provider,
    provider_stop, raise_event, routing, row_event, run_event, setup, time,
};

#[test]
fn row_event_matrix_uses_declared_edges_or_typed_refusals_for_admitted_cases() {
    let (mut book, run, requester, parent) = setup();
    let key = key(&run, 1);
    let raise = provider(&key, &requester, "Matrix");
    let routing = routing(Some(&parent), true);
    let start = ApprovalEvent::Raise {
        key: key.clone(),
        raise: Box::new(raise.clone()),
        routing: routing.clone(),
        admission: RaiseAdmission {
            provider_session: ProviderSessionAvailability::Live,
            outstanding: 0,
        },
    };
    let stop = ApprovalEvent::Stop {
        key: key.clone(),
        request: provider_stop(&raise),
    };
    let unraised = ApprovalState::Unraised;
    let marker = row_event(&unraised, stop.clone(), 0, 0).unwrap().0;
    run_event(&mut book, raise_event(&run, raise, routing), 0);
    let waiting = book.requests[&key].clone();
    let pass_on = ApprovalEvent::PassOn {
        key: key.clone(),
        live_parent: parent,
    };
    let passed = row_event(&waiting, pass_on.clone(), 1, 0).unwrap().0;
    let offer = offered_frame(&run_event(
        &mut book,
        decide_event(&key, person(12, FactorStanding::Fresh), 101, answer(1)),
        1,
    ));
    let owed = book.requests[&key].clone();
    let delivered = ApprovalEvent::DeliveryReply {
        key: key.clone(),
        request: Box::new(offer.clone()),
        reply: DeliverEndingReply::Received {},
    };
    let not_held = ApprovalEvent::DeliveryReply {
        key: key.clone(),
        request: Box::new(offer),
        reply: DeliverEndingReply::NotHeld {},
    };
    let retired = row_event(&owed, ApprovalEvent::RunRetired, 1, 0).unwrap().0;
    let received = row_event(&owed, delivered.clone(), 1, 0).unwrap().0;
    let stopped = row_event(&owed, stop.clone(), 1, 0).unwrap().0;
    let scenarios = [
        (unraised, true),
        (marker, true),
        (waiting, true),
        (passed, true),
        (owed, true),
        (retired, false),
        (received, true),
        (stopped, true),
    ];
    let actor = person(12, FactorStanding::Fresh);
    let events = [
        start,
        stop,
        pass_on,
        delivered,
        not_held,
        ApprovalEvent::Alarm {
            deadline: time(HOUR),
        },
        ApprovalEvent::Alarm {
            deadline: time(HOUR + 1),
        },
        ApprovalEvent::Reoffer,
        ApprovalEvent::RunRetired,
        ApprovalEvent::SessionEnded,
        ApprovalEvent::EnrollmentEnded,
        ApprovalEvent::Decide {
            key: key.clone(),
            actor,
            request: DecideRequest {
                request_id: key.request_id.clone(),
                operation_id: operation_id(102),
                value: answer(1),
            },
            prior: None,
        },
        ApprovalEvent::Read {
            key,
            person_id: person_id(12),
        },
    ];
    let transitions = ApprovalLifecycle::transitions();
    for (state, admitted_holder) in scenarios {
        for event in &events {
            for sample in [2, HOUR] {
                let tagged = ApprovalApply {
                    high_water: time(0),
                    event: event.clone(),
                };
                let tag = tagged.event_tag();
                if !admitted_holder && matches!(tag, "raise" | "stop" | "deliveryReply" | "reoffer")
                {
                    continue;
                }
                match row_event(&state, event.clone(), sample, 0) {
                    Ok((next, _)) => assert!(
                        transitions.contains(&Transition {
                            from: state.state_tag(),
                            event: tag,
                            to: next.state_tag(),
                            terminal: next.is_terminal(),
                        }),
                        "undeclared {state:?} + {event:?} -> {next:?}"
                    ),
                    Err(TransitionError::Stale) => {
                        assert!(matches!(tag, "alarm" | "deliveryReply" | "reoffer"))
                    }
                    Err(other) => panic!("unexpected refusal for {tag}: {other:?}"),
                }
            }
        }
    }
}
