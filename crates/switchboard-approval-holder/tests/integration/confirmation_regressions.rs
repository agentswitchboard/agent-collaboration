use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Ending, RaiseReply, RegisterRunReply, RunRefusal,
};
use state_machine_core::{StateMachine, StateTag, TransitionError};
use switchboard_approval_holder::waiting_request::{
    ProviderReply, WriterOutcome,
    gate_access::{GateWaitEvent, may_enqueue, step_under_gate},
};

use super::fixtures::{
    GateEvent, Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority, call,
    ending_reply, link, raise, try_offer,
};

fn ready() -> Mac {
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac
}

fn ending() -> DeliverEndingRequest {
    DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap()
}

// The gate can retire before a queued candidate has been handed over, even
// when that wait has not yet consumed the separate retirement input.
fn replace_gate_link(mac: &mut Mac) {
    mac.gate_event(GateEvent::ConnectionLost { link: link(1) });
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});
}

#[test]
fn retained_c1_publication_is_refused_under_ready_c2() {
    let mut mac = ready();
    let snapshot = mac.gate.clone();
    let retained_call = call(1, 1);
    assert_eq!(snapshot.ready_link(), Some(&retained_call.link));
    replace_gate_link(&mut mac);

    assert_eq!(
        step_under_gate(
            &mac.wait,
            &mac.gate,
            GateWaitEvent::Publish {
                authority: authority(),
                call: retained_call,
            },
        ),
        Err(TransitionError::Stale),
        "C1 can no longer publish under the current C2 gate"
    );
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            authority: authority(),
            call: call(2, 1),
            request: raise(),
        }]
    );
}

#[test]
fn retained_c1_ending_cannot_claim_or_reply_under_ready_c2() {
    let mut mac = ready();
    mac.publish();
    let snapshot = mac.gate.clone();
    let retired_link = snapshot.ready_link().unwrap();
    mac.lose_connection(1);
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});

    let (next, effects) = try_offer(
        &mac.wait,
        &mac.gate,
        authority(),
        retired_link.clone(),
        ending(),
    )
    .unwrap();
    assert!(effects.is_empty(), "an old carrier cannot receive a reply");
    assert!(
        next.claimed_reply().is_none(),
        "an old carrier cannot claim"
    );
    assert_eq!(next, mac.wait);
    assert_eq!(
        mac.offer(2, ending()),
        [ending_reply(2, DeliverEndingReply::Received {})]
    );
}

#[test]
fn queued_c1_raise_is_refused_at_enqueue_and_still_due() {
    let mut mac = ready();
    let [candidate]: [RequestEffect; 1] = mac.publish().try_into().unwrap();
    assert!(may_enqueue(&mac.gate, &candidate));
    replace_gate_link(&mut mac);

    assert!(
        !may_enqueue(&mac.gate, &candidate),
        "C2 cannot enqueue the queued C1 Raise"
    );
    assert!(mac.wait.has_pending_calls());
    mac.wait_event(RequestEvent::CallEndedWithoutReply { call: call(1, 1) });
    assert!(!mac.wait.has_pending_calls());
    let [retried]: [RequestEffect; 1] = mac.publish().try_into().unwrap();
    assert_eq!(
        retried,
        RequestEffect::Raise {
            authority: authority(),
            call: call(2, 2),
            request: raise(),
        }
    );
    assert!(may_enqueue(&mac.gate, &retried));
}

#[test]
fn queued_c1_stop_is_refused_at_enqueue_and_still_due() {
    let mut mac = ready();
    mac.publish();
    mac.wait_event(RequestEvent::CallEndedWithoutReply { call: call(1, 1) });
    mac.wait_event(RequestEvent::ProviderWithdrew {
        responder: RESPONDER,
    });
    let [candidate]: [RequestEffect; 1] = mac.publish().try_into().unwrap();
    let RequestEffect::Stop { request, .. } = &candidate else {
        panic!("withdrawal after possible Raise owes Stop");
    };
    assert!(may_enqueue(&mac.gate, &candidate));
    replace_gate_link(&mut mac);

    assert!(
        !may_enqueue(&mac.gate, &candidate),
        "C2 cannot enqueue the queued C1 Stop"
    );
    mac.wait_event(RequestEvent::CallEndedWithoutReply { call: call(1, 2) });
    assert!(mac.wait.holds_place(), "a lost Stop still owes settlement");
    let [retried]: [RequestEffect; 1] = mac.publish().try_into().unwrap();
    assert_eq!(
        retried,
        RequestEffect::Stop {
            authority: authority(),
            call: call(2, 3),
            request: request.clone(),
        }
    );
    assert!(may_enqueue(&mac.gate, &retried));
}

#[test]
fn queued_c1_ending_reply_is_refused_at_enqueue() {
    let mut mac = ready();
    mac.publish();
    let [candidate]: [RequestEffect; 1] = mac.offer(1, ending()).try_into().unwrap();
    assert_eq!(candidate, ending_reply(1, DeliverEndingReply::Received {}));
    assert!(may_enqueue(&mac.gate, &candidate));
    mac.lose_connection(1);
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});

    assert!(
        !may_enqueue(&mac.gate, &candidate),
        "C2 cannot enqueue the queued C1 ending reply"
    );
    let [reoffered]: [RequestEffect; 1] = mac.offer(2, ending()).try_into().unwrap();
    assert_eq!(reoffered, ending_reply(2, DeliverEndingReply::Received {}));
    assert!(may_enqueue(&mac.gate, &reoffered));
}

fn draining() -> Mac {
    let mut mac = ready();
    mac.publish();
    let request = ending();
    let reply = ProviderReply::CentralEnding(request.ending().clone());
    assert_eq!(
        mac.offer(1, request),
        [ending_reply(1, DeliverEndingReply::Received {})]
    );
    mac.wait_event(RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: reply.clone(),
    });
    mac.wait_event(RequestEvent::StartWrite {
        responder: RESPONDER,
        reply,
    });
    mac.wait_event(RequestEvent::WriteFinished {
        responder: RESPONDER,
        outcome: WriterOutcome::WriteReturned,
    });
    mac
}

fn assert_drained(mac: &Mac) {
    assert!(!mac.wait.holds_place());
    assert!(
        !mac.wait.has_pending_calls(),
        "the exact quit path must clear the call"
    );
    assert!(mac.wait.may_be_forgotten());
    assert!(mac.wait.is_terminal());
}

#[test]
fn draining_releases_the_place_and_late_run_retirement_still_fences_the_gate() {
    let mut mac = draining();
    assert!(
        !mac.wait.holds_place(),
        "the 256-place limit excludes the settled wait"
    );
    assert!(mac.wait.has_pending_calls());
    assert!(
        !mac.wait.may_be_forgotten() && !mac.wait.is_terminal(),
        "the displaced Raise still carries run authority"
    );
    assert!(mac.gate.ready_connection().is_some());

    let effects = mac.wait_event(RequestEvent::RaiseReply {
        call: call(1, 1),
        reply: RaiseReply::AuthorityRefused {
            refusal: RunRefusal::RunRetired {},
        },
    });
    assert!(
        matches!(
            effects.as_slice(),
            [RequestEffect::RunRefused(matched)] if matched.reason() == RunRefusal::RunRetired {}
        ),
        "the late matching reply carries the refusal across the wait boundary"
    );
    assert!(mac.gate.is_terminal(), "the reply must fence the run gate");
    assert_drained(&mac);
}

#[test]
fn draining_only_accepts_exact_call_end_and_does_not_answer_an_ending() {
    let mut mac = draining();
    assert!(!mac.wait.holds_place() && !mac.wait.is_terminal());
    for event in [
        RequestEvent::CallEndedWithoutReply { call: call(1, 9) },
        RequestEvent::RaiseReply {
            call: call(1, 9),
            reply: RaiseReply::AuthorityRefused {
                refusal: RunRefusal::RunRetired {},
            },
        },
        RequestEvent::ConnectionRetired { link: link(2) },
        RequestEvent::ProviderWithdrew {
            responder: RESPONDER,
        },
    ] {
        assert_eq!(
            RequestMachine::transition(&mac.wait, event),
            Err(TransitionError::Stale)
        );
    }
    assert_eq!(
        try_offer(&mac.wait, &mac.gate, authority(), link(1), ending()),
        Err(TransitionError::Stale),
        "a drain accepts only events that resolve the outstanding call"
    );
    assert!(
        mac.wait_event(RequestEvent::CallEndedWithoutReply { call: call(1, 1) })
            .is_empty()
    );
    assert_drained(&mac);
    assert!(mac.gate.ready_connection().is_some());
}

#[test]
fn draining_is_forgettable_after_its_link_is_retired() {
    let mut mac = draining();
    assert!(!mac.wait.holds_place() && !mac.wait.is_terminal());
    mac.lose_connection(1);
    assert_drained(&mac);
    assert!(mac.gate.ready_connection().is_none());
}

#[test]
fn draining_is_forgettable_after_run_fencing() {
    let mut mac = draining();
    assert!(!mac.wait.holds_place() && !mac.wait.is_terminal());
    mac.gate_event(GateEvent::AuthenticatedEnrollmentEnded {
        enrollment: authority().enrollment,
    });
    assert_drained(&mac);
    assert!(mac.gate.is_terminal());
}
