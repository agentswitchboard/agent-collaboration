use approval_request_protocol::{
    AdmittedDestination, Answer, CallCancellation, CallStopCause, DecidedBy, DecisionFactor,
    DeliverEndingReply, DeliverEndingRequest, Ending, HolderStopCause, InvalidApprovalEnding,
    ParkedCallEnding, PrivateEnding, Raise, RegisterRunReply, SessionCallDecision, StopReply,
    StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{LinkAttemptDisposition, LinkCallAnswer};
use switchboard_approval_holder::{
    run_gate::CallAttempt,
    waiting_request::{ProviderReply, WaitRefusal, WriterOutcome},
};

use super::fixtures::{
    Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority, call, call_raise,
    ending_reply, raise, try_offer, try_publish,
};

const WITHDRAW: RequestEvent = RequestEvent::LocalStop {
    cause: HolderStopCause::CallWithdrawn,
};

fn ready(raise: Raise) -> Mac {
    let mut mac = Mac::with_raise(raise);
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac
}

fn assert_raised(mac: &mut Mac, request: &Raise) {
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(1, 1),
            authority: authority(),
            request: request.clone(),
        }]
    );
}

fn decision_frame(
    raise: &Raise,
    decision: SessionCallDecision,
) -> (DeliverEndingRequest, ParkedCallEnding) {
    let person_id: workspace_ids::PersonId =
        "01890f2e-7b4c-7cc0-98c4-000000000004".parse().unwrap();
    let person = DecidedBy::Person {
        person_id: person_id.clone(),
    };
    let admitted =
        AdmittedDestination::for_request(raise, &person, std::slice::from_ref(&person)).unwrap();
    let decision_proof = admitted
        .decide(
            Answer::UnprovenCall { decision },
            DecisionFactor::FreshPersonFactor,
        )
        .unwrap();
    let frame = DeliverEndingRequest::from_decision(raise, decision_proof).unwrap();
    (
        frame,
        ParkedCallEnding::Decided {
            decision,
            person_id,
        },
    )
}

fn assert_claim_and_submit(
    mac: &mut Mac,
    frame: DeliverEndingRequest,
    expected: ParkedCallEnding,
    conflicting: DeliverEndingRequest,
) {
    let reply = ProviderReply::CentralEnding(PrivateEnding::UnprovenCall { ending: expected });
    assert_eq!(
        mac.offer(1, frame.clone()),
        [ending_reply(1, DeliverEndingReply::Received {})]
    );
    assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &reply)));
    let claimed = mac.wait.clone();
    assert_eq!(
        mac.offer(1, frame),
        [ending_reply(1, DeliverEndingReply::Received {})]
    );
    assert_eq!(mac.wait, claimed);
    assert_eq!(
        mac.offer(1, conflicting),
        [
            RequestEffect::EndingConflict {
                request_id: call_raise().request_id().clone(),
                reason: InvalidApprovalEnding::BindingMismatch,
            },
            ending_reply(1, DeliverEndingReply::EndingDiffers {}),
        ]
    );
    assert_eq!(mac.wait, claimed);
    assert!(
        mac.wait_event(RequestEvent::TakeFinalPermit {
            responder: RESPONDER,
            reply: reply.clone(),
        })
        .is_empty()
    );
    let start = RequestEvent::StartWrite {
        responder: RESPONDER,
        reply: reply.clone(),
    };
    assert_eq!(
        mac.wait_event(start.clone()),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply
        }]
    );
    assert_eq!(
        RequestMachine::transition(&mac.wait, start),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        mac.wait_event(RequestEvent::WriteFinished {
            responder: RESPONDER,
            outcome: WriterOutcome::WriteReturned,
        }),
        [
            RequestEffect::ReleaseBinding {
                responder: RESPONDER
            },
            RequestEffect::WriterResolved {
                responder: RESPONDER,
                outcome: WriterOutcome::WriteReturned
            },
        ]
    );
    assert!(!mac.wait.holds_place());
}

#[test]
fn unproven_call_allow_claims_and_submits_once() {
    let request = call_raise();
    assert_eq!(request.requesting_session(), None);
    let (allow, expected) = decision_frame(&request, SessionCallDecision::Allow);
    let (decline, _) = decision_frame(&request, SessionCallDecision::Decline);
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);

    // A different frozen invocation or provider ending must not acquire this responder.
    let mut wrong_digest = serde_json::to_value(&allow).unwrap();
    wrong_digest["contentsDigest"] =
        serde_json::to_value(raise().contents_digest().unwrap()).unwrap();
    let mut wrong_kind = serde_json::to_value(
        DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap(),
    )
    .unwrap();
    wrong_kind["contentsDigest"] =
        serde_json::to_value(request.contents_digest().unwrap()).unwrap();
    let raised = mac.wait.clone();
    for wrong in [wrong_digest, wrong_kind] {
        assert_eq!(
            mac.offer(1, serde_json::from_value(wrong).unwrap()),
            [
                RequestEffect::EndingConflict {
                    request_id: request.request_id().clone(),
                    reason: InvalidApprovalEnding::BindingMismatch,
                },
                ending_reply(1, DeliverEndingReply::EndingDiffers {}),
            ]
        );
        assert_eq!(mac.wait, raised);
    }
    let mut wrong_id = serde_json::to_value(&allow).unwrap();
    wrong_id["requestId"] = serde_json::json!("01890f2e-7b4c-7cc0-98c4-00000000000f");
    assert_eq!(
        try_offer(
            &mac.wait,
            &mac.gate,
            authority(),
            call(1, 0).link,
            serde_json::from_value(wrong_id).unwrap()
        ),
        Err(TransitionError::Stale)
    );

    assert_claim_and_submit(&mut mac, allow, expected, decline);
}

#[test]
fn unproven_call_decline_claims_and_submits_once() {
    let request = call_raise();
    let (decline, expected) = decision_frame(&request, SessionCallDecision::Decline);
    let (allow, _) = decision_frame(&request, SessionCallDecision::Allow);
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);
    assert_claim_and_submit(&mut mac, decline, expected, allow);
}

#[test]
fn unproven_call_cancelled_claims_and_submits_once() {
    let request = call_raise();
    let cancelled = DeliverEndingRequest::from_raise(
        &request,
        Ending::Cancelled {
            reason: approval_request_protocol::CancelReason::RequesterGone {},
        },
    )
    .unwrap();
    let timed_out = DeliverEndingRequest::from_raise(&request, Ending::TimedOut {}).unwrap();
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);
    assert_claim_and_submit(
        &mut mac,
        cancelled,
        ParkedCallEnding::Cancelled {
            reason: CallCancellation::RequesterGone {},
        },
        timed_out,
    );
}

#[test]
fn unproven_call_timed_out_claims_and_submits_once() {
    let request = call_raise();
    let timed_out = DeliverEndingRequest::from_raise(&request, Ending::TimedOut {}).unwrap();
    let (allow, _) = decision_frame(&request, SessionCallDecision::Allow);
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);
    assert_claim_and_submit(&mut mac, timed_out, ParkedCallEnding::TimedOut {}, allow);
}

#[test]
fn unproven_call_withdrawn_before_raise_has_no_central_duty() {
    let mut mac = ready(call_raise());
    assert_eq!(
        mac.wait_event(WITHDRAW),
        [RequestEffect::ReleaseBinding {
            responder: RESPONDER
        }]
    );
    assert!(mac.wait.may_be_forgotten());
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(1)),
        Err(TransitionError::Stale)
    );
}

#[test]
fn unproven_call_withdrawn_after_raise_replays_one_exact_stop() {
    let request = call_raise();
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);
    mac.lose_answer(1, 1);
    assert_eq!(
        mac.wait_event(WITHDRAW),
        [RequestEffect::ReleaseBinding {
            responder: RESPONDER
        }]
    );
    let stop = StopRequest::UnprovenCall {
        request_id: request.request_id().clone(),
        router_run: request.router_run().clone(),
        contents_digest: request.contents_digest().unwrap(),
        cause: CallStopCause::CallWithdrawn,
    };
    assert_eq!(stop.cause(), HolderStopCause::CallWithdrawn);
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(1, 2),
            authority: authority(),
            request: stop.clone(),
        }]
    );
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(3)),
        Err(TransitionError::Stale)
    );

    mac.lose_connection(1);
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(2, 3),
            authority: authority(),
            request: stop,
        }]
    );
    assert_eq!(
        RequestMachine::transition(
            &mac.wait,
            RequestEvent::AttemptSettled {
                call: call(1, 2),
                disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(
                    StopReply::NotVisible {}
                ))
            }
        ),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        mac.wait_event(RequestEvent::AttemptSettled {
            call: call(2, 3),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(
                StopReply::NotVisible {}
            ))
        }),
        [RequestEffect::StopNotVisible {
            request_id: request.request_id().clone(),
            contents_digest: request.contents_digest().unwrap(),
        }]
    );
    assert!(mac.wait.may_be_forgotten());
}

#[test]
fn unproven_call_refuses_provider_stop_and_take_back() {
    let request = call_raise();
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);
    let provider_causes = [
        HolderStopCause::TurnStopped,
        HolderStopCause::AskReplaced,
        HolderStopCause::AskWithdrawn,
        HolderStopCause::SessionClosed,
        HolderStopCause::ProviderConnectionLost,
    ];
    let events = provider_causes
        .into_iter()
        .map(|cause| RequestEvent::LocalStop { cause })
        .chain([RequestEvent::ProviderWithdrew {
            responder: RESPONDER,
        }]);
    for event in events {
        assert_eq!(
            RequestMachine::transition(&mac.wait, event),
            Err(TransitionError::Domain(
                WaitRefusal::StopCauseForDifferentKind
            ))
        );
    }
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(2)),
        Err(TransitionError::Stale)
    );
}

#[test]
fn unproven_call_cause_is_refused_on_a_provider_ask() {
    let request = raise();
    let mut mac = ready(request.clone());
    assert_raised(&mut mac, &request);
    assert_eq!(
        RequestMachine::transition(&mac.wait, WITHDRAW),
        Err(TransitionError::Domain(
            WaitRefusal::StopCauseForDifferentKind
        ))
    );
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(2)),
        Err(TransitionError::Stale)
    );
}
