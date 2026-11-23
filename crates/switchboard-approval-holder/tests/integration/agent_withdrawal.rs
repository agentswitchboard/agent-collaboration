use approval_request_protocol::{
    DeliverEndingReply, DeliverEndingRequest, Ending, ProviderStopCause, RegisterRunReply,
    StopReceiptEnding, StopReply, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{LinkAttemptDisposition, LinkCallAnswer};
use switchboard_approval_holder::{
    run_gate::CallAttempt,
    waiting_request::{ProviderReply, WriterOutcome},
};

use super::fixtures::{
    Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority, call, ending_reply,
    raise, try_publish,
};

fn withdrawn() -> RequestEvent {
    RequestEvent::ProviderWithdrew {
        responder: RESPONDER,
    }
}

fn take(reply: ProviderReply) -> RequestEvent {
    RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply,
    }
}

fn start(reply: ProviderReply) -> RequestEvent {
    RequestEvent::StartWrite {
        responder: RESPONDER,
        reply,
    }
}

fn claimed(mac: &mut Mac) -> ProviderReply {
    mac.publish();
    let ending = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    let reply = ProviderReply::CentralEnding(ending.ending().clone());
    assert_eq!(
        mac.offer(1, ending),
        [ending_reply(1, DeliverEndingReply::Received {})]
    );
    reply
}

fn ready() -> Mac {
    let mut mac = Mac::new();
    mac.welcome(1);
    mac.registered(1, RegisterRunReply::Registered {});
    mac
}

fn no_second_release(mac: &Mac) {
    assert_eq!(
        RequestMachine::transition(&mac.wait, withdrawn()),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        RequestMachine::transition(
            &mac.wait,
            RequestEvent::WriteFinished {
                responder: RESPONDER,
                outcome: WriterOutcome::WriteReturned,
            }
        ),
        Err(TransitionError::Stale)
    );
}

#[test]
fn agent_take_back_before_raise_releases_without_central_duty() {
    let mut mac = ready();
    assert_eq!(
        RequestMachine::transition(&mac.wait, RequestEvent::ProviderWithdrew { responder: 2 }),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        mac.wait_event(withdrawn()),
        [RequestEffect::ReleaseBinding {
            responder: RESPONDER
        }],
        "unsent take-back must release the available responder"
    );
    assert!(!mac.wait.holds_place());
    assert!(!mac.wait.has_pending_calls());
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(1)),
        Err(TransitionError::Stale)
    );
    no_second_release(&mac);
}

#[test]
fn agent_take_back_after_possible_raise_keeps_an_exact_stop_across_reconnect() {
    let mut mac = ready();
    mac.publish();
    assert_eq!(
        mac.wait_event(withdrawn()),
        [RequestEffect::ReleaseBinding {
            responder: RESPONDER
        }]
    );
    assert!(mac.wait.claimed_reply().is_none());
    assert!(
        mac.wait.holds_place(),
        "exposed take-back owes a Stop before closure"
    );
    no_second_release(&mac);
    mac.lose_answer(1, 1);
    let request = StopRequest::Provider {
        request_id: raise().request_id().clone(),
        router_run: authority().router_run,
        contents_digest: raise().contents_digest().unwrap(),
        cause: ProviderStopCause::AskWithdrawn,
    };
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(1, 2),
            authority: authority(),
            request: request.clone()
        }],
        "exposed take-back owes a Stop with the same binding"
    );
    mac.lose_connection(1);
    mac.welcome(2);
    mac.registered(2, RegisterRunReply::Registered {});
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(2, 3),
            authority: authority(),
            request
        }]
    );
    assert_eq!(
        mac.wait_event(RequestEvent::AttemptSettled {
            call: call(2, 3),
            disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(
                StopReply::Stands {
                    ending: StopReceiptEnding::TimedOut {}
                }
            ))
        }),
        [RequestEffect::StopReconciled(
            StopReceiptEnding::TimedOut {}
        )]
    );
    assert!(!mac.wait.holds_place());
}

#[test]
fn agent_take_back_of_a_claim_or_permit_revokes_the_reply_without_stop() {
    for permitted in [false, true] {
        let mut mac = ready();
        let reply = claimed(&mut mac);
        if permitted {
            mac.wait_event(take(reply.clone()));
        }
        assert_eq!(
            mac.wait_event(withdrawn()),
            [RequestEffect::ReleaseBinding {
                responder: RESPONDER
            }],
            "claimed take-back revokes the reply before start"
        );
        assert_eq!(
            RequestMachine::transition(&mac.wait, take(reply.clone())),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            RequestMachine::transition(&mac.wait, start(reply)),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            try_publish(&mac.wait, &mac.gate, CallAttempt::new(2)),
            Err(TransitionError::Stale)
        );
        assert!(!mac.wait.holds_place());
        no_second_release(&mac);
    }
}

#[test]
fn agent_take_back_after_start_leaves_the_original_writer_to_report_once() {
    let mut mac = ready();
    let reply = claimed(&mut mac);
    mac.wait_event(take(reply.clone()));
    assert_eq!(
        mac.wait_event(start(reply.clone())),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply: reply.clone()
        }]
    );
    assert!(
        mac.wait_event(withdrawn()).is_empty(),
        "take-back after start cannot release the running writer"
    );
    assert_eq!(
        RequestMachine::transition(&mac.wait, start(reply)),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        mac.wait_event(RequestEvent::WriteFinished {
            responder: RESPONDER,
            outcome: WriterOutcome::OutcomeUnknown,
        }),
        [
            RequestEffect::ReleaseBinding {
                responder: RESPONDER
            },
            RequestEffect::WriterResolved {
                responder: RESPONDER,
                outcome: WriterOutcome::OutcomeUnknown
            },
        ]
    );
    assert!(!mac.wait.holds_place());
    no_second_release(&mac);
}
