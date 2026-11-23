use approval_request_protocol::{
    ApprovalWindow, CallStopCause, CancelReason, HolderStopCause, Raise, RaiseReceipt, RaiseReply,
    RegisterRunReply, RunRefusal, StopReceiptEnding, StopReply, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::{CallAttempt, CallKey},
    waiting_request::{LinkAttemptDisposition, LinkCallAnswer},
};

use super::fixtures::{
    Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority, call, call_raise,
    raise, try_publish,
};

const CALL_WITHDREW: RequestEvent = RequestEvent::LocalStop {
    cause: HolderStopCause::CallWithdrawn,
};

fn waits() -> [(Raise, RequestEvent); 2] {
    [
        (call_raise(), CALL_WITHDREW),
        (
            raise(),
            RequestEvent::ProviderWithdrew {
                responder: RESPONDER,
            },
        ),
    ]
}

fn registered(mac: &mut Mac, connection: u8) {
    mac.welcome(connection);
    mac.registered(connection, RegisterRunReply::Registered {});
}

fn first_attempt(request: &Raise) -> Mac {
    let mut mac = Mac::with_raise(request.clone());
    registered(&mut mac, 1);
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(1, 1),
            authority: authority(),
            request: request.clone(),
        }]
    );
    mac
}

fn settle(call: CallKey<u8>, disposition: LinkAttemptDisposition) -> RequestEvent {
    RequestEvent::AttemptSettled { call, disposition }
}

fn expected_stop(request: &Raise) -> StopRequest {
    let request_id = request.request_id().clone();
    let router_run = request.router_run().clone();
    let contents_digest = request.contents_digest().unwrap();
    match request {
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => StopRequest::Provider {
            request_id,
            router_run,
            contents_digest,
            cause: approval_request_protocol::ProviderStopCause::AskWithdrawn,
        },
        Raise::UnprovenCall { .. } => StopRequest::UnprovenCall {
            request_id,
            router_run,
            contents_digest,
            cause: CallStopCause::CallWithdrawn,
        },
    }
}

fn assert_release(mac: &mut Mac, withdraw: RequestEvent) {
    assert_eq!(
        mac.wait_event(withdraw),
        [RequestEffect::ReleaseBinding {
            responder: RESPONDER
        }]
    );
}

fn assert_no_publication(mac: &Mac) {
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(9)),
        Err(TransitionError::Stale)
    );
}

#[test]
fn first_proven_unseen_raise_owes_no_stop_for_either_family() {
    for (request, withdraw) in waits() {
        let mut mac = first_attempt(&request);
        assert!(
            mac.wait_event(settle(call(1, 1), LinkAttemptDisposition::NotReached))
                .is_empty()
        );
        assert_release(&mut mac, withdraw);
        assert!(mac.wait.may_be_forgotten());
        assert_no_publication(&mac);
    }
}

#[test]
fn first_pre_admission_refusal_owes_no_stop_for_either_family() {
    for (request, withdraw) in waits() {
        let mut mac = first_attempt(&request);
        let refusal =
            LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(RaiseReply::AuthorityRefused {
                refusal: RunRefusal::RunNotRegistered {},
            }));
        assert!(matches!(
            mac.wait_event(settle(call(1, 1), refusal)).as_slice(),
            [RequestEffect::RunRefused(_)]
        ));
        assert_release(&mut mac, withdraw);
        registered(&mut mac, 2);
        assert_no_publication(&mac);
        assert!(mac.wait.may_be_forgotten());
    }
}

#[test]
fn withdrawal_awaits_the_exact_first_disposition_even_across_retirement() {
    for (request, withdraw) in waits() {
        let mut mac = first_attempt(&request);
        assert_release(&mut mac, withdraw);
        assert_no_publication(&mac);
        mac.lose_connection(1);
        registered(&mut mac, 2);
        assert!(mac.wait.has_pending_calls());
        assert_no_publication(&mac);
        assert_eq!(
            RequestMachine::transition(
                &mac.wait,
                settle(call(2, 1), LinkAttemptDisposition::NotReached)
            ),
            Err(TransitionError::Stale)
        );
        assert!(
            mac.wait_event(settle(call(1, 1), LinkAttemptDisposition::NotReached))
                .is_empty()
        );
        assert!(mac.wait.may_be_forgotten());
        assert_no_publication(&mac);
    }
}

#[test]
fn withdrawal_while_first_raise_is_pending_owes_stop_when_answer_is_lost() {
    for (request, withdraw) in waits() {
        let mut mac = first_attempt(&request);
        assert_release(&mut mac, withdraw);
        assert_no_publication(&mac);
        assert!(
            mac.wait_event(settle(call(1, 1), LinkAttemptDisposition::AnswerLost))
                .is_empty()
        );
        assert_eq!(
            mac.publish(),
            [RequestEffect::Stop {
                call: call(1, 2),
                authority: authority(),
                request: expected_stop(&request),
            }]
        );
    }
}

#[test]
fn earlier_answer_loss_keeps_the_stop_after_a_later_unseen_raise() {
    for (request, withdraw) in waits() {
        let mut mac = first_attempt(&request);
        mac.wait_event(settle(call(1, 1), LinkAttemptDisposition::AnswerLost));
        assert_eq!(
            mac.publish(),
            [RequestEffect::Raise {
                call: call(1, 2),
                authority: authority(),
                request: request.clone(),
            }]
        );
        mac.wait_event(settle(call(1, 2), LinkAttemptDisposition::NotReached));
        assert_release(&mut mac, withdraw);
        assert_eq!(
            mac.publish(),
            [RequestEffect::Stop {
                call: call(1, 3),
                authority: authority(),
                request: expected_stop(&request),
            }]
        );
    }
}

#[test]
fn seen_call_replays_stop_and_only_the_current_standing_cancellation_settles_it() {
    let request = call_raise();
    let mut mac = first_attempt(&request);
    mac.wait_event(settle(
        call(1, 1),
        LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(RaiseReply::Receipt {
            receipt: RaiseReceipt {
                request_id: request.request_id().clone(),
                window: ApprovalWindow::from_first_save("2026-11-22T17:00:00Z".parse().unwrap())
                    .unwrap(),
            },
        })),
    ));
    assert_release(&mut mac, CALL_WITHDREW);
    let stop = expected_stop(&request);
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(1, 2),
            authority: authority(),
            request: stop.clone(),
        }]
    );

    mac.wait_event(settle(call(1, 2), LinkAttemptDisposition::AnswerLost));
    mac.lose_connection(1);
    registered(&mut mac, 2);
    assert_eq!(
        mac.publish(),
        [RequestEffect::Stop {
            call: call(2, 3),
            authority: authority(),
            request: stop,
        }]
    );

    let ending = StopReceiptEnding::Cancelled {
        reason: CancelReason::Stopped {
            cause: HolderStopCause::CallWithdrawn,
        },
    };
    let stands =
        LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(StopReply::Stands { ending }));
    assert_eq!(
        RequestMachine::transition(&mac.wait, settle(call(1, 2), stands.clone())),
        Err(TransitionError::Stale)
    );
    let current = settle(call(2, 3), stands);
    assert_eq!(
        mac.wait_event(current.clone()),
        [RequestEffect::StopReconciled(ending)]
    );
    assert!(mac.wait.may_be_forgotten());
    assert_eq!(
        RequestMachine::transition(&mac.wait, current),
        Err(TransitionError::Stale)
    );
}
