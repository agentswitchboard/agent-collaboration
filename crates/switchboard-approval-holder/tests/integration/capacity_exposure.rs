use approval_request_protocol::{
    CallStopCause, CancelReason, HolderStopCause, ProviderStopCause, Raise, RaiseReply,
    RegisterRunReply, StopReceiptEnding, StopReply, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{
    LinkAttemptDisposition, LinkCallAnswer, LocalNegative, ProviderReply, RaiseRefusal,
    WaitRefusal, WriterOutcome,
};
use switchboard_run_gate::CallAttempt;

use super::fixtures::{
    Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, authority, call, call_raise,
    raise, try_publish,
};

const CAPACITY: RaiseReply = RaiseReply::TooManyOutstanding {};
const NEGATIVE: ProviderReply = ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
    RaiseRefusal::TooManyOutstanding,
));

fn register(mac: &mut Mac, connection: u8) {
    mac.welcome(connection);
    mac.registered(connection, RegisterRunReply::Registered {});
}

fn assert_raise(mac: &mut Mac, request: &Raise, connection: u8, attempt: u64) {
    assert_eq!(
        mac.publish(),
        [RequestEffect::Raise {
            call: call(connection, attempt),
            authority: authority(),
            request: request.clone(),
        }]
    );
}

fn capacity_reply(connection: u8, attempt: u64) -> RequestEvent {
    RequestEvent::AttemptSettled {
        call: call(connection, attempt),
        disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(CAPACITY)),
    }
}

fn assert_no_publication(mac: &Mac) {
    assert_eq!(
        try_publish(&mac.wait, &mac.gate, CallAttempt::new(99)),
        Err(TransitionError::Stale)
    );
}

fn permit() -> RequestEvent {
    RequestEvent::TakeFinalPermit {
        responder: RESPONDER,
        reply: NEGATIVE,
    }
}

fn start() -> RequestEvent {
    RequestEvent::StartWrite {
        responder: RESPONDER,
        reply: NEGATIVE,
    }
}

fn start_negative(mac: &mut Mac) {
    assert!(mac.wait_event(permit()).is_empty());
    assert_eq!(
        mac.wait_event(start()),
        [RequestEffect::Submit {
            responder: RESPONDER,
            reply: NEGATIVE
        }]
    );
}

fn finish_negative(mac: &mut Mac, outcome: WriterOutcome) {
    assert_eq!(
        mac.wait_event(RequestEvent::WriteFinished {
            responder: RESPONDER,
            outcome
        }),
        [
            RequestEffect::ReleaseBinding {
                responder: RESPONDER
            },
            RequestEffect::WriterResolved {
                responder: RESPONDER,
                outcome
            },
        ]
    );
}

fn pending_retry(request: &Raise) -> Mac {
    let mut mac = Mac::with_raise(request.clone());
    register(&mut mac, 1);
    assert_raise(&mut mac, request, 1, 1);
    mac.lose_answer(1, 1);
    mac.lose_connection(1);
    register(&mut mac, 2);
    assert_raise(&mut mac, request, 2, 2);
    mac
}

fn select_capacity_negative(mac: &mut Mac) {
    assert!(mac.wait_event(capacity_reply(2, 2)).is_empty());
    assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &NEGATIVE)));
}

fn provider_stop(raise: &Raise, cause: ProviderStopCause) -> StopRequest {
    StopRequest::Provider {
        request_id: raise.request_id().clone(),
        router_run: raise.router_run().clone(),
        contents_digest: raise.contents_digest().unwrap(),
        cause,
    }
}

fn call_stop(raise: &Raise, cause: CallStopCause) -> StopRequest {
    StopRequest::UnprovenCall {
        request_id: raise.request_id().clone(),
        router_run: raise.router_run().clone(),
        contents_digest: raise.contents_digest().unwrap(),
        cause,
    }
}

fn cleanup_stop(raise: &Raise) -> StopRequest {
    match raise {
        Raise::ProviderApproval { .. } | Raise::ProviderQuestion { .. } => {
            provider_stop(raise, ProviderStopCause::HolderRefusedLocally)
        }
        Raise::UnprovenCall { .. } => call_stop(raise, CallStopCause::HolderRefusedLocally),
    }
}

fn stop_effect(request: StopRequest, connection: u8, attempt: u64) -> RequestEffect {
    RequestEffect::Stop {
        call: call(connection, attempt),
        authority: authority(),
        request,
    }
}

fn answer_stop(connection: u8, attempt: u64, reply: StopReply) -> RequestEvent {
    RequestEvent::AttemptSettled {
        call: call(connection, attempt),
        disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Stop(reply)),
    }
}

#[test]
fn refusal_selection_owes_cleanup_even_if_the_pipe_breaks_before_start() {
    for (request, disconnected) in [
        (
            raise(),
            RequestEvent::LocalStop {
                cause: HolderStopCause::ProviderConnectionLost,
            },
        ),
        (
            call_raise(),
            RequestEvent::CapabilityWithdrawn {
                responder: RESPONDER,
            },
        ),
    ] {
        for permitted in [false, true] {
            let mut mac = pending_retry(&request);
            select_capacity_negative(&mut mac);
            let owed = stop_effect(cleanup_stop(&request), 2, 3);
            assert_eq!(
                try_publish(&mac.wait, &mac.gate, CallAttempt::new(3))
                    .unwrap()
                    .1
                    .as_slice(),
                std::slice::from_ref(&owed),
                "the negative selection already owes Stop before the pipe breaks"
            );
            if permitted {
                assert!(mac.wait_event(permit()).is_empty());
            }
            assert_eq!(
                mac.wait_event(disconnected.clone()),
                [RequestEffect::ReleaseBinding {
                    responder: RESPONDER
                }]
            );
            assert_eq!(
                RequestMachine::transition(&mac.wait, start()),
                Err(TransitionError::Stale)
            );
            assert_eq!(mac.publish(), [owed]);
            assert_no_publication(&mac);
            let ending = StopReceiptEnding::Cancelled {
                reason: CancelReason::Stopped {
                    cause: HolderStopCause::HolderRefusedLocally,
                },
            };
            let matched = answer_stop(2, 3, StopReply::Stands { ending });
            assert!(!mac.wait.may_be_forgotten());
            assert_eq!(
                mac.wait_event(matched.clone()),
                [RequestEffect::StopReconciled(ending)]
            );
            assert!(mac.wait.may_be_forgotten());
            assert_eq!(
                RequestMachine::transition(&mac.wait, matched),
                Err(TransitionError::Stale)
            );
        }
    }
}

#[test]
fn failed_or_completed_writer_keeps_one_frozen_cleanup_stop_across_reconnect() {
    for request in [raise(), call_raise()] {
        for outcome in [
            WriterOutcome::NotSubmitted,
            WriterOutcome::OutcomeUnknown,
            WriterOutcome::WriteReturned,
        ] {
            let mut mac = pending_retry(&request);
            select_capacity_negative(&mut mac);
            start_negative(&mut mac);
            let stop = cleanup_stop(&request);
            assert_eq!(mac.publish(), [stop_effect(stop.clone(), 2, 3)]);
            finish_negative(&mut mac, outcome);
            mac.lose_answer(2, 3);
            assert_eq!(
                try_publish(&mac.wait, &mac.gate, CallAttempt::new(3)),
                Err(TransitionError::Stale),
                "a settled Stop key cannot publish again"
            );
            mac.lose_connection(2);
            register(&mut mac, 3);
            assert_eq!(mac.publish(), [stop_effect(stop, 3, 4)]);
            assert_no_publication(&mac);
            assert_eq!(
                RequestMachine::transition(&mac.wait, answer_stop(2, 3, StopReply::NotVisible {})),
                Err(TransitionError::Stale)
            );
            assert_eq!(
                RequestMachine::transition(
                    &mac.wait,
                    RequestEvent::AttemptSettled {
                        call: call(1, 1),
                        disposition: LinkAttemptDisposition::Answered(LinkCallAnswer::Raise(
                            RaiseReply::AlreadyStopped {}
                        )),
                    }
                ),
                Err(TransitionError::Stale)
            );
            assert!(!mac.wait.may_be_forgotten());
            assert_eq!(
                mac.wait_event(answer_stop(3, 4, StopReply::NotVisible {})),
                [RequestEffect::StopNotVisible {
                    request_id: request.request_id().clone(),
                    contents_digest: request.contents_digest().unwrap(),
                },]
            );
            assert!(mac.wait.may_be_forgotten());
        }
    }
}

#[test]
fn connection_loss_or_take_back_before_selection_preserves_its_own_cause() {
    let provider = raise();
    let parked = call_raise();
    for (request, local_stop, expected) in [
        (
            &provider,
            RequestEvent::LocalStop {
                cause: HolderStopCause::ProviderConnectionLost,
            },
            provider_stop(&provider, ProviderStopCause::ProviderConnectionLost),
        ),
        (
            &provider,
            RequestEvent::ProviderWithdrew {
                responder: RESPONDER,
            },
            provider_stop(&provider, ProviderStopCause::AskWithdrawn),
        ),
        (
            &parked,
            RequestEvent::LocalStop {
                cause: HolderStopCause::CallWithdrawn,
            },
            call_stop(&parked, CallStopCause::CallWithdrawn),
        ),
    ] {
        let mut mac = pending_retry(request);
        assert_eq!(
            mac.wait_event(local_stop),
            [RequestEffect::ReleaseBinding {
                responder: RESPONDER
            }]
        );
        assert!(mac.wait_event(capacity_reply(2, 2)).is_empty());
        assert!(mac.wait.claimed_reply().is_none());
        assert_eq!(mac.publish(), [stop_effect(expected.clone(), 2, 3)]);
        assert_no_publication(&mac);
        let ending = StopReceiptEnding::Cancelled {
            reason: CancelReason::Stopped {
                cause: expected.cause(),
            },
        };
        assert_eq!(
            mac.wait_event(answer_stop(2, 3, StopReply::Stands { ending })),
            [RequestEffect::StopReconciled(ending)]
        );
        assert!(mac.wait.may_be_forgotten());
    }
}

#[test]
fn first_capacity_refusal_completes_without_an_owed_stop() {
    for request in [raise(), call_raise()] {
        let mut mac = Mac::with_raise(request.clone());
        register(&mut mac, 1);
        assert_raise(&mut mac, &request, 1, 1);
        assert!(mac.wait_event(capacity_reply(1, 1)).is_empty());
        assert_no_publication(&mac);
        start_negative(&mut mac);
        finish_negative(&mut mac, WriterOutcome::WriteReturned);
        assert!(mac.wait.may_be_forgotten());
        assert_no_publication(&mac);
    }
}

#[test]
fn outside_local_stop_cannot_assert_holder_refused_locally() {
    for request in [raise(), call_raise()] {
        let mut mac = pending_retry(&request);
        let outside = RequestEvent::LocalStop {
            cause: HolderStopCause::HolderRefusedLocally,
        };
        let refusal = Err(TransitionError::Domain(
            WaitRefusal::HolderRefusedLocallyRequiresRefusalSelection,
        ));
        assert_eq!(
            RequestMachine::transition(&mac.wait, outside.clone()),
            refusal
        );
        select_capacity_negative(&mut mac);
        assert_eq!(RequestMachine::transition(&mac.wait, outside), refusal);
        assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &NEGATIVE)));
    }
}
