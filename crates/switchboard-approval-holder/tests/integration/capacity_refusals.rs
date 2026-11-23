use approval_request_protocol::{HolderStopCause, RegisterRunReply};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::{
    run_gate::CallAttempt,
    waiting_request::{LocalNegative, ProviderReply, RequestFamily, WaitRefusal, WaitState},
};

use super::fixtures::{Mac, RESPONDER, RequestEffect, RequestEvent, RequestMachine, try_publish};

const CALL_WITHDREW: RequestEvent = RequestEvent::LocalStop {
    cause: HolderStopCause::CallWithdrawn,
};

#[test]
fn held_caller_withdrawal_releases_before_and_after_capacity_permit() {
    let disconnect = RequestEvent::LocalStop {
        cause: HolderStopCause::ProviderConnectionLost,
    };
    let provider_take_back = RequestEvent::ProviderWithdrew {
        responder: RESPONDER,
    };
    assert_capacity_withdrawal(
        RequestFamily::HeldCall,
        CALL_WITHDREW,
        [disconnect, provider_take_back],
    );
}

#[test]
fn provider_take_back_releases_before_and_after_capacity_permit() {
    assert_capacity_withdrawal(
        RequestFamily::ProviderAsk,
        RequestEvent::ProviderWithdrew {
            responder: RESPONDER,
        },
        [CALL_WITHDREW],
    );
}

fn assert_capacity_withdrawal<const CASES: usize>(
    family: RequestFamily,
    withdraw: RequestEvent,
    wrong_family: [RequestEvent; CASES],
) {
    for permitted in [false, true] {
        let mut mac = Mac::new();
        mac.wait = WaitState::at_capacity(family, RESPONDER);
        mac.welcome(1);
        mac.registered(1, RegisterRunReply::Registered {});
        let negative = ProviderReply::LocalNegative(LocalNegative::HolderAtCapacity);
        let permit = RequestEvent::TakeFinalPermit {
            responder: RESPONDER,
            reply: negative.clone(),
        };
        let start = RequestEvent::StartWrite {
            responder: RESPONDER,
            reply: negative.clone(),
        };
        if permitted {
            assert!(mac.wait_event(permit.clone()).is_empty());
        }
        assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &negative)));
        for event in &wrong_family {
            assert_eq!(
                RequestMachine::transition(&mac.wait, event.clone()),
                Err(TransitionError::Domain(
                    WaitRefusal::StopCauseForDifferentKind
                ))
            );
        }
        assert_eq!(mac.wait.claimed_reply(), Some((&RESPONDER, &negative)));
        assert_eq!(
            mac.wait_event(withdraw.clone()),
            [RequestEffect::ReleaseBinding {
                responder: RESPONDER
            }]
        );
        assert!(mac.wait.may_be_forgotten());
        assert!(mac.wait.claimed_reply().is_none());
        for event in [permit, start, withdraw.clone()] {
            assert_eq!(
                RequestMachine::transition(&mac.wait, event),
                Err(TransitionError::Stale)
            );
        }
        assert_eq!(
            try_publish(&mac.wait, &mac.gate, CallAttempt::new(1)),
            Err(TransitionError::Stale)
        );
    }
}
