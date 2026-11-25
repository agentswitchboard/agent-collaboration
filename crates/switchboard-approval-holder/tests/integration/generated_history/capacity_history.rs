use approval_request_protocol::{
    CancelReason, HolderStopCause, RaiseReply, RegisterRunReply, StopReceiptEnding, StopReply,
};
use switchboard_approval_holder::waiting_request::{
    LocalNegative, ProviderReply, RaiseRefusal, WriterOutcome,
};
use switchboard_run_gate::CallAttempt;

use super::super::fixtures::{GateEvent, RESPONDER, RequestEffect, authority, call, link};
use super::{
    HistoryFacts, Observation, Product, WaitInput, assert_observations, registered_history,
};

fn compare_step(
    actual: &mut Product,
    facts: &mut HistoryFacts,
    input: WaitInput,
) -> Option<Vec<Observation>> {
    let expected = facts.wait(&input);
    assert_eq!(actual.wait(&input), expected, "{input:?}");
    assert_observations(actual, facts);
    expected
}

pub(super) fn check(call_wait: bool, withdrew: bool) {
    let (mut actual, mut facts) = registered_history(call_wait);
    for input in [
        WaitInput::Publish(CallAttempt::new(2)),
        WaitInput::AnswerLost(call(1, 2)),
    ] {
        assert!(compare_step(&mut actual, &mut facts, input).is_some());
    }
    for event in [
        GateEvent::ConnectionLost { link: link(1) },
        GateEvent::DialStarted { link: link(2) },
        GateEvent::Welcome {
            authority: authority(),
            link: link(2),
        },
        GateEvent::PublishRegistration { call: call(2, 0) },
        GateEvent::RegistrationReply {
            call: call(2, 0),
            reply: RegisterRunReply::Registered {},
        },
    ] {
        assert_eq!(actual.gate(event.clone()), facts.gate(&event), "{event:?}");
        assert_observations(&actual, &facts);
    }
    for input in [
        WaitInput::Publish(CallAttempt::new(3)),
        WaitInput::Raise {
            key: call(2, 3),
            reply: RaiseReply::TooManyOutstanding {},
        },
    ] {
        assert!(compare_step(&mut actual, &mut facts, input).is_some());
    }
    let negative = ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
        RaiseRefusal::TooManyOutstanding,
    ));
    assert!(
        compare_step(
            &mut actual,
            &mut facts,
            WaitInput::Permit {
                responder: RESPONDER,
                reply: negative.clone(),
            }
        )
        .is_some()
    );
    if withdrew {
        let input = if call_wait {
            WaitInput::LocalStop(HolderStopCause::CallWithdrawn)
        } else {
            WaitInput::ProviderWithdrew(RESPONDER)
        };
        assert!(compare_step(&mut actual, &mut facts, input).is_some());
    } else {
        for input in [
            WaitInput::Start {
                responder: RESPONDER,
                reply: negative,
            },
            WaitInput::Finish(WriterOutcome::WriteReturned),
        ] {
            assert!(compare_step(&mut actual, &mut facts, input).is_some());
        }
    }
    let cause = HolderStopCause::HolderRefusedLocally;
    let publication = compare_step(
        &mut actual,
        &mut facts,
        WaitInput::Publish(CallAttempt::new(4)),
    );
    assert!(
        matches!(publication.as_deref(), Some([Observation::Wait(RequestEffect::Stop { request, .. })]) if request.cause() == cause)
    );
    let ending = StopReceiptEnding::Cancelled {
        reason: CancelReason::Stopped { cause },
    };
    assert_eq!(
        compare_step(
            &mut actual,
            &mut facts,
            WaitInput::Stop {
                key: call(2, 4),
                reply: StopReply::Stands { ending }
            }
        ),
        Some(vec![Observation::Wait(RequestEffect::StopReconciled(
            ending
        ))])
    );
    assert!(actual.wait.may_be_forgotten());
}
