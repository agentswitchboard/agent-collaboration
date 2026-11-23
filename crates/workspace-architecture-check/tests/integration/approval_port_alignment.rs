use approval_request_protocol::{
    ProviderHolder, ProviderStopCause, QuestionField, QuestionFieldId, QuestionFields,
    QuestionOffer, Raise, RegisterRunReply,
};
use state_machine_core::{StateMachine, TransitionError};
use std::num::NonZeroU64;
use switchboard_approval_holder::run_gate::{
    CallAttempt, CallKey, DialAttempt, LinkAttempt, RunAuthority, RunGate, RunGateEvent,
    RunGateRefusal, RunGateState,
};
use switchboard_approval_holder::waiting_request::{
    LocalNegative, ProviderReply, RaiseRefusal, WaitEffect, WaitEvent, WaitState, WaitingRequest,
    WriterOutcome,
    gate_access::{GateWaitEvent, step_under_gate},
};
use switchboard_approval_ports::{
    ApprovalScope, ApprovalTurnId, ProviderAskOffer, ReplyableStopCause, TerminalRaiseRefusal,
    WaitingRequestId, approval_value_builders,
};

macro_rules! raise_refusal_pairs {
    ($($case:ident),+ $(,)?) => {{
        fn ports_case(reason: RaiseRefusal) -> TerminalRaiseRefusal {
            match reason { $(RaiseRefusal::$case => TerminalRaiseRefusal::$case),+ }
        }
        fn holder_case(reason: TerminalRaiseRefusal) -> RaiseRefusal {
            match reason { $(TerminalRaiseRefusal::$case => RaiseRefusal::$case),+ }
        }
        $(assert_eq!(ports_case(RaiseRefusal::$case), TerminalRaiseRefusal::$case);
          assert_eq!(holder_case(TerminalRaiseRefusal::$case), RaiseRefusal::$case);)+
    }};
}

#[test]
fn terminal_raise_refusals_exhaust_both_domains() {
    raise_refusal_pairs!(
        AlreadyStopped,
        SessionEnded,
        ContentsDiffer,
        TooManyOutstanding
    );
}

const RESPONDER: u8 = 1;
type HolderWait = WaitingRequest<u8, u8, u8>;

fn bound_wait() -> Result<WaitState<u8, u8>, Box<dyn std::error::Error>> {
    Ok(WaitState::bound(
        question_raise(question_offer()?)?,
        RESPONDER,
    )?)
}

fn question_raise(offer: QuestionOffer) -> Result<Raise, Box<dyn std::error::Error>> {
    Ok(Raise::ProviderQuestion {
        request_id: "01890f2e-7b4c-7cc0-98c4-000000000001".parse()?,
        holder: ProviderHolder {
            session_id: "01890f2e-7b4c-7cc0-98c4-000000000002".parse()?,
            router_run: "01890f2e-7b4c-7cc0-98c4-000000000003".parse()?,
        },
        offer,
    })
}

fn question_offer() -> Result<QuestionOffer, Box<dyn std::error::Error>> {
    Ok(QuestionOffer {
        prompt: "Confirm the ask".to_owned().try_into()?,
        fields: QuestionFields::new(vec![QuestionField::Boolean {
            field_id: QuestionFieldId::new(1)?,
            label: "Confirm".to_owned().try_into()?,
            description: None,
            required: true,
        }])?,
    })
}

fn registered_gate(
    authority: &RunAuthority<u8>,
    link: &LinkAttempt<u8>,
) -> Result<RunGateState<u8, u8>, TransitionError<RunGateRefusal>> {
    let registration = CallKey {
        link: link.clone(),
        attempt: CallAttempt::new(0),
    };
    let mut gate = RunGateState::new(authority.clone());
    for event in [
        RunGateEvent::DialStarted { link: link.clone() },
        RunGateEvent::Welcome {
            authority: authority.clone(),
            link: link.clone(),
        },
        RunGateEvent::PublishRegistration {
            call: registration.clone(),
        },
        RunGateEvent::RegistrationReply {
            call: registration,
            reply: RegisterRunReply::Registered {},
        },
    ] {
        gate = RunGate::transition(&gate, event)?.0;
    }
    Ok(gate)
}

#[test]
fn pre_start_handle_loss_releases_the_responder_and_keeps_the_exact_stop_due() {
    type ExactWait = WaitingRequest<WaitingRequestId, u8, u8>;
    let (harness, _, owner) = approval_value_builders();
    let binding =
        owner.allocate_session_binding("01890f2e-7b4c-7cc0-98c4-000000000011".parse().unwrap());
    let scope = ApprovalScope::from_admitted_turn(
        binding,
        NonZeroU64::MIN,
        ApprovalTurnId::parse("01890f2e-7b4c-7cc0-98c4-000000000012").unwrap(),
    );
    let offer = question_offer().unwrap();
    let raise = question_raise(offer.clone()).unwrap();
    let ask = harness.admitted_ask(
        scope,
        "01890f2e-7b4c-7cc0-98c4-000000000013".parse().unwrap(),
        ProviderAskOffer::ProviderQuestion { offer },
    );
    let responder = ask.waiting_request().clone();
    let authority = RunAuthority {
        enrollment: 1,
        router_run: raise.router_run().clone(),
    };
    let link = LinkAttempt {
        connection: 1,
        dial: DialAttempt::new(1),
    };
    let gate = registered_gate(&authority, &link).unwrap();
    let wait = WaitState::bound(raise, responder.clone()).unwrap();
    let (raising, raises) = step_under_gate(
        &wait,
        &gate,
        GateWaitEvent::Publish {
            authority: authority.clone(),
            call: CallKey {
                link: link.clone(),
                attempt: CallAttempt::new(1),
            },
        },
    )
    .unwrap();
    assert!(matches!(raises.as_slice(), [WaitEffect::Raise { .. }]));
    let (stopping, effects) = ExactWait::transition(
        &raising,
        WaitEvent::LocalStop {
            cause: ProviderStopCause::TurnStopped,
        },
    )
    .unwrap();
    assert!(effects.is_empty());
    let reply =
        ProviderReply::LocalNegative(LocalNegative::Stopped(ProviderStopCause::TurnStopped));
    let permit = || WaitEvent::TakeFinalPermit {
        responder: responder.clone(),
        reply: reply.clone(),
    };
    let start = || WaitEvent::StartWrite {
        responder: responder.clone(),
        reply: reply.clone(),
    };
    let publish_stop = || GateWaitEvent::Publish {
        authority: authority.clone(),
        call: CallKey {
            link: link.clone(),
            attempt: CallAttempt::new(2),
        },
    };
    let (_, expected_stop) = step_under_gate(&stopping, &gate, publish_stop()).unwrap();
    assert!(
        matches!(expected_stop.as_slice(), [WaitEffect::Stop { request, .. }]
        if request.cause() == approval_request_protocol::HolderStopCause::TurnStopped)
    );
    let (permitted, effects) = ExactWait::transition(&stopping, permit()).unwrap();
    assert!(effects.is_empty());

    for before_start in [stopping, permitted] {
        let loss = harness.reply_handle_unavailable(responder.clone());
        let (released, effects) = ExactWait::transition(
            &before_start,
            WaitEvent::CapabilityWithdrawn {
                responder: loss.into_waiting_request(),
            },
        )
        .unwrap();
        assert_eq!(
            effects,
            [WaitEffect::ReleaseBinding {
                responder: responder.clone()
            }]
        );
        assert!(released.claimed_reply().is_none());
        assert_eq!(
            ExactWait::transition(&released, permit()),
            Err(TransitionError::Stale)
        );
        assert_eq!(
            ExactWait::transition(&released, start()),
            Err(TransitionError::Stale)
        );
        let (_, still_owed) = step_under_gate(&released, &gate, publish_stop()).unwrap();
        assert_eq!(still_owed, expected_stop);
    }
}

#[test]
fn replyable_stops_exhaust_the_holder_claims() {
    fn replyable_cause(cause: ProviderStopCause) -> Option<ReplyableStopCause> {
        match cause {
            ProviderStopCause::TurnStopped => Some(ReplyableStopCause::TurnStopped),
            ProviderStopCause::SessionClosed => Some(ReplyableStopCause::SessionClosed),
            ProviderStopCause::AskReplaced => Some(ReplyableStopCause::AskReplaced),
            ProviderStopCause::ProviderConnectionLost | ProviderStopCause::AskWithdrawn => None,
        }
    }
    fn provider_cause(cause: ReplyableStopCause) -> ProviderStopCause {
        match cause {
            ReplyableStopCause::TurnStopped => ProviderStopCause::TurnStopped,
            ReplyableStopCause::SessionClosed => ProviderStopCause::SessionClosed,
            ReplyableStopCause::AskReplaced => ProviderStopCause::AskReplaced,
        }
    }
    for cause in [
        ProviderStopCause::TurnStopped,
        ProviderStopCause::SessionClosed,
        ProviderStopCause::AskReplaced,
        ProviderStopCause::ProviderConnectionLost,
        ProviderStopCause::AskWithdrawn,
    ] {
        let state = bound_wait().unwrap();
        let event = if cause == ProviderStopCause::AskWithdrawn {
            WaitEvent::ProviderWithdrew {
                responder: RESPONDER,
            }
        } else {
            WaitEvent::LocalStop { cause }
        };
        let (stopped, _) = HolderWait::transition(&state, event).unwrap();
        let expected = replyable_cause(cause).map(|replyable| {
            assert_eq!(provider_cause(replyable), cause);
            ProviderReply::LocalNegative(LocalNegative::Stopped(cause))
        });
        assert_eq!(
            stopped.claimed_reply().map(|(_, reply)| reply),
            expected.as_ref()
        );
    }
}

#[test]
fn withdrawal_before_start_blocks_submission_but_fence_after_start_keeps_its_outcome() {
    let refused = WaitState::unbound(RESPONDER);
    let reply = ProviderReply::LocalNegative(LocalNegative::SessionNotBound);
    let (permitted, _) = HolderWait::transition(
        &refused,
        WaitEvent::TakeFinalPermit {
            responder: RESPONDER,
            reply: reply.clone(),
        },
    )
    .unwrap();
    let start = || WaitEvent::StartWrite {
        responder: RESPONDER,
        reply: reply.clone(),
    };
    let release = WaitEffect::ReleaseBinding {
        responder: RESPONDER,
    };
    let (withdrawn, effects) = HolderWait::transition(
        &permitted,
        WaitEvent::ProviderWithdrew {
            responder: RESPONDER,
        },
    )
    .unwrap();
    assert_eq!(effects.as_slice(), std::slice::from_ref(&release));
    assert_eq!(
        HolderWait::transition(&withdrawn, start()),
        Err(TransitionError::Stale)
    );

    let (started, effects) = HolderWait::transition(&permitted, start()).unwrap();
    assert_eq!(
        effects,
        [WaitEffect::Submit {
            responder: RESPONDER,
            reply: reply.clone()
        }]
    );
    let (fenced, effects) = HolderWait::transition(&started, WaitEvent::AuthorityEnded).unwrap();
    assert_eq!(effects, [release]);
    assert_eq!(
        HolderWait::transition(&fenced, start()),
        Err(TransitionError::Stale)
    );

    let finish = WaitEvent::WriteFinished {
        responder: RESPONDER,
        outcome: WriterOutcome::WriteReturned,
    };
    let (finished, effects) = HolderWait::transition(&fenced, finish.clone()).unwrap();
    assert_eq!(
        effects,
        [WaitEffect::WriterResolved {
            responder: RESPONDER,
            outcome: WriterOutcome::WriteReturned
        }]
    );
    assert_eq!(
        HolderWait::transition(&finished, finish),
        Err(TransitionError::Stale)
    );
}
