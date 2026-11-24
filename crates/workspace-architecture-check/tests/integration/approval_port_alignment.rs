use approval_request_protocol::{
    HolderStopCause, ProviderHolder, ProviderStopCause, QuestionField, QuestionFieldId,
    QuestionFields, QuestionOffer, Raise, RegisterRunReply, StopReceiptEnding, StopRequest,
};
use state_machine_core::{StateMachine, TransitionError};
use std::num::NonZeroU64;
use switchboard_approval_holder::run_gate::{
    CallAttempt, CallKey, DialAttempt, LinkAttempt, RunAuthority, RunGate, RunGateEvent,
    RunGateRefusal, RunGateState,
};
use switchboard_approval_holder::waiting_request::{
    LinkAttemptDisposition, LocalNegative, ProviderReply, RaiseRefusal, WaitEffect, WaitEvent,
    WaitRefusal, WaitState, WaitingRequest, WriterOutcome,
    gate_access::{GateWaitEvent, step_under_gate},
};
use switchboard_approval_ports::{
    ApprovalScope, ApprovalTurnId, ProviderAskOffer, ReplyableStopCause, TerminalRaiseRefusal,
    WaitingRequestId, approval_value_builders,
};

macro_rules! raise_refusal_pairs {
    ($($case:ident),+ $(,)?) => {{
        fn ports_case(reason: RaiseRefusal) -> TerminalRaiseRefusal {
            match reason {
                $(RaiseRefusal::$case => TerminalRaiseRefusal::$case),+,
                RaiseRefusal::AlreadyEnded(ending) => TerminalRaiseRefusal::AlreadyEnded(ending),
            }
        }
        fn holder_case(reason: TerminalRaiseRefusal) -> RaiseRefusal {
            match reason {
                $(TerminalRaiseRefusal::$case => RaiseRefusal::$case),+,
                TerminalRaiseRefusal::AlreadyEnded(ending) => RaiseRefusal::AlreadyEnded(ending),
            }
        }
        $(assert_eq!(ports_case(RaiseRefusal::$case), TerminalRaiseRefusal::$case);
          assert_eq!(holder_case(TerminalRaiseRefusal::$case), RaiseRefusal::$case);)+
        let ended = StopReceiptEnding::TimedOut {};
        assert_eq!(
            ports_case(RaiseRefusal::AlreadyEnded(ended)),
            TerminalRaiseRefusal::AlreadyEnded(ended)
        );
        assert_eq!(
            holder_case(TerminalRaiseRefusal::AlreadyEnded(ended)),
            RaiseRefusal::AlreadyEnded(ended)
        );
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
    let stop_request = StopRequest::Provider {
        request_id: raise.request_id().clone(),
        router_run: raise.router_run().clone(),
        contents_digest: raise.contents_digest().unwrap(),
        cause: ProviderStopCause::TurnStopped,
    };
    let raise_call = CallKey {
        link: link.clone(),
        attempt: CallAttempt::new(1),
    };
    let wait = WaitState::bound(raise.clone(), responder.clone()).unwrap();
    let (raising, raises) = step_under_gate(
        &wait,
        &gate,
        GateWaitEvent::Publish {
            authority: authority.clone(),
            call: raise_call.clone(),
        },
    )
    .unwrap();
    assert_eq!(
        raises,
        [WaitEffect::Raise {
            call: raise_call.clone(),
            authority: authority.clone(),
            request: raise,
        }]
    );
    let (awaiting_disposition, effects) = ExactWait::transition(
        &raising,
        WaitEvent::LocalStop {
            cause: HolderStopCause::TurnStopped,
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
    let stop_call = CallKey {
        link,
        attempt: CallAttempt::new(2),
    };
    let publish_stop = || GateWaitEvent::Publish {
        authority: authority.clone(),
        call: stop_call.clone(),
    };
    assert_eq!(
        step_under_gate(&awaiting_disposition, &gate, publish_stop()),
        Err(TransitionError::Stale)
    );
    let (stopping, effects) = ExactWait::transition(
        &awaiting_disposition,
        WaitEvent::AttemptSettled {
            call: raise_call,
            disposition: LinkAttemptDisposition::AnswerLost,
        },
    )
    .unwrap();
    assert!(effects.is_empty());
    let (_, expected_stop) = step_under_gate(&stopping, &gate, publish_stop()).unwrap();
    assert_eq!(
        expected_stop,
        [WaitEffect::Stop {
            call: stop_call.clone(),
            authority: authority.clone(),
            request: stop_request,
        }]
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
            ProviderStopCause::ProviderConnectionLost
            | ProviderStopCause::AskWithdrawn
            | ProviderStopCause::HolderRefusedLocally => None,
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
            WaitEvent::LocalStop {
                cause: cause.holder_cause(),
            }
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
    let cause = ProviderStopCause::HolderRefusedLocally;
    assert_eq!(replyable_cause(cause), None);
    assert_eq!(
        HolderWait::transition(
            &bound_wait().unwrap(),
            WaitEvent::LocalStop {
                cause: cause.holder_cause()
            }
        ),
        Err(TransitionError::Domain(
            WaitRefusal::HolderRefusedLocallyRequiresRefusalSelection
        ))
    );
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
