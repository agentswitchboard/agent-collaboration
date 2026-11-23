use approval_request_protocol::{
    ProviderHolder, ProviderStopCause, QuestionField, QuestionFieldId, QuestionFields,
    QuestionOffer, Raise,
};
use state_machine_core::{StateMachine, TransitionError};
use switchboard_approval_holder::waiting_request::{
    LocalNegative, ProviderReply, RaiseRefusal, WaitEffect, WaitEvent, WaitState, WaitingRequest,
    WriterOutcome,
};
use switchboard_approval_ports::{ReplyableStopCause, TerminalRaiseRefusal};

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
        Raise::ProviderQuestion {
            request_id: "01890f2e-7b4c-7cc0-98c4-000000000001".parse()?,
            holder: ProviderHolder {
                session_id: "01890f2e-7b4c-7cc0-98c4-000000000002".parse()?,
                router_run: "01890f2e-7b4c-7cc0-98c4-000000000003".parse()?,
            },
            offer: QuestionOffer {
                prompt: "Confirm the ask".to_owned().try_into()?,
                fields: QuestionFields::new(vec![QuestionField::Boolean {
                    field_id: QuestionFieldId::new(1)?,
                    label: "Confirm".to_owned().try_into()?,
                    description: None,
                    required: true,
                }])?,
            },
        },
        RESPONDER,
    )?)
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
