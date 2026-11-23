use approval_request_protocol::{
    ApprovalOffer, CancelReason, DeliverEndingReply, DeliverEndingRequest, Ending, OfferedOption,
    OfferedOptionId, OfferedOptions, OptionEffect, OptionLifetime, ProviderHolder,
    ProviderStopCause, Raise, RaiseReply, StopReceiptEnding, StopReply, StopRequest,
};
use hegel::generators as gs;
use state_machine_core::{EventTag, StateMachine, StateTag, Transition, TransitionError};

use super::{
    LocalNegative, ProviderReply, RaiseRefusal, WaitEffect, WaitEvent, WaitState, WaitingRequest,
    WriterOutcome,
};
use crate::run_gate::{
    CallAttempt, CallKey, DialAttempt, EndingIngress, LinkAttempt, RunAuthority, RunGate,
    RunGateEvent, RunGateState,
};
use approval_request_protocol::RegisterRunReply;
use std::sync::OnceLock;

type Wait = WaitingRequest<'static, u8, u8>;
type State = WaitState<u8, u8>;
type Event = WaitEvent<'static, u8, u8>;
type Effect = WaitEffect<u8, u8>;
const RESPONDER: u8 = 3;
const STOP_CAUSE: ProviderStopCause = ProviderStopCause::TurnStopped {};

fn raise() -> Raise {
    Raise::ProviderApproval {
        request_id: "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap(),
        holder: ProviderHolder {
            session_id: "01890f2e-7b4c-7cc0-98c4-000000000003".parse().unwrap(),
            router_run: "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap(),
        },
        offer: ApprovalOffer {
            title: "Provider approval".to_owned().try_into().unwrap(),
            description: None,
            subject: None,
            options: OfferedOptions::new(vec![OfferedOption {
                id: OfferedOptionId::new(1).unwrap(),
                label: "Allow".to_owned().try_into().unwrap(),
                effect: OptionEffect::Allow,
                lasts: OptionLifetime::ThisRequest {},
            }])
            .unwrap(),
        },
    }
}

fn apply(state: &mut State, event: Event) -> Vec<Effect> {
    let event_tag = event.event_tag();
    let (next, effects) = Wait::transition(state, event).unwrap();
    assert!(Wait::transitions().contains(&Transition {
        from: state.state_tag(),
        event: event_tag,
        to: next.state_tag(),
        terminal: next.is_terminal(),
    }));
    *state = next;
    effects
}

fn call(connection: u8, attempt: u64) -> CallKey<u8> {
    CallKey {
        link: LinkAttempt {
            connection,
            dial: DialAttempt::new(u64::from(connection)),
        },
        attempt: CallAttempt::new(attempt),
    }
}

fn ready_gate(connection: u8) -> &'static RunGateState<u8, u8> {
    static GATES: [OnceLock<RunGateState<u8, u8>>; 2] = [const { OnceLock::new() }; 2];
    GATES[usize::from(connection - 1)].get_or_init(|| {
        let authority = RunAuthority {
            enrollment: 1u8,
            router_run: raise().router_run().clone(),
        };
        let key = call(connection, 0);
        let mut gate = RunGateState::new(authority.clone());
        for event in [
            RunGateEvent::DialStarted {
                link: key.link.clone(),
            },
            RunGateEvent::Welcome {
                authority,
                link: key.link.clone(),
            },
            RunGateEvent::PublishRegistration { call: key.clone() },
            RunGateEvent::RegistrationReply {
                call: key,
                reply: RegisterRunReply::Registered {},
            },
        ] {
            gate = RunGate::transition(&gate, event).unwrap().0;
        }
        gate
    })
}

fn publish(connection: u8, attempt: u64) -> Event {
    Event::Publish {
        ready: ready_gate(connection).ready_run().unwrap(),
        attempt: CallAttempt::new(attempt),
    }
}

fn deliver(request: DeliverEndingRequest) -> Event {
    Event::DeliverEnding {
        request,
        ingress: EndingIngress::Admitted(ready_gate(1).ready_run().unwrap()),
    }
}

fn bound() -> State {
    State::bound(raise(), RESPONDER).unwrap()
}

fn published() -> State {
    let mut state = bound();
    assert_eq!(
        apply(&mut state, publish(1, 1)),
        [Effect::Raise {
            call: call(1, 1),
            request: raise()
        }]
    );
    state
}

fn claimed() -> (State, DeliverEndingRequest, ProviderReply) {
    let mut state = published();
    let ending = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    let reply = ProviderReply::CentralEnding(ending.ending.clone());
    assert_eq!(
        apply(&mut state, deliver(ending.clone())),
        [Effect::EndingReply(DeliverEndingReply::Received {})]
    );
    assert_eq!(state.claimed_reply(), Some((&RESPONDER, &reply)));
    (state, ending, reply)
}

fn permit(state: &mut State, reply: &ProviderReply) {
    assert!(
        apply(
            state,
            Event::TakeFinalPermit {
                responder: RESPONDER,
                reply: reply.clone()
            }
        )
        .is_empty()
    );
}

fn start(reply: &ProviderReply) -> Event {
    Event::StartWrite {
        responder: RESPONDER,
        reply: reply.clone(),
    }
}

fn finish(outcome: WriterOutcome) -> Event {
    Event::WriteFinished {
        responder: RESPONDER,
        outcome,
    }
}

#[test]
fn finished_and_released_exact_offer_is_not_held_and_never_writes_twice() {
    let (mut state, ending, reply) = claimed();
    let different = DeliverEndingRequest::from_raise(
        &raise(),
        Ending::Cancelled {
            reason: CancelReason::RequesterGone {},
        },
    )
    .unwrap();
    let conflict = apply(&mut state, deliver(different));
    assert!(matches!(conflict.as_slice(),
        [Effect::EndingConflict { request_id, .. }, Effect::EndingReply(DeliverEndingReply::EndingDiffers {})]
            if request_id == &ending.request_id));
    assert_eq!(
        apply(&mut state, deliver(ending.clone())),
        [Effect::EndingReply(DeliverEndingReply::Received {})]
    );
    permit(&mut state, &reply);
    assert_eq!(
        apply(&mut state, start(&reply)),
        [Effect::Submit {
            responder: RESPONDER,
            reply: reply.clone()
        }]
    );
    assert_eq!(
        apply(&mut state, finish(WriterOutcome::WriteReturned)),
        [
            Effect::ReleaseBinding {
                responder: RESPONDER
            },
            Effect::WriterResolved {
                responder: RESPONDER,
                outcome: WriterOutcome::WriteReturned
            },
        ]
    );

    assert!(state.is_closed());
    assert_eq!(
        apply(&mut state, deliver(ending)),
        [Effect::EndingReply(DeliverEndingReply::NotHeld {})],
        "the authoritative identical offer after release has no retained claim"
    );
    assert_eq!(
        Wait::transition(&state, start(&reply)),
        Err(TransitionError::Stale)
    );
}

#[hegel::test(test_cases = 240)]
fn one_capability_across_ending_local_negative_withdrawal_and_fence(case: hegel::TestCase) {
    let (mut state, _, ending) = claimed();
    assert_eq!(
        Wait::transition(
            &state,
            Event::TakeFinalPermit {
                responder: 4,
                reply: ending.clone()
            }
        ),
        Err(TransitionError::Stale),
    );
    permit(&mut state, &ending);

    let race = case.draw(gs::integers::<u8>().max_value(3));
    match race {
        0 => {
            let effects = apply(
                &mut state,
                Event::CapabilityWithdrawn {
                    responder: RESPONDER,
                },
            );
            assert_eq!(
                effects,
                [Effect::ReleaseBinding {
                    responder: RESPONDER
                }]
            );
            assert_eq!(
                Wait::transition(&state, start(&ending)),
                Err(TransitionError::Stale),
                "withdrawal before start must prevent the submission"
            );
        }
        1 => {
            let effects = apply(&mut state, Event::AuthorityEnded);
            assert_eq!(
                Wait::transition(&state, start(&ending)),
                Err(TransitionError::Stale),
                "terminal authority before start must prevent the submission"
            );
            assert_eq!(
                effects,
                [Effect::ReleaseBinding {
                    responder: RESPONDER
                }]
            );
        }
        2 => {
            assert!(apply(&mut state, Event::LocalStop { cause: STOP_CAUSE }).is_empty());
            let negative = ProviderReply::LocalNegative(LocalNegative::Stopped(STOP_CAUSE));
            permit(&mut state, &negative);
            assert_eq!(
                Wait::transition(&state, start(&ending)),
                Err(TransitionError::Stale),
                "the displaced ending cannot spend the local negative's permit"
            );
            assert_eq!(
                apply(&mut state, start(&negative)),
                [Effect::Submit {
                    responder: RESPONDER,
                    reply: negative.clone()
                },]
            );
            assert_eq!(
                Wait::transition(&state, start(&negative)),
                Err(TransitionError::Stale),
                "the responder is already spent once the negative started"
            );
            apply(&mut state, finish(WriterOutcome::NotSubmitted));
        }
        _ => {
            assert_eq!(
                apply(&mut state, start(&ending)),
                [Effect::Submit {
                    responder: RESPONDER,
                    reply: ending.clone()
                },]
            );
            assert!(
                apply(&mut state, Event::LocalStop { cause: STOP_CAUSE }).is_empty(),
                "a local Stop cannot displace a started write"
            );
            assert_eq!(
                apply(&mut state, Event::AuthorityEnded),
                [Effect::ReleaseBinding {
                    responder: RESPONDER
                }]
            );
            assert_eq!(
                Wait::transition(&state, start(&ending)),
                Err(TransitionError::Stale)
            );
            assert_eq!(
                apply(&mut state, finish(WriterOutcome::OutcomeUnknown)),
                [Effect::WriterResolved {
                    responder: RESPONDER,
                    outcome: WriterOutcome::OutcomeUnknown
                },]
            );
        }
    }
    assert!(state.is_closed());
}

#[test]
fn a_writer_result_describes_only_the_local_submission() {
    for observed in [
        WriterOutcome::NotSubmitted,
        WriterOutcome::WriteReturned,
        WriterOutcome::OutcomeUnknown,
    ] {
        let (mut state, _, reply) = claimed();
        permit(&mut state, &reply);
        apply(&mut state, start(&reply));
        assert_eq!(
            apply(&mut state, finish(observed)),
            [
                Effect::ReleaseBinding {
                    responder: RESPONDER
                },
                Effect::WriterResolved {
                    responder: RESPONDER,
                    outcome: observed
                },
            ],
            "only the observed local writer result can be surfaced"
        );
        assert!(state.is_closed());
    }
}

#[test]
fn capacity_and_other_terminal_refusals_use_the_same_one_time_writer() {
    for (reply, refusal) in [
        (
            RaiseReply::TooManyOutstanding {},
            RaiseRefusal::TooManyOutstanding,
        ),
        (RaiseReply::AlreadyStopped {}, RaiseRefusal::AlreadyStopped),
        (RaiseReply::ContentsDiffer {}, RaiseRefusal::ContentsDiffer),
        (RaiseReply::SessionEnded {}, RaiseRefusal::SessionEnded),
    ] {
        let mut state = published();
        apply(
            &mut state,
            Event::RaiseReply {
                call: call(1, 1),
                reply,
            },
        );
        let negative = ProviderReply::LocalNegative(LocalNegative::RaiseRefused(refusal));
        assert_eq!(
            state.claimed_reply(),
            Some((&RESPONDER, &negative)),
            "a capacity refusal must claim the same responder as every terminal refusal"
        );
        assert_eq!(
            Wait::transition(&state, publish(1, 1)),
            Err(TransitionError::Stale)
        );
        permit(&mut state, &negative);
        assert_eq!(
            apply(&mut state, start(&negative)),
            [Effect::Submit {
                responder: RESPONDER,
                reply: negative
            }]
        );
        apply(&mut state, finish(WriterOutcome::WriteReturned));
        assert!(state.is_closed());
    }
    let mut state = State::unbound(RESPONDER);
    let negative = ProviderReply::LocalNegative(LocalNegative::SessionNotBound);
    assert_eq!(state.claimed_reply(), Some((&RESPONDER, &negative)));
    assert_eq!(
        Wait::transition(&state, publish(1, 1)),
        Err(TransitionError::Stale)
    );
    permit(&mut state, &negative);
    assert_eq!(
        apply(&mut state, start(&negative)),
        [Effect::Submit {
            responder: RESPONDER,
            reply: negative
        }]
    );
    apply(&mut state, finish(WriterOutcome::WriteReturned));
    assert!(state.is_closed());
}

#[test]
fn owed_stop_is_exact_after_local_write_lost_reply_and_reconnect() {
    let mut state = published();
    let frozen = raise();
    let stop = StopRequest::Provider {
        request_id: frozen.request_id().clone(),
        router_run: frozen.router_run().clone(),
        contents_digest: frozen.contents_digest().unwrap(),
        cause: STOP_CAUSE,
    };
    apply(&mut state, Event::LocalStop { cause: STOP_CAUSE });
    let offered = DeliverEndingRequest::from_raise(&frozen, Ending::TimedOut {}).unwrap();
    assert_eq!(
        apply(&mut state, deliver(offered)),
        [Effect::EndingReply(DeliverEndingReply::StopOwed {})]
    );
    assert_eq!(
        apply(&mut state, publish(1, 2)),
        [Effect::Stop {
            call: call(1, 2),
            request: stop.clone()
        }]
    );
    let local = ProviderReply::LocalNegative(LocalNegative::Stopped(STOP_CAUSE));
    permit(&mut state, &local);
    apply(&mut state, start(&local));
    apply(&mut state, finish(WriterOutcome::WriteReturned));
    assert!(
        !state.is_closed(),
        "provider completion cannot clear the exact Stop debt"
    );

    apply(
        &mut state,
        Event::ConnectionRetired {
            link: call(1, 2).link,
        },
    );
    assert_eq!(
        apply(&mut state, publish(2, 3)),
        [Effect::Stop {
            call: call(2, 3),
            request: stop
        }]
    );
    let authoritative = StopReply::Stands {
        ending: StopReceiptEnding::TimedOut {},
    };
    assert_eq!(
        Wait::transition(
            &state,
            Event::StopReply {
                call: call(1, 2),
                reply: authoritative.clone()
            }
        ),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        apply(
            &mut state,
            Event::StopReply {
                call: call(2, 3),
                reply: authoritative
            }
        ),
        [Effect::StopReconciled(StopReceiptEnding::TimedOut {})]
    );
    assert!(state.is_closed());
}

#[test]
fn wait_transition_matrix_reaches_exactly_its_declared_edges() {
    use approval_request_protocol::{ApprovalWindow, RaiseReceipt, RunRefusal};
    use state_machine_core::check_state_machine_transitions;

    let ending = DeliverEndingRequest::from_raise(&raise(), Ending::TimedOut {}).unwrap();
    let ready = ready_gate(1).ready_run().unwrap();
    let mut events = vec![
        publish(1, 1),
        publish(1, 2),
        publish(2, 3),
        deliver(ending.clone()),
        Event::DeliverEnding {
            request: ending.clone(),
            ingress: EndingIngress::Discard,
        },
        Event::LocalStop { cause: STOP_CAUSE },
        Event::LocalStop {
            cause: ProviderStopCause::ProviderConnectionLost {},
        },
        Event::ProviderWithdrew {
            responder: RESPONDER,
        },
        Event::CapabilityWithdrawn {
            responder: RESPONDER,
        },
        finish(WriterOutcome::WriteReturned),
        Event::AuthorityEnded,
    ];
    for key in [call(1, 1), call(1, 2), call(2, 3)] {
        for reply in [
            RaiseReply::Receipt {
                receipt: RaiseReceipt {
                    request_id: raise().request_id().clone(),
                    window: ApprovalWindow::from_first_save(
                        "2026-11-22T17:00:00Z".parse().unwrap(),
                    )
                    .unwrap(),
                },
            },
            RaiseReply::TooManyOutstanding {},
            RaiseReply::AuthorityRefused {
                refusal: RunRefusal::RunRetired {},
            },
        ] {
            events.push(Event::RaiseReply {
                call: key.clone(),
                reply,
            });
        }
        for reply in [
            StopReply::Stands {
                ending: StopReceiptEnding::TimedOut {},
            },
            StopReply::NotVisible {},
            StopReply::AuthorityRefused {
                refusal: RunRefusal::RunNotRegistered {},
            },
        ] {
            events.push(Event::StopReply {
                call: key.clone(),
                reply,
            });
        }
        events.push(Event::CallEndedWithoutReply { call: key.clone() });
        events.push(Event::ConnectionRetired { link: key.link });
    }
    for reply in [
        ProviderReply::CentralEnding(ending.ending),
        ProviderReply::LocalNegative(LocalNegative::Stopped(STOP_CAUSE)),
        ProviderReply::LocalNegative(LocalNegative::HolderAtCapacity),
        ProviderReply::LocalNegative(LocalNegative::RaiseRefused(
            RaiseRefusal::TooManyOutstanding,
        )),
    ] {
        events.push(Event::TakeFinalPermit {
            responder: RESPONDER,
            reply: reply.clone(),
        });
        events.push(start(&reply));
    }
    assert_eq!(ready.router_run(), raise().router_run());

    let mut states = vec![bound(), State::at_capacity(RESPONDER)];
    let mut index = 0;
    while index < states.len() {
        for event in &events {
            if let Ok((next, _)) = Wait::transition(&states[index], event.clone())
                && !states.contains(&next)
            {
                states.push(next);
            }
        }
        index += 1;
    }
    let cases = states.into_iter().flat_map(|state| {
        events
            .iter()
            .cloned()
            .map(move |event| (state.clone(), event))
    });
    check_state_machine_transitions::<Wait>(cases).unwrap();
}
