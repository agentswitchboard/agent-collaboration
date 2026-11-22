use hegel::generators as gs;
use state_machine_core::{EventTag, StateMachine, StateTag, Transition, TransitionError};

use super::{
    CarrierRunGate, EndingIngress, GateCallId, GateEffect, GateEvent, GateState, RunAuthority,
    TerminalAuthority,
};
use approval_request_protocol::{AuthorityProbeReply, DeliverEndingReply, RegisterRunReply};

type Gate = CarrierRunGate<u8, u8, u8>;
type State = GateState<u8, u8, u8>;
type Event = GateEvent<u8, u8, u8>;
type Effect = GateEffect<u8, u8>;

fn authority(enrollment: u8) -> RunAuthority<u8> {
    RunAuthority {
        enrollment,
        router_run: "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap(),
    }
}

fn apply(state: &mut State, event: Event) -> Vec<Effect> {
    let event_tag = event.event_tag();
    let (next, effects) = Gate::transition(state, event).unwrap();
    assert!(Gate::transitions().contains(&Transition {
        from: state.state_tag(),
        event: event_tag,
        to: next.state_tag(),
        terminal: next.is_terminal(),
    }));
    *state = next;
    effects
}

fn open_link(state: &mut State, dial: u8, carrier: u8) {
    assert!(apply(state, Event::DialStarted { dial }).is_empty());
    assert!(
        apply(
            state,
            Event::Welcome {
                dial,
                authority: authority(1),
                carrier
            }
        )
        .is_empty()
    );
}

fn register(state: &mut State, carrier: u8) -> GateCallId {
    apply(state, Event::PublishRegistration { carrier })
        .into_iter()
        .find_map(|effect| {
            if let Effect::PublishRegistration {
                carrier: outgoing,
                call,
                request,
            } = effect
            {
                assert_eq!(outgoing, carrier);
                assert_eq!(request.router_run, authority(1).router_run);
                Some(call)
            } else {
                None
            }
        })
        .unwrap()
}

fn probe(state: &mut State, carrier: u8) -> GateCallId {
    apply(state, Event::PublishProbe { carrier })
        .into_iter()
        .find_map(|effect| {
            if let Effect::PublishProbe {
                carrier: outgoing,
                call,
                request,
            } = effect
            {
                assert_eq!(outgoing, carrier);
                assert_eq!(request.router_run, authority(1).router_run);
                Some(call)
            } else {
                None
            }
        })
        .unwrap()
}

fn registered(carrier: u8, call: GateCallId) -> Event {
    Event::RegistrationReply {
        carrier,
        call,
        reply: RegisterRunReply::Registered {},
    }
}

#[hegel::test(test_cases = 300)]
fn only_the_live_attempt_on_the_current_carrier_opens_ingress(case: hegel::TestCase) {
    let mut state = State::new(authority(1));
    open_link(&mut state, 1, 1);
    let mut retired_calls = Vec::new();
    let mut expected_carrier = 1;
    let mut live_call = register(&mut state, expected_carrier);
    let retries = case.draw(gs::integers::<u8>().min_value(1).max_value(6));

    for dial in 2..=retries + 1 {
        retired_calls.push((expected_carrier, live_call));
        if case.draw(gs::booleans()) {
            // A local timeout on the same carrier must retire this attempt.
            apply(
                &mut state,
                Event::AttemptFailed {
                    carrier: expected_carrier,
                    call: live_call,
                },
            );
            let stale = Gate::transition(&state, registered(expected_carrier, live_call));
            assert_eq!(
                stale,
                Err(TransitionError::Stale),
                "a timed-out reply has no pending call"
            );
        } else {
            // A displaced carrier can still have already-published copies in flight.
            apply(
                &mut state,
                Event::CarrierLost {
                    carrier: expected_carrier,
                },
            );
            expected_carrier = dial;
            open_link(&mut state, dial, expected_carrier);
        }
        live_call = register(&mut state, expected_carrier);

        for &(source_carrier, ended_call) in &retired_calls {
            let stale = Gate::transition(&state, registered(source_carrier, ended_call));
            assert_eq!(
                stale,
                Err(TransitionError::Stale),
                "an ended attempt cannot open the gate"
            );
        }
        assert_eq!(
            state.ending_ingress(&authority(1), &expected_carrier),
            EndingIngress::Reply(DeliverEndingReply::HolderNotReady {}),
        );
    }

    // Taking one wire copy away leaves no observable gate input.
    let first_observation = registered(expected_carrier, live_call);
    let duplicate = first_observation.clone();
    let effects = apply(&mut state, first_observation);
    assert!(effects.contains(&Effect::AcknowledgedReplayDue));
    assert_eq!(
        state.ending_ingress(&authority(1), &expected_carrier),
        EndingIngress::Authoritative
    );
    assert_eq!(
        Gate::transition(&state, duplicate),
        Err(TransitionError::Stale)
    );
}

#[hegel::test(test_cases = 300)]
fn outage_epoch_survives_timeouts_and_welcome_until_matched_success(case: hegel::TestCase) {
    let mut state = State::new(authority(1));
    apply(&mut state, Event::DialStarted { dial: 1 });
    let effects = apply(&mut state, Event::DialFailed { dial: 1 });
    let first = state.outage().unwrap();
    assert_eq!(effects, [Effect::OutageStarted { epoch: first }]);

    for dial in 2..2 + case.draw(gs::integers::<u8>().max_value(5)) {
        assert!(apply(&mut state, Event::DialStarted { dial }).is_empty());
        assert!(apply(&mut state, Event::DialFailed { dial }).is_empty());
        assert_eq!(state.outage(), Some(first));
    }
    open_link(&mut state, 10, 8);
    assert_eq!(
        state.outage(),
        Some(first),
        "Welcome alone cannot end the outage"
    );
    let call = register(&mut state, 8);
    let effects = apply(&mut state, registered(8, call));
    assert_eq!(
        effects,
        [
            Effect::OutageEnded { epoch: first },
            Effect::AcknowledgedReplayDue
        ]
    );
    assert_eq!(state.outage(), None);

    let effects = apply(&mut state, Event::HolderAttemptFailed { carrier: 8 });
    let second = state.outage().unwrap();
    assert_ne!(second, first);
    assert_eq!(effects, [Effect::OutageStarted { epoch: second }]);

    let expired = probe(&mut state, 8);
    assert!(
        apply(
            &mut state,
            Event::AttemptFailed {
                carrier: 8,
                call: expired
            }
        )
        .is_empty()
    );
    let pending = probe(&mut state, 8);
    assert_ne!(pending, expired);
    let stale_replies = [(8, expired), (9, pending)];
    for (carrier, call) in stale_replies {
        assert_eq!(
            Gate::transition(
                &state,
                Event::ProbeReply {
                    carrier,
                    call,
                    reply: AuthorityProbeReply::Admitted {}
                }
            ),
            Err(TransitionError::Stale),
            "only a current correlated probe can end the outage",
        );
    }
    assert_eq!(state.outage(), Some(second));
    let effects = apply(
        &mut state,
        Event::ProbeReply {
            carrier: 8,
            call: pending,
            reply: AuthorityProbeReply::Admitted {},
        },
    );
    assert_eq!(effects, [Effect::OutageEnded { epoch: second }]);
    assert_eq!(state.outage(), None);
}

#[test]
fn terminal_authority_fences_only_its_addressed_scope() {
    let mut state = State::new(authority(1));
    apply(&mut state, Event::DialStarted { dial: 2 });
    let old_enrollment = Event::AuthenticatedEnrollmentEnded { enrollment: 2 };
    assert_eq!(
        Gate::transition(&state, old_enrollment),
        Err(TransitionError::Stale)
    );
    apply(
        &mut state,
        Event::Welcome {
            dial: 2,
            authority: authority(1),
            carrier: 2,
        },
    );
    let call = register(&mut state, 2);
    let effects = apply(
        &mut state,
        Event::RegistrationReply {
            carrier: 2,
            call,
            reply: RegisterRunReply::RunRetired {},
        },
    );
    assert_eq!(
        effects,
        [Effect::Fence {
            authority: authority(1),
            reason: TerminalAuthority::RunRetired,
        }]
    );
    assert_eq!(
        state.ending_ingress(&authority(1), &2),
        EndingIngress::Discard
    );
    assert_eq!(
        Gate::transition(&state, registered(2, call)),
        Err(TransitionError::Stale)
    );
}
