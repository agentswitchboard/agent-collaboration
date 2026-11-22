use approval_request_protocol::{DeliverEndingReply, RegisterRunReply, RunRefusal};
use hegel::generators as gs;
use state_machine_core::{EventTag, StateMachine, StateTag, Transition, TransitionError};

use super::{
    AuthorityEnd, EndingIngress, RunAuthority, RunGate, RunGateEffect, RunGateEvent, RunGateState,
};

type Gate = RunGate<u8, u8>;
type State = RunGateState<u8, u8>;
type Event = RunGateEvent<u8, u8>;
type Effect = RunGateEffect<u8, u8>;

fn authority() -> RunAuthority<u8> {
    RunAuthority {
        enrollment: 1,
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

fn welcome(state: &mut State, connection: u8) {
    assert!(
        apply(
            state,
            Event::Welcome {
                authority: authority(),
                connection
            }
        )
        .is_empty()
    );
}

fn publish(state: &mut State, connection: u8) {
    assert_eq!(
        apply(state, Event::PublishRegistration { connection }),
        [Effect::Register {
            connection,
            request: approval_request_protocol::RegisterRunRequest {
                router_run: authority().router_run
            },
        }]
    );
}

fn registered(connection: u8) -> Event {
    Event::RegistrationReply {
        connection,
        reply: RegisterRunReply::Registered {},
    }
}

#[hegel::test(test_cases = 200)]
fn only_current_pending_registration_opens_the_run_gate(case: hegel::TestCase) {
    let mut state = State::new(authority());
    let connections = case.draw(gs::integers::<u8>().min_value(2).max_value(8));
    for connection in 1..=connections {
        welcome(&mut state, connection);
        assert_eq!(
            Gate::transition(&state, registered(connection)),
            Err(TransitionError::Stale),
            "a Register reply without a pending typed call cannot open"
        );
        publish(&mut state, connection);

        for ended in 1..connection {
            assert_eq!(
                Gate::transition(&state, registered(ended)),
                Err(TransitionError::Stale),
                "a reply for a disconnected connection cannot open"
            );
        }
        assert_eq!(
            state.ending_ingress(&authority(), &connection),
            EndingIngress::Reply(DeliverEndingReply::HolderNotReady {})
        );

        if case.draw(gs::booleans()) {
            apply(&mut state, Event::RegistrationFailed { connection });
            assert_eq!(
                Gate::transition(&state, registered(connection)),
                Err(TransitionError::Stale)
            );
            publish(&mut state, connection);
        }
        if connection < connections {
            apply(&mut state, Event::ConnectionLost { connection });
            assert_eq!(state.ready_connection(), None);
        } else {
            apply(&mut state, registered(connection));
            assert_eq!(state.ready_connection(), Some(&connection));
            assert_eq!(
                state.ending_ingress(&authority(), &connection),
                EndingIngress::Authoritative
            );
        }
    }
}

#[test]
fn current_run_refusal_re_registers_or_fences_the_waits() {
    for (terminal_reply, expected_end) in [
        (RegisterRunReply::RunRetired {}, AuthorityEnd::RunRetired),
        (
            RegisterRunReply::EnrollmentEnded {},
            AuthorityEnd::EnrollmentEnded,
        ),
    ] {
        let mut state = State::new(authority());
        welcome(&mut state, 1);
        publish(&mut state, 1);
        apply(&mut state, registered(1));

        apply(
            &mut state,
            Event::HolderAuthorityRefused {
                connection: 1,
                refusal: RunRefusal::RunNotRegistered {},
            },
        );
        assert_eq!(state.ready_connection(), None);
        publish(&mut state, 1);
        assert_eq!(
            apply(
                &mut state,
                Event::RegistrationReply {
                    connection: 1,
                    reply: terminal_reply
                }
            ),
            [Effect::FenceWaits {
                authority: authority(),
                reason: expected_end
            }],
        );
        assert_eq!(
            state.ending_ingress(&authority(), &1),
            EndingIngress::Discard
        );
        assert_eq!(
            Gate::transition(&state, registered(1)),
            Err(TransitionError::Stale)
        );
    }
}
