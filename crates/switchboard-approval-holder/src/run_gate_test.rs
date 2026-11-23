use approval_request_protocol::{RegisterRunReply, RunRefusal};
use hegel::generators as gs;
use state_machine_core::{EventTag, StateMachine, StateTag, Transition, TransitionError};

use super::{
    AuthorityEnd, CallAttempt, CallKey, DialAttempt, LinkAdmission, LinkAttempt, MatchedRunRefusal,
    RunAuthority, RunGate, RunGateEffect, RunGateEvent, RunGateState,
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

fn link(connection: u8) -> LinkAttempt<u8> {
    LinkAttempt {
        connection,
        dial: DialAttempt::new(u64::from(connection)),
    }
}

fn call(connection: u8, attempt: u64) -> CallKey<u8> {
    CallKey {
        link: link(connection),
        attempt: CallAttempt::new(attempt),
    }
}

fn welcome(state: &mut State, connection: u8) {
    apply(
        state,
        Event::DialStarted {
            link: link(connection),
        },
    );
    assert!(
        apply(
            state,
            Event::Welcome {
                authority: authority(),
                link: link(connection)
            }
        )
        .is_empty()
    );
}

fn publish(state: &mut State, connection: u8, attempt: u64) {
    assert_eq!(
        apply(
            state,
            Event::PublishRegistration {
                call: call(connection, attempt)
            }
        ),
        [Effect::Register {
            call: call(connection, attempt),
            request: approval_request_protocol::RegisterRunRequest {
                router_run: authority().router_run
            },
        }]
    );
}

fn registered(connection: u8, attempt: u64) -> Event {
    Event::RegistrationReply {
        call: call(connection, attempt),
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
            Gate::transition(&state, registered(connection, 1)),
            Err(TransitionError::Stale),
            "a Register reply without a pending typed call cannot open"
        );
        publish(&mut state, connection, 1);

        for ended in 1..connection {
            assert_eq!(
                Gate::transition(&state, registered(ended, 1)),
                Err(TransitionError::Stale),
                "a reply for a disconnected connection cannot open"
            );
        }
        assert_eq!(
            state.admit_link(&authority(), &link(connection)),
            LinkAdmission::NotReady
        );

        let mut attempt = 1;
        if case.draw(gs::booleans()) {
            apply(
                &mut state,
                Event::RegistrationFailed {
                    call: call(connection, 1),
                },
            );
            assert_eq!(
                Gate::transition(&state, registered(connection, 1)),
                Err(TransitionError::Stale)
            );
            attempt = 2;
            publish(&mut state, connection, attempt);
        }
        if connection < connections {
            apply(
                &mut state,
                Event::ConnectionLost {
                    link: link(connection),
                },
            );
            assert_eq!(state.ready_connection(), None);
        } else {
            apply(&mut state, registered(connection, attempt));
            assert_eq!(state.ready_connection(), Some(&connection));
            assert!(matches!(
                state.admit_link(&authority(), &link(connection)),
                LinkAdmission::Ready
            ));
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
        publish(&mut state, 1, 1);
        apply(&mut state, registered(1, 1));

        assert_eq!(
            apply(
                &mut state,
                Event::HolderAuthorityRefused(MatchedRunRefusal {
                    call: call(1, 2),
                    router_run: authority().router_run,
                    refusal: RunRefusal::RunNotRegistered {},
                }),
            ),
            [Effect::RetireConnection { link: link(1) }]
        );
        assert_eq!(state.ready_connection(), None);
        welcome(&mut state, 2);
        publish(&mut state, 2, 1);
        assert_eq!(
            apply(
                &mut state,
                Event::RegistrationReply {
                    call: call(2, 1),
                    reply: terminal_reply
                }
            ),
            [Effect::FenceWaits {
                authority: authority(),
                reason: expected_end
            }],
        );
        assert_eq!(
            state.admit_link(&authority(), &link(2)),
            LinkAdmission::Discard
        );
        assert_eq!(
            Gate::transition(&state, registered(1, 1)),
            Err(TransitionError::Stale)
        );
    }
}

#[test]
fn gate_transition_matrix_reaches_exactly_its_declared_edges() {
    use super::Phase;
    use state_machine_core::check_state_machine_transitions;

    let phases = [
        Phase::AwaitingDial,
        Phase::Dialing(link(1)),
        Phase::RegistrationDue(link(1)),
        Phase::Registering(call(1, 1)),
        Phase::Ready(link(1)),
        Phase::Ended(AuthorityEnd::RunRetired),
    ];
    let mut events = vec![
        Event::DialStarted { link: link(1) },
        Event::Welcome {
            authority: authority(),
            link: link(1),
        },
        Event::PublishRegistration { call: call(1, 1) },
        Event::RegistrationFailed { call: call(1, 1) },
        Event::ConnectionLost { link: link(1) },
        Event::AuthenticatedEnrollmentEnded { enrollment: 1 },
    ];
    for reply in [
        RegisterRunReply::Registered {},
        RegisterRunReply::RunNotRegistered {},
        RegisterRunReply::RunRetired {},
        RegisterRunReply::EnrollmentEnded {},
    ] {
        events.push(Event::RegistrationReply {
            call: call(1, 1),
            reply,
        });
    }
    for refusal in [
        RunRefusal::RunNotRegistered {},
        RunRefusal::RunRetired {},
        RunRefusal::EnrollmentEnded {},
    ] {
        events.push(Event::HolderAuthorityRefused(MatchedRunRefusal {
            call: call(1, 2),
            router_run: authority().router_run,
            refusal,
        }));
    }
    let cases = phases.into_iter().flat_map(|phase| {
        events.iter().cloned().map(move |event| {
            (
                State {
                    authority: authority(),
                    phase: phase.clone(),
                },
                event,
            )
        })
    });
    check_state_machine_transitions::<Gate>(cases).unwrap();
}
