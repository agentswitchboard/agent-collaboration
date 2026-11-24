use std::{sync::Arc, time::Duration};

use central_link_client::{ClientAction, ConnectionAttempt, EnrollmentEnd, LinkClient, LinkFault};
use central_link_protocol::{CorrelationId, MachineLinkCloseCode, TokenRenewalRefusal};
use hegel::generators as gs;
use router_clock::{ClockInstant, ManualClock};
use state_machine_core::{EventTag, StateMachine, StateTag, Transition, TransitionError};

use super::{
    DialFailure, LeaseVersion, LeaseWindow, LinkEffect, LinkInput, LinkPolicy, LinkState,
    LinkStatus,
};
use workspace_ids::MachineId;

type State = LinkState<u8>;
type Input = LinkInput<u8>;
type Effect = LinkEffect<u8>;
type Policy = LinkPolicy<u8>;

#[path = "generated_link_orders_test.rs"]
mod generated_link_orders;

fn machine() -> MachineId {
    "01890f2e-7b4c-7cc0-98c4-000000000002".parse().unwrap()
}

fn at(seconds: u64) -> ClockInstant {
    ClockInstant::ZERO.after(Duration::from_secs(seconds))
}

fn identifier(number: u8) -> CorrelationId {
    format!("01890f2e-7b4c-7cc0-98c4-{number:012x}")
        .parse()
        .unwrap()
}

fn attempt_pair() -> (ConnectionAttempt, ConnectionAttempt) {
    let clock = Arc::new(ManualClock::new("2030-01-01T00:00:00Z".parse().unwrap()));
    let first = LinkClient::new(machine(), clock.clone())
        .poll_time()
        .unwrap();
    let second = LinkClient::new(machine(), clock).poll_time().unwrap();
    match (first, second) {
        (ClientAction::Connect { attempt: first }, ClientAction::Connect { attempt: second }) => {
            (first, second)
        }
        (wrong, _) => panic!("needed connection attempts: {wrong:?}"),
    }
}

fn apply(state: &mut State, input: Input) -> Vec<Effect> {
    let event = input.event_tag();
    let (next, effects) = Policy::transition(state, input).unwrap();
    assert!(Policy::transitions().contains(&Transition {
        from: state.state_tag(),
        event,
        to: next.state_tag(),
        terminal: next.is_terminal(),
    }));
    *state = next;
    effects
}

fn lease(version: u64) -> LeaseWindow<u8> {
    LeaseWindow {
        binding: 1,
        version: LeaseVersion(version),
        usable_until: at(2000),
    }
}

fn waiting() -> State {
    let mut state = State::new(machine());
    apply(&mut state, Input::BindingAvailable(1));
    state
}

fn dial(state: &mut State, attempt: &ConnectionAttempt, lease: LeaseWindow<u8>, now: u64) {
    assert!(
        apply(
            state,
            Input::DialStarted {
                attempt: attempt.clone(),
                lease,
                now: at(now)
            }
        )
        .is_empty()
    );
}

#[hegel::test(test_cases = 100)]
fn unauthorized_has_one_newer_token_retry_then_a_recoverable_fault(case: hegel::TestCase) {
    let mut state = waiting();
    let current = case.draw(gs::integers::<u64>().max_value(100));
    let (first, second) = attempt_pair();
    dial(&mut state, &first, lease(current), 0);
    let effects = apply(
        &mut state,
        Input::DialFailed {
            attempt: first.clone(),
            reason: DialFailure::Unauthorized,
            now: at(1),
        },
    );
    assert_eq!(
        effects,
        [
            Effect::BackOff { attempt: first },
            Effect::NeedLease {
                binding: 1,
                newer_than: Some(LeaseVersion(current))
            },
        ]
    );
    assert!(
        Policy::transition(
            &state,
            Input::DialStarted {
                attempt: second.clone(),
                lease: lease(current),
                now: at(2)
            }
        )
        .is_err()
    );
    dial(&mut state, &second, lease(current + 1), 2);
    assert!(
        apply(
            &mut state,
            Input::DialFailed {
                attempt: second,
                reason: DialFailure::Unauthorized,
                now: at(2)
            }
        )
        .is_empty()
    );
    assert_eq!(
        state.status(),
        LinkStatus::Fault {
            kind: LinkFault::FreshCredentialRefused
        }
    );
    assert!(apply(&mut state, Input::Tick { now: at(61) }).is_empty());
    assert_eq!(
        apply(&mut state, Input::Tick { now: at(62) }),
        [Effect::NeedLease {
            binding: 1,
            newer_than: None
        },]
    );
}

#[hegel::test(test_cases = 120)]
fn current_renewal_alone_can_move_the_elapsed_link_deadline(case: hegel::TestCase) {
    let (attempt, other) = attempt_pair();
    let mut state = waiting();
    dial(&mut state, &attempt, lease(1), 0);
    assert_eq!(
        Policy::transition(
            &state,
            Input::Welcome {
                attempt: other.clone(),
                machine_id: machine(),
                deadline: at(120),
                now: at(0)
            }
        ),
        Err(TransitionError::Stale),
    );
    apply(
        &mut state,
        Input::Welcome {
            attempt: attempt.clone(),
            machine_id: machine(),
            deadline: at(120),
            now: at(0),
        },
    );
    assert_eq!(
        state.status(),
        LinkStatus::Welcomed {
            machine_id: machine()
        }
    );
    assert!(apply(&mut state, Input::Tick { now: at(59) }).is_empty());
    assert_eq!(
        apply(&mut state, Input::Tick { now: at(60) }),
        [Effect::NeedLease {
            binding: 1,
            newer_than: Some(LeaseVersion(1))
        },]
    );
    assert!(apply(&mut state, Input::Tick { now: at(61) }).is_empty());
    let correlation_id = identifier(1);
    assert_eq!(
        apply(
            &mut state,
            Input::RenewalLease {
                attempt: attempt.clone(),
                lease: lease(2),
                correlation_id: correlation_id.clone(),
                now: at(62),
            }
        ),
        [Effect::RenewOnSocket {
            attempt: attempt.clone(),
            correlation_id: correlation_id.clone()
        }]
    );
    for (foreign, id) in [
        (other, correlation_id.clone()),
        (attempt.clone(), identifier(2)),
    ] {
        assert_eq!(
            Policy::transition(
                &state,
                Input::RenewalAccepted {
                    attempt: foreign,
                    correlation_id: id,
                    deadline: at(300),
                    now: at(70),
                }
            ),
            Err(TransitionError::Stale),
            "an unrelated renewal cannot extend authority"
        );
    }
    let accept = case.draw(gs::booleans());
    if accept {
        apply(
            &mut state,
            Input::RenewalAccepted {
                attempt: attempt.clone(),
                correlation_id,
                deadline: at(300),
                now: at(70),
            },
        );
        assert!(apply(&mut state, Input::Tick { now: at(120) }).is_empty());
        assert_eq!(state.next_deadline(), Some(at(240)));
    } else {
        apply(
            &mut state,
            Input::RenewalRefused {
                attempt: attempt.clone(),
                correlation_id,
                reason: TokenRenewalRefusal::TokenRejected,
                now: at(70),
            },
        );
        assert_eq!(
            state.status(),
            LinkStatus::Welcomed {
                machine_id: machine()
            }
        );
        assert_eq!(state.next_deadline(), Some(at(120)));
        assert_eq!(
            apply(&mut state, Input::Tick { now: at(120) }),
            [
                Effect::BackOff { attempt },
                Effect::NeedLease {
                    binding: 1,
                    newer_than: Some(LeaseVersion(2))
                },
            ]
        );
    }
}

#[hegel::test(test_cases = 80)]
fn final_close_fences_its_exact_binding_until_a_different_binding_appears(case: hegel::TestCase) {
    let (attempt, other) = attempt_pair();
    let mut state = waiting();
    dial(&mut state, &attempt, lease(1), 0);
    let (code, cause) = if case.draw(gs::booleans()) {
        (MachineLinkCloseCode::Revoked, EnrollmentEnd::Revoked)
    } else {
        (MachineLinkCloseCode::Inactive, EnrollmentEnd::Inactive)
    };
    assert_eq!(
        Policy::transition(
            &state,
            Input::SocketClosed {
                attempt: other,
                code: Some(code)
            }
        ),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        apply(
            &mut state,
            Input::SocketClosed {
                attempt,
                code: Some(code)
            }
        ),
        [Effect::EndEnrollment { binding: 1, cause }]
    );
    assert_eq!(state.status(), LinkStatus::EnrollmentEnded { cause });
    assert!(apply(&mut state, Input::Tick { now: at(1000) }).is_empty());
    assert_eq!(
        Policy::transition(&state, Input::BindingAvailable(1)),
        Err(TransitionError::Stale)
    );
    assert_eq!(
        apply(&mut state, Input::BindingAvailable(2)),
        [Effect::NeedLease {
            binding: 2,
            newer_than: None
        }]
    );
}

#[hegel::test(test_cases = 120)]
fn dial_retries_busy_and_lost_connections_but_holds_a_bad_address(case: hegel::TestCase) {
    let (attempt, _) = attempt_pair();
    let mut state = waiting();
    let is_bad = case.draw(gs::booleans());
    let reason = if is_bad {
        DialFailure::InvalidAddress
    } else if case.draw(gs::booleans()) {
        DialFailure::Busy
    } else {
        DialFailure::Transport
    };
    dial(&mut state, &attempt, lease(1), 0);
    let effects = apply(
        &mut state,
        Input::DialFailed {
            attempt: attempt.clone(),
            reason,
            now: at(1),
        },
    );
    if is_bad {
        assert!(effects.is_empty());
        assert_eq!(
            state.status(),
            LinkStatus::Fault {
                kind: LinkFault::MalformedAddress
            }
        );
        assert!(apply(&mut state, Input::Tick { now: at(1000) }).is_empty());
        assert_eq!(
            apply(&mut state, Input::AddressCorrected),
            [Effect::NeedLease {
                binding: 1,
                newer_than: None
            }]
        );
    } else {
        assert_eq!(
            effects,
            [
                Effect::BackOff { attempt },
                Effect::NeedLease {
                    binding: 1,
                    newer_than: None
                }
            ]
        );
        assert_eq!(state.status(), LinkStatus::Connecting);
    }
}

#[test]
fn expired_close_refreshes_and_renewal_refusal_keeps_the_confirmed_deadline() {
    let (attempt, second) = attempt_pair();
    let mut state = waiting();
    dial(&mut state, &attempt, lease(4), 0);
    assert_eq!(
        apply(
            &mut state,
            Input::SocketClosed {
                attempt: attempt.clone(),
                code: Some(MachineLinkCloseCode::Expired),
            }
        ),
        [
            Effect::BackOff { attempt },
            Effect::NeedLease {
                binding: 1,
                newer_than: Some(LeaseVersion(4))
            }
        ]
    );
    dial(&mut state, &second, lease(5), 1);
    let other_machine = "01890f2e-7b4c-7cc0-98c4-000000000099".parse().unwrap();
    assert_eq!(
        Policy::transition(
            &state,
            Input::Welcome {
                attempt: second.clone(),
                machine_id: other_machine,
                deadline: at(120),
                now: at(1)
            }
        ),
        Err(TransitionError::Domain(super::LinkRefusal::WrongMachine))
    );
    apply(
        &mut state,
        Input::Welcome {
            attempt: second.clone(),
            machine_id: machine(),
            deadline: at(120),
            now: at(1),
        },
    );
    assert!(matches!(
        apply(&mut state, Input::Tick { now: at(90) }).as_slice(),
        [Effect::NeedLease { .. }]
    ));
    let correlation_id = identifier(7);
    apply(
        &mut state,
        Input::RenewalLease {
            attempt: second.clone(),
            lease: lease(6),
            correlation_id: correlation_id.clone(),
            now: at(100),
        },
    );
    assert_eq!(
        apply(
            &mut state,
            Input::RenewalRefused {
                attempt: second.clone(),
                correlation_id,
                reason: TokenRenewalRefusal::ConnectionEnrollmentEnded,
                now: at(101),
            }
        ),
        []
    );
    assert_eq!(state.next_deadline(), Some(at(120)));
    assert!(apply(&mut state, Input::Tick { now: at(119) }).is_empty());
    assert_eq!(
        apply(&mut state, Input::Tick { now: at(120) }),
        [
            Effect::BackOff { attempt: second },
            Effect::NeedLease {
                binding: 1,
                newer_than: Some(LeaseVersion(6))
            }
        ]
    );
}

#[test]
fn every_renewal_refusal_preserves_attachment_deadline_and_enrollment() {
    use TokenRenewalRefusal as Refusal;

    let (attempt, _) = attempt_pair();
    let mut pending = waiting();
    dial(&mut pending, &attempt, lease(4), 0);
    apply(
        &mut pending,
        Input::Welcome {
            attempt: attempt.clone(),
            machine_id: machine(),
            deadline: at(120),
            now: at(0),
        },
    );
    apply(&mut pending, Input::Tick { now: at(60) });
    let correlation_id = identifier(8);
    apply(
        &mut pending,
        Input::RenewalLease {
            attempt: attempt.clone(),
            lease: lease(5),
            correlation_id: correlation_id.clone(),
            now: at(61),
        },
    );
    for reason in [
        Refusal::TokenRejected,
        Refusal::PresentedTokenExpired,
        Refusal::ConnectionEnrollmentEnded,
        Refusal::DifferentMachine,
        Refusal::DifferentEnrollment,
        Refusal::CentralUnavailable,
    ] {
        let mut state = pending.clone();
        let refusal = Input::RenewalRefused {
            attempt: attempt.clone(),
            correlation_id: correlation_id.clone(),
            reason,
            now: at(70),
        };
        assert!(apply(&mut state, refusal.clone()).is_empty());
        assert_eq!(
            state.renewal_deadline(),
            Some(at(120)),
            "{reason:?} leaves the confirmed deadline in force"
        );
        assert!(
            matches!(state.status(), LinkStatus::Welcomed { .. }),
            "{reason:?} does not end the enrollment"
        );
        assert_eq!(
            Policy::transition(&state, refusal),
            Err(TransitionError::Stale)
        );
        let expected = if reason == Refusal::PresentedTokenExpired {
            vec![Effect::NeedLease {
                binding: 1,
                newer_than: Some(LeaseVersion(5)),
            }]
        } else {
            Vec::new()
        };
        assert_eq!(
            apply(&mut state, Input::Tick { now: at(70) }),
            expected,
            "same-link refresh policy for {reason:?}"
        );
        assert_eq!(
            apply(&mut state, Input::Tick { now: at(120) }),
            [
                Effect::BackOff {
                    attempt: attempt.clone()
                },
                Effect::NeedLease {
                    binding: 1,
                    newer_than: Some(LeaseVersion(5))
                }
            ]
        );
    }
}

#[test]
fn deadline_wins_over_a_matching_renewal_at_equality() {
    let (attempt, _) = attempt_pair();
    let mut state = waiting();
    let correlation_id = identifier(7);
    dial(&mut state, &attempt, lease(1), 0);
    apply(
        &mut state,
        Input::Welcome {
            attempt: attempt.clone(),
            machine_id: machine(),
            deadline: at(120),
            now: at(0),
        },
    );
    apply(&mut state, Input::Tick { now: at(60) });
    apply(
        &mut state,
        Input::RenewalLease {
            attempt: attempt.clone(),
            lease: lease(2),
            correlation_id: correlation_id.clone(),
            now: at(61),
        },
    );
    assert_eq!(
        Policy::transition(
            &state,
            Input::RenewalAccepted {
                attempt: attempt.clone(),
                correlation_id,
                deadline: at(300),
                now: at(120)
            }
        ),
        Err(TransitionError::Stale)
    );
    assert!(
        matches!(apply(&mut state, Input::Tick { now: at(120) }).as_slice(),
        [Effect::BackOff { attempt: ended }, Effect::NeedLease { .. }] if ended == &attempt)
    );
}
