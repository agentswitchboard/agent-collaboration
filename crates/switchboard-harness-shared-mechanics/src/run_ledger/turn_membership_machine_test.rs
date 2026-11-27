use super::*;
use state_machine_core::check_state_machine_transitions;

fn turns() -> [TurnId; 2] {
    [
        TurnId("019ac19f-aa00-7000-8000-000000000001".parse().unwrap()),
        TurnId("019ac19f-aa00-7000-8000-000000000002".parse().unwrap()),
    ]
}

fn events(first: TurnId, second: TurnId) -> [TurnMembershipEvent; 11] {
    use TurnMembershipEvent::{Activate, Begin, End, ObserveEligible, RemoveOnRevoke, TakeNew};
    [
        Activate,
        Begin { turn: first },
        Begin { turn: second },
        ObserveEligible { turn: first },
        ObserveEligible { turn: second },
        TakeNew { turn: first },
        TakeNew { turn: second },
        RemoveOnRevoke { turn: first },
        RemoveOnRevoke { turn: second },
        End { turn: first },
        End { turn: second },
    ]
}

#[test]
fn turn_membership_takes_exactly_the_declared_edges() {
    let [first, second] = turns();
    use TurnMembershipState::{Eligible, Idle, Loading, ProviderStarted, Provisional};
    let states = [
        Loading,
        Idle,
        Provisional { turn: first },
        Eligible { turn: first },
        ProviderStarted { turn: first },
    ];
    let cases = states
        .into_iter()
        .flat_map(|state| events(first, second).map(|event| (state, event)));

    assert_eq!(
        check_state_machine_transitions::<TurnMembershipMachine>(cases),
        Ok(()),
    );
}

#[test]
fn turn_membership_checks_every_refusal_and_keeps_the_exact_turn() {
    use TurnMembershipRefusal::{AlreadyLive, NotLive, NotThatTurn, TurnAlreadySet};
    use TurnMembershipState::{Eligible, Idle, Loading, ProviderStarted, Provisional};

    let [first, second] = turns();
    let (provisional, other_provisional) =
        (Provisional { turn: first }, Provisional { turn: second });
    let (eligible, started, other_started) = (
        Eligible { turn: first },
        ProviderStarted { turn: first },
        ProviderStarted { turn: second },
    );
    let refusals = [
        (AlreadyLive, "already_live"),
        (NotLive, "not_live"),
        (TurnAlreadySet, "turn_already_set"),
        (NotThatTurn, "not_that_turn"),
    ];
    for (refusal, expected_tag) in refusals {
        assert_eq!(refusal.refusal_tag(), expected_tag);
    }

    let live = Err(AlreadyLive);
    let loading = Err(NotLive);
    let occupied = Err(TurnAlreadySet);
    let wrong_turn = Err(NotThatTurn);
    let cases = [
        (
            Loading,
            [
                Ok(Idle),
                loading,
                loading,
                loading,
                loading,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
            ],
        ),
        (
            Idle,
            [
                live,
                Ok(provisional),
                Ok(other_provisional),
                Ok(started),
                Ok(other_started),
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
            ],
        ),
        (
            provisional,
            [
                live,
                occupied,
                occupied,
                occupied,
                occupied,
                Ok(eligible),
                wrong_turn,
                Ok(Idle),
                wrong_turn,
                wrong_turn,
                wrong_turn,
            ],
        ),
        (
            eligible,
            [
                live,
                occupied,
                occupied,
                occupied,
                occupied,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                Ok(Idle),
                wrong_turn,
            ],
        ),
        (
            started,
            [
                live,
                occupied,
                occupied,
                occupied,
                occupied,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                wrong_turn,
                Ok(Idle),
                wrong_turn,
            ],
        ),
    ];

    for (state, expected) in cases {
        for (event, oracle) in events(first, second).into_iter().zip(expected) {
            let mut membership = state;
            let result =
                TurnMembershipMachine::transition(&membership, event).map(|(next, effects)| {
                    assert!(effects.is_empty());
                    membership = next;
                    next
                });

            assert_eq!(
                result,
                oracle.map_err(TransitionError::Domain),
                "{state:?} {event:?}"
            );
            assert_eq!(membership, oracle.unwrap_or(state));
        }
    }
}
