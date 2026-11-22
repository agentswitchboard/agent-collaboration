use std::time::Duration;

use approval_request_protocol::{
    ApprovalWindow, CallOffer, InvocationDigest, OfferedOption, OfferedOptionId, OfferedOptions,
    OptionEffect, OptionLifetime, ParkedCallHolder, Raise, RaiseReceipt, RaiseReply,
};
use hegel::generators as gs;
use state_machine_core::{StateMachine, TransitionError};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use super::{
    DispatchEffect as Effect, DispatchEvent as Event, DispatchRefusal as Refusal,
    DispatcherState as State, FirstRaiseDispatcher as Dispatcher, RaiseAttempt, RaisePublication,
};

fn run() -> RouterRunId {
    "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap()
}

fn request_id(number: u16) -> ApprovalRequestId {
    format!("01890f2e-7b4c-7cc0-98c4-{number:012x}")
        .parse()
        .unwrap()
}

fn parked_raise(number: u16) -> Raise {
    Raise::UnprovenCall {
        request_id: request_id(number),
        holder: ParkedCallHolder { router_run: run() },
        invocation: InvocationDigest::from_bound_bytes(&number.to_be_bytes()),
        offer: CallOffer {
            operation: "parked_check".to_owned().try_into().unwrap(),
            summary: "Run the parked check".to_owned().try_into().unwrap(),
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

fn apply(state: &mut State<u8>, event: Event<u8>) -> Vec<Effect<u8>> {
    let (next, effects) = Dispatcher::transition(state, event).unwrap();
    *state = next;
    effects
}

fn publication(number: u16, carrier: u8, seconds: u64) -> Event<u8> {
    Event::Publish {
        request_id: request_id(number),
        carrier,
        now: Duration::from_secs(seconds),
    }
}

fn send(
    state: &mut State<u8>,
    number: u16,
    carrier: u8,
    seconds: u64,
    expected: RaisePublication,
) -> RaiseAttempt<u8> {
    let result = Dispatcher::transition(state, publication(number, carrier, seconds));
    assert!(
        result.is_ok(),
        "Raise {number} must be eligible at {seconds}s: {result:?}"
    );
    let (next, effects) = result.unwrap();
    *state = next;
    assert_eq!(effects.len(), 1);
    effects
        .into_iter()
        .find_map(|effect| {
            if let Effect::PublishRaise {
                attempt,
                publication,
                request,
            } = effect
            {
                assert_eq!(attempt.carrier, carrier);
                assert_eq!(publication, expected);
                assert_eq!(request, parked_raise(number));
                Some(attempt)
            } else {
                None
            }
        })
        .unwrap()
}

fn capacity_refused(state: &mut State<u8>, number: u16, attempt: RaiseAttempt<u8>) {
    apply(
        state,
        Event::Reply {
            request_id: request_id(number),
            attempt,
            reply: RaiseReply::TooManyOutstanding {},
            now: Duration::ZERO,
        },
    );
}

fn admit(state: &mut State<u8>, number: u16, attempt: RaiseAttempt<u8>) {
    let id = request_id(number);
    let raised_at = "2026-11-22T17:00:00Z".parse().unwrap();
    let window = ApprovalWindow::from_first_save(raised_at).unwrap();
    apply(
        state,
        Event::Reply {
            request_id: id.clone(),
            attempt,
            reply: RaiseReply::Receipt {
                receipt: RaiseReceipt {
                    request_id: id,
                    window,
                },
            },
            now: Duration::ZERO,
        },
    );
}

#[hegel::test(test_cases = 100)]
fn deferred_timeout_keeps_the_oldest_place_and_retries_immediately(case: hegel::TestCase) {
    let mut state = State::new(run(), Duration::ZERO);
    apply(&mut state, Event::Registered { carrier: 1 });
    for number in [9, 4, 6] {
        apply(&mut state, Event::Enqueue(parked_raise(number)));
    }
    let oldest = send(&mut state, 9, 1, 0, RaisePublication::First);
    let successor = send(&mut state, 4, 1, 0, RaisePublication::First);
    if case.draw(gs::booleans()) {
        capacity_refused(&mut state, 4, successor);
        capacity_refused(&mut state, 9, oldest);
    } else {
        capacity_refused(&mut state, 9, oldest);
        capacity_refused(&mut state, 4, successor);
    }

    assert_eq!(
        Dispatcher::transition(&state, publication(6, 1, 29)),
        Err(TransitionError::Domain(Refusal::HeldByDeferredHead)),
        "a queued parked call cannot overtake a deferred first Raise",
    );
    assert_eq!(
        Dispatcher::transition(&state, publication(4, 1, 30)),
        Err(TransitionError::Domain(Refusal::HeldByDeferredHead)),
        "enqueue order, not request ID or reply order, chooses the deferred head",
    );
    assert_eq!(
        Dispatcher::transition(&state, publication(9, 1, 29)),
        Err(TransitionError::Domain(Refusal::DeferredRetryNotDue)),
    );

    let expired = send(&mut state, 9, 1, 30, RaisePublication::Retry);
    let timed_out_at = case.draw(gs::integers::<u64>().min_value(31).max_value(59));
    apply(
        &mut state,
        Event::AttemptEnded {
            request_id: request_id(9),
            attempt: expired.clone(),
        },
    );
    let current = send(&mut state, 9, 1, timed_out_at, RaisePublication::Retry);
    assert_ne!(
        current, expired,
        "even on the same carrier, retry uses a fresh call"
    );
    assert_eq!(
        Dispatcher::transition(
            &state,
            Event::Reply {
                request_id: request_id(9),
                attempt: expired,
                reply: RaiseReply::TooManyOutstanding {},
                now: Duration::from_secs(timed_out_at),
            }
        ),
        Err(TransitionError::Stale),
    );
    assert_eq!(
        Dispatcher::transition(&state, publication(4, 1, timed_out_at)),
        Err(TransitionError::Domain(Refusal::HeldByDeferredHead)),
    );

    // The wait's local Stop removes the head's right to publish. Stop custody is owned by the wait.
    apply(
        &mut state,
        Event::RemoveWait {
            request_id: request_id(9),
        },
    );
    let next_head = send(&mut state, 4, 1, timed_out_at, RaisePublication::Retry);
    admit(&mut state, 4, next_head);
    send(&mut state, 6, 1, timed_out_at, RaisePublication::First);
}

#[hegel::test(test_cases = 70)]
fn registration_leaves_the_five_minute_replay_allowance_spent(case: hegel::TestCase) {
    let mut state = State::new(run(), Duration::ZERO);
    apply(&mut state, Event::Registered { carrier: 1 });
    let count = case.draw(gs::integers::<u16>().min_value(17).max_value(23));
    for number in 1..=count {
        apply(&mut state, Event::Enqueue(parked_raise(number)));
        let attempt = send(&mut state, number, 1, 0, RaisePublication::First);
        admit(&mut state, number, attempt);
    }

    for number in 1..=16 {
        assert_eq!(
            state.next_acknowledged_replay(Duration::from_secs(300)),
            Some(&request_id(number))
        );
        let attempt = send(
            &mut state,
            number,
            1,
            300,
            RaisePublication::AcknowledgedReplay,
        );
        admit(&mut state, number, attempt);
    }
    apply(&mut state, Event::CarrierLost { carrier: 1 });
    apply(&mut state, Event::Registered { carrier: 2 });
    let reconnected = case.draw(gs::integers::<u64>().min_value(301).max_value(599));
    assert_eq!(
        Dispatcher::transition(&state, publication(17, 2, reconnected)),
        Err(TransitionError::Domain(Refusal::AcknowledgedReplayNotDue)),
        "re-registration must not refill a spent five-minute allowance",
    );
    assert_eq!(
        state.next_acknowledged_replay(Duration::from_secs(reconnected)),
        None
    );
    assert_eq!(
        state.next_acknowledged_replay(Duration::from_secs(600)),
        Some(&request_id(17))
    );
    send(&mut state, 17, 2, 600, RaisePublication::AcknowledgedReplay);
}
