use approval_request_protocol::{DeliverEndingReply, DeliverEndingRequest, HolderEnding, PrivateEnding, ProviderCancellation};
use hegel::generators as gs;
use state_machine_core::StateMachine;

use super::{
    FinishedReceiptEffect as Effect, FinishedReceiptEvent as Event,
    FinishedReceiptLookup as Lookup, FinishedReceiptState as State, FinishedReceiptStore as Store,
};

fn claim(number: u16) -> DeliverEndingRequest {
    DeliverEndingRequest {
        request_id: format!("01890f2e-7b4c-7cc0-98c4-{number:012x}").parse().unwrap(),
        contents_digest: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned().try_into().unwrap(),
        ending: PrivateEnding::ProviderApproval { ending: HolderEnding::TimedOut {} },
    }
}

fn apply(state: &mut State, event: Event) -> Vec<Effect> {
    let (next, effects) = Store::transition(state, event).unwrap();
    *state = next;
    effects
}

fn lookup(state: &mut State, offer: &DeliverEndingRequest, expected: Lookup) {
    assert_eq!(
        apply(state, Event::LookupOffer(offer.clone())),
        [Effect::Lookup { request_id: offer.request_id.clone(), result: expected }],
    );
}

#[hegel::test(test_cases = 60)]
fn replay_keeps_the_last_256_closures_and_never_refreshes_their_order(case: hegel::TestCase) {
    let pivot = case.draw(gs::integers::<u16>().max_value(256));
    let reverse = case.draw(gs::booleans());
    let mut order = (0..257)
        .map(|step| claim((pivot + if reverse { 256 - step } else { step }) % 257));
    let first = order.next().unwrap();
    let mut state = State::default();
    let received = Lookup::Reply(DeliverEndingReply::Received {});

    lookup(&mut state, &first, Lookup::Unretained);
    assert!(apply(&mut state, Event::RetainClosedClaim(first.clone())).is_empty());
    let mut participants = vec![first.clone()];
    for next in order.by_ref().take(255) {
        assert!(apply(&mut state, Event::RetainClosedClaim(next.clone())).is_empty());
        participants.push(next);
    }
    lookup(&mut state, &first, received);

    let second = participants.get(1).unwrap();
    if case.draw(gs::booleans()) {
        lookup(&mut state, second, received);
        assert!(apply(&mut state, Event::RetainClosedClaim(first.clone())).is_empty());
    }
    let last = order.next().unwrap();
    let effects = apply(&mut state, Event::RetainClosedClaim(last.clone()));
    assert_eq!(effects, [Effect::Evicted { request_id: first.request_id.clone() }],
        "the 257th close evicts the first close even after offer replay");
    lookup(&mut state, &first, Lookup::Unretained);
    lookup(&mut state, &last, received);
    for retained in participants.iter().skip(1) {
        lookup(&mut state, retained, received);
    }

    let mut conflicting = if case.draw(gs::booleans()) { second.clone() } else { last };
    let retained = conflicting.clone();
    conflicting.ending = PrivateEnding::ProviderApproval {
        ending: HolderEnding::Cancelled { reason: ProviderCancellation::RequesterGone {} },
    };
    assert_eq!(
        apply(&mut state, Event::LookupOffer(conflicting)),
        [
            Effect::EndingConflict { request_id: retained.request_id.clone() },
            Effect::Lookup {
                request_id: retained.request_id.clone(),
                result: Lookup::Reply(DeliverEndingReply::EndingDiffers {}),
            },
        ],
        "the same request ID with a different ending must preserve the first claim",
    );
    lookup(&mut state, &retained, received);
}
