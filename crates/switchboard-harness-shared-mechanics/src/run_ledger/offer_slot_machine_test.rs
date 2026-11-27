use super::*;
use state_machine_core::check_state_machine_transitions;

const EVENTS: [OfferSlotEvent; 2] = [OfferSlotEvent::HandOver, OfferSlotEvent::Revoke];

#[test]
fn offer_slots_take_exactly_the_declared_edges() {
    let states = [
        OfferSlotState::Offered,
        OfferSlotState::Taken,
        OfferSlotState::Revoked,
    ];
    let cases = states
        .into_iter()
        .flat_map(|state| EVENTS.map(|event| (state, event)));

    assert_eq!(
        check_state_machine_transitions::<OfferSlotMachine>(cases),
        Ok(())
    );
    assert_eq!(
        OfferSlotMachine::transition(&OfferSlotState::Offered, OfferSlotEvent::HandOver),
        Ok((OfferSlotState::Taken, Vec::new())),
    );
    assert_eq!(
        OfferSlotMachine::transition(&OfferSlotState::Offered, OfferSlotEvent::Revoke),
        Ok((OfferSlotState::Revoked, Vec::new())),
    );
}

#[test]
fn a_taken_or_revoked_slot_refuses_both_events_without_changing() {
    for (state, refusal, tag) in [
        (
            OfferSlotState::Taken,
            OfferSlotRefusal::AlreadyTaken,
            "already_taken",
        ),
        (
            OfferSlotState::Revoked,
            OfferSlotRefusal::AlreadyRevoked,
            "already_revoked",
        ),
    ] {
        for event in EVENTS {
            let mut slot = state;
            let result = OfferSlotMachine::transition(&slot, event).map(|(next, _)| slot = next);

            assert_eq!(
                result,
                Err(TransitionError::Domain(refusal)),
                "{state:?} {event:?}"
            );
            assert_eq!(slot, state);
            assert_eq!(refusal.refusal_tag(), tag);
        }
    }
}
