use super::{StampHealth, StampStanding, StandingCache, StandingEvent, StandingMachine};
use fleet_identity_protocol::UnixSeconds;
use fleet_runtime_ports::SyncTime;
use hegel::{TestCase, generators as gs};
use state_machine_core::check_state_machine_transitions;

fn observations() -> [StampHealth; 4] {
    [
        StampHealth::Unchecked,
        StampHealth::Unprovisioned,
        StampHealth::Verified {
            issued_at: UnixSeconds(10),
            expires_at: UnixSeconds(20),
        },
        StampHealth::Refused,
    ]
}

#[test]
fn the_standing_table_covers_every_state_and_event() {
    let states = observations();
    let events = states.map(StandingEvent::Observed);
    let pairs = states.into_iter().flat_map(|state| {
        events
            .iter()
            .cloned()
            .chain([StandingEvent::Invalidate])
            .map(move |event| (state, event))
    });
    check_state_machine_transitions::<StandingMachine>(pairs).unwrap();
}

#[hegel::test(test_cases = 256)]
fn standing_uses_the_first_definitive_observation_after_each_invalidation(case: TestCase) {
    const SAMPLES: [i64; 5] = [9_999, 10_000, 19_999, 20_000, 20_001];
    let cache = StandingCache::default();
    let mut first_definitive = None;
    for _ in 0..case.draw(gs::integers::<usize>().min_value(1).max_value(32)) {
        let action = case.draw(gs::integers::<usize>().max_value(4));
        if let Some(observed) = observations().get(action) {
            cache.apply(StandingEvent::Observed(*observed));
            if action != 0 && first_definitive.is_none() {
                first_definitive = Some(action);
            }
        } else {
            cache.forget();
            first_definitive = None;
        }

        assert_eq!(cache.needs_check(), first_definitive.is_none());
        for now in SAMPLES {
            let expected = match first_definitive {
                Some(1) => StampStanding::Unprovisioned,
                Some(2) if (10_000..20_000).contains(&now) => StampStanding::Valid,
                _ => StampStanding::Refused,
            };
            assert_eq!(cache.standing(SyncTime::from_unix_millis(now)), expected);
        }
    }
}
