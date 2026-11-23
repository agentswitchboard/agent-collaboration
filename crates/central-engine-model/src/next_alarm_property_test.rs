use super::*;
use crate::MAX_ALARM_UNIX_MILLIS;
use hegel::{TestCase, generators as gs};

#[hegel::test]
fn alarm_properties_cover_supplied_times_and_every_pending_combination(case: TestCase) {
    let draw_time = || {
        EngineTime::from_timestamp_millis(
            case.draw(
                gs::integers::<i64>()
                    .min_value(-MAX_ALARM_UNIX_MILLIS)
                    .max_value(MAX_ALARM_UNIX_MILLIS),
            ),
        )
        .unwrap()
    };
    let optional_time = || case.draw(gs::booleans()).then(draw_time);
    let grid = case.draw(gs::booleans()).then(|| {
        let mark = case.draw(
            gs::integers::<i64>()
                .min_value((-MAX_ALARM_UNIX_MILLIS).div_euclid(300_000) + 1)
                .max_value(MAX_ALARM_UNIX_MILLIS.div_euclid(300_000)),
        );
        EngineTime::from_timestamp_millis(mark * 300_000).unwrap()
    });
    let leftovers_beyond_cap = case.draw(gs::booleans());
    let last_batch_start = if leftovers_beyond_cap {
        Some(draw_time())
    } else {
        optional_time()
    };
    let inputs = AlarmInputs {
        now: draw_time(),
        last_batch_start,
        leftovers_beyond_cap,
        earliest_on_grid_due: grid,
        earliest_legacy_due: optional_time(),
    };
    let decision = next_alarm(&inputs);
    let now = inputs.now.timestamp_millis();
    let next_mark = |after: i64| (after.div_euclid(300_000) + 1) * 300_000;
    let exact_due = [grid, inputs.earliest_legacy_due]
        .into_iter()
        .flatten()
        .min();
    let expected = if let Some(start) = last_batch_start.filter(|_| leftovers_beyond_cap) {
        Some(next_mark(now.max(start.timestamp_millis())))
    } else {
        exact_due.map(|due| {
            let due = due.timestamp_millis();
            next_mark(now).max(due + (300_000 - due.rem_euclid(300_000)).rem_euclid(300_000))
        })
    };
    match expected {
        None => assert_eq!(decision, AlarmDecision::Clear),
        Some(at) if at > MAX_ALARM_UNIX_MILLIS => {
            assert_eq!(decision, AlarmDecision::Unpublishable)
        }
        Some(at) => {
            assert!(at > now);
            assert_eq!(at.rem_euclid(300_000), 0);
            assert_eq!(
                decision,
                AlarmDecision::SetAt(EngineTime::from_timestamp_millis(at).unwrap())
            );
        }
    }
}
