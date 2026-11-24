use super::*;
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
    let inputs = AlarmInputs {
        now: draw_time(),
        earliest_on_grid_due: grid,
        earliest_legacy_due: optional_time(),
    };
    let earliest = [inputs.earliest_on_grid_due, inputs.earliest_legacy_due]
        .into_iter()
        .flatten()
        .min();

    match earliest {
        None => assert_eq!(next_alarm(&inputs), AlarmDecision::Clear),
        Some(due) if due > inputs.now => {
            assert_eq!(next_alarm(&inputs), AlarmDecision::SetAt(due));
        }
        Some(_) => {
            let mark = (inputs.now.timestamp_millis().div_euclid(300_000) + 1) * 300_000;
            let expected = if mark <= MAX_ALARM_UNIX_MILLIS {
                AlarmDecision::SetAt(EngineTime::from_timestamp_millis(mark).unwrap())
            } else {
                AlarmDecision::Unpublishable
            };
            assert_eq!(next_alarm(&inputs), expected);
        }
    }
}
