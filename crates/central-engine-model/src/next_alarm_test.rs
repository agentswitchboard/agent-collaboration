use super::*;
use crate::MAX_ALARM_UNIX_MILLIS;

fn time(milliseconds: i64) -> EngineTime {
    EngineTime::from_timestamp_millis(milliseconds).unwrap()
}
fn inputs() -> AlarmInputs {
    AlarmInputs {
        retained_mark: None,
        now: time(300_000),
        last_batch_start: None,
        leftovers_beyond_cap: false,
        earliest_on_grid_due: None,
        earliest_legacy_due: None,
    }
}
fn decide(inputs: AlarmInputs) -> AlarmDecision {
    next_alarm(&inputs)
}

#[test]
fn nothing_pending_clears_even_with_a_batch_floor() {
    assert_eq!(
        decide(AlarmInputs {
            last_batch_start: Some(time(300_000)),
            ..inputs()
        }),
        AlarmDecision::Clear
    );
}

#[test]
fn leftovers_continue_at_the_next_mark_before_a_later_pending_mark() {
    assert_eq!(
        decide(AlarmInputs {
            last_batch_start: Some(time(300_000)),
            leftovers_beyond_cap: true,
            earliest_on_grid_due: Some(time(900_000)),
            ..inputs()
        }),
        AlarmDecision::SetAt(time(600_000))
    );
}

#[test]
fn leftovers_do_not_arm_an_idle_object() {
    assert_eq!(
        decide(AlarmInputs {
            last_batch_start: Some(time(300_000)),
            leftovers_beyond_cap: true,
            ..inputs()
        }),
        AlarmDecision::Clear
    );
}

#[test]
fn due_unclaimed_retained_mark_survives_recomputation() {
    let pending = AlarmInputs {
        now: time(300_001),
        retained_mark: Some(time(300_000)),
        last_batch_start: Some(time(298_000)),
        earliest_legacy_due: Some(time(300_000)),
        ..inputs()
    };
    assert_eq!(decide(pending), AlarmDecision::SetAt(time(300_000)));
    assert_eq!(
        decide(AlarmInputs {
            last_batch_start: Some(time(300_000)),
            ..pending
        }),
        AlarmDecision::SetAt(time(600_000))
    );
    assert_eq!(
        decide(AlarmInputs {
            earliest_legacy_due: None,
            ..pending
        }),
        AlarmDecision::Clear
    );
}

#[test]
fn future_grid_time_is_kept_and_due_or_past_time_moves_to_next_mark() {
    for (due, expected) in [
        (299_999, 600_000),
        (300_000, 600_000),
        (300_001, 600_000),
        (600_000, 600_000),
    ] {
        assert_eq!(
            decide(AlarmInputs {
                earliest_on_grid_due: Some(time(due)),
                ..inputs()
            }),
            AlarmDecision::SetAt(time(expected))
        );
    }
}

#[test]
fn legacy_due_times_reach_the_first_available_mark() {
    for (due, expected) in [(299_999, 600_000), (300_000, 600_000), (305_000, 600_000)] {
        assert_eq!(
            decide(AlarmInputs {
                earliest_legacy_due: Some(time(due)),
                ..inputs()
            }),
            AlarmDecision::SetAt(time(expected))
        );
    }
}

#[test]
fn the_earlier_due_kind_selects_its_first_available_mark() {
    for (grid, legacy, expected) in [
        (Some(310_000), None, 600_000),
        (Some(299_999), None, 600_000),
        (None, Some(305_000), 600_000),
        (None, Some(299_999), 600_000),
        (Some(600_000), Some(340_000), 600_000),
    ] {
        assert_eq!(
            decide(AlarmInputs {
                last_batch_start: Some(time(300_000)),
                earliest_on_grid_due: grid.map(time),
                earliest_legacy_due: legacy.map(time),
                ..inputs()
            }),
            AlarmDecision::SetAt(time(expected))
        );
    }
}

#[test]
fn delayed_continuation_moves_to_the_next_future_mark() {
    assert_eq!(
        decide(AlarmInputs {
            retained_mark: None,
            now: time(360_000),
            last_batch_start: Some(time(300_000)),
            leftovers_beyond_cap: true,
            earliest_on_grid_due: Some(time(300_000)),
            ..inputs()
        }),
        AlarmDecision::SetAt(time(600_000))
    );
}

#[test]
fn failing_legacy_retry_counts_as_a_grid_time() {
    assert_eq!(
        decide(AlarmInputs {
            earliest_on_grid_due: Some(time(600_000)),
            earliest_legacy_due: None,
            ..inputs()
        }),
        AlarmDecision::SetAt(time(600_000))
    );
}

#[test]
fn next_mark_handles_negative_times_and_refuses_upper_edge() {
    for (now, expected) in [(-300_001, -300_000), (-300_000, 0), (-1, 0), (0, 300_000)] {
        assert_eq!(next_five_minute_mark(time(now)), Ok(time(expected)));
    }
    let last_mark = MAX_ALARM_UNIX_MILLIS.div_euclid(300_000) * 300_000;
    assert_eq!(
        next_five_minute_mark(time(last_mark)),
        Err(AlarmDecisionError::NoLaterGridMark)
    );
    assert_eq!(
        next_five_minute_mark(time(last_mark - 1)),
        Ok(time(last_mark))
    );
    assert_eq!(
        decide(AlarmInputs {
            retained_mark: None,
            now: time(last_mark),
            earliest_on_grid_due: Some(time(last_mark)),
            ..inputs()
        }),
        AlarmDecision::Unpublishable
    );
}

#[test]
fn unsupported_inputs_and_a_continuation_without_start_are_explicit_errors() {
    assert_eq!(
        decide(AlarmInputs {
            earliest_on_grid_due: Some(time(MAX_ALARM_UNIX_MILLIS + 1)),
            ..inputs()
        }),
        AlarmDecision::Unpublishable
    );
    assert_eq!(
        decide(AlarmInputs {
            leftovers_beyond_cap: true,
            earliest_on_grid_due: Some(time(600_000)),
            ..inputs()
        }),
        AlarmDecision::Unpublishable
    );
}

#[test]
fn nearby_request_start_does_not_postpone_an_aligned_due_time() {
    assert_eq!(
        decide(AlarmInputs {
            retained_mark: None,
            now: time(299_999),
            last_batch_start: Some(time(299_998)),
            earliest_legacy_due: Some(time(300_000)),
            ..inputs()
        }),
        AlarmDecision::SetAt(time(300_000))
    );
}

#[test]
fn a_future_time_after_the_final_portable_mark_cannot_be_published() {
    let last_mark = MAX_ALARM_UNIX_MILLIS.div_euclid(300_000) * 300_000;
    assert_eq!(
        decide(AlarmInputs {
            retained_mark: None,
            now: time(last_mark - 1),
            earliest_legacy_due: Some(time(last_mark + 1)),
            ..inputs()
        }),
        AlarmDecision::Unpublishable
    );
}
