use crate::DueBatchPolicy;
use crate::{
    EngineTime, aligned_at_or_after, aligned_strictly_after, on_five_minute_grid, supported_time,
};

const FIVE_MINUTE_SECONDS: u32 = 300;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlarmInputs {
    pub now: EngineTime,
    pub retained_mark: Option<EngineTime>,
    pub last_batch_start: Option<EngineTime>,
    pub leftovers_beyond_cap: bool,
    pub earliest_on_grid_due: Option<EngineTime>,
    pub earliest_legacy_due: Option<EngineTime>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlarmDecision {
    Clear,
    SetAt(EngineTime),
    Unpublishable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AlarmDecisionError {
    #[error("no later five-minute mark is within the portable range")]
    NoLaterGridMark,
    #[error("the alarm time is outside the portable range")]
    OutOfRange,
}

pub fn next_five_minute_mark(now: EngineTime) -> Result<EngineTime, AlarmDecisionError> {
    if !supported_time(now) {
        return Err(AlarmDecisionError::OutOfRange);
    }
    aligned_strictly_after(now, FIVE_MINUTE_SECONDS).ok_or(AlarmDecisionError::NoLaterGridMark)
}

/// Combine pending times after an event; an idle object has no alarm.
pub fn next_alarm(inputs: &AlarmInputs) -> AlarmDecision {
    let earliest_due = inputs
        .earliest_on_grid_due
        .into_iter()
        .chain(inputs.earliest_legacy_due)
        .min();
    let Some(earliest_due) = earliest_due else {
        return AlarmDecision::Clear;
    };
    if !supported_time(inputs.now)
        || [inputs.earliest_on_grid_due, inputs.earliest_legacy_due]
            .into_iter()
            .flatten()
            .any(|due| !supported_time(due))
    {
        return AlarmDecision::Unpublishable;
    }
    if let Some(mark) = inputs.retained_mark
        && supported_time(mark)
        && on_five_minute_grid(mark)
        && (earliest_due <= mark
            || inputs.leftovers_beyond_cap
                && inputs.last_batch_start.is_some_and(|started| {
                    started
                        .checked_add_signed(DueBatchPolicy::DEFAULT.look_ahead)
                        .is_some_and(|cutoff| earliest_due <= cutoff)
                }))
        && mark <= inputs.now
        && inputs.last_batch_start.is_none_or(|start| start < mark)
    {
        return AlarmDecision::SetAt(mark);
    }
    let alarm = if inputs.leftovers_beyond_cap {
        inputs.last_batch_start.and_then(|start| {
            supported_time(start)
                .then_some(start.max(inputs.now))
                .and_then(|after| next_five_minute_mark(after).ok())
        })
    } else if earliest_due > inputs.now {
        aligned_at_or_after(earliest_due, FIVE_MINUTE_SECONDS)
    } else {
        next_five_minute_mark(inputs.now).ok()
    };
    alarm.map_or(AlarmDecision::Unpublishable, AlarmDecision::SetAt)
}

#[cfg(test)]
#[path = "next_alarm_property_test.rs"]
mod property_tests;
#[cfg(test)]
#[path = "next_alarm_test.rs"]
mod tests;
