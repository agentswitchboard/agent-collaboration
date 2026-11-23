use super::DueBatchState;
use central_engine_model::{AlarmDecision, AlarmInputs, EngineTime, next_alarm};
use central_storage::{AlarmPublicationFailure, EngineAlarmDecider};
use std::sync::Arc;

pub fn engine_alarm_decider(state: Arc<DueBatchState>) -> EngineAlarmDecider {
    Arc::new(move |times, now_unix_millis, read_retained, write| {
        state.with_publication(|current| {
            let now = EngineTime::from_timestamp_millis(now_unix_millis)
                .ok_or(AlarmPublicationFailure::NowOutOfRange)?;
            let time = |time: Option<i64>| {
                time.map(|time| {
                    EngineTime::from_timestamp_millis(time)
                        .ok_or(AlarmPublicationFailure::EarliestDueOutOfRange)
                })
                .transpose()
            };
            let inputs = AlarmInputs {
                now,
                retained_mark: read_retained().and_then(EngineTime::from_timestamp_millis),
                last_batch_start: time(current.last_start_unix_millis)?,
                leftovers_beyond_cap: current.leftovers_pending,
                earliest_on_grid_due: time(times.earliest_on_grid_due_unix_millis)?,
                earliest_legacy_due: time(times.earliest_legacy_due_unix_millis)?,
            };
            let time = match next_alarm(&inputs) {
                AlarmDecision::Clear => None,
                AlarmDecision::SetAt(time) => Some(time.timestamp_millis()),
                AlarmDecision::Unpublishable => {
                    return Err(AlarmPublicationFailure::NoLaterGridMark);
                }
            };
            write(time);
            Ok(time)
        })
    })
}
