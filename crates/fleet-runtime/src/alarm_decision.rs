use central_engine_model::{AlarmDecision, AlarmInputs, EngineTime, next_alarm};
use central_storage::{AlarmPublicationFailure, EngineAlarmDecider};
use std::sync::Arc;

/// The host keeps durable reads and publication in its normal storage ordering.
pub fn engine_alarm_decider() -> EngineAlarmDecider {
    Arc::new(|times, now_unix_millis, write| {
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
        let time = central_storage::alarm_publication_time(time, now_unix_millis)?;
        write(time);
        Ok(time)
    })
}
