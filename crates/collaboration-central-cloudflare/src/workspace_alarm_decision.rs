//! Workerd's alarm decider binding and its publication fault fixture.
use super::CentralStatements;
#[cfg(feature = "test-support")]
use std::sync::Arc;

impl CentralStatements {
    pub(super) fn engine_decider(
        &self,
        _writes_engine_alarm: bool,
    ) -> central_storage::EngineAlarmDecider {
        let decide_alarm = self.decide_alarm.clone();
        #[cfg(feature = "test-support")]
        let decide_alarm = if _writes_engine_alarm
            && self
                .zero_next_write_alarm
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            // The store calls this after the guarded SQL. Workerd rejects epoch zero at setAlarm.
            let zero_alarm: central_storage::EngineAlarmDecider =
                Arc::new(move |times, now, read_retained, write| {
                    decide_alarm(times, now, read_retained, write)?;
                    Ok(Some(0))
                });
            zero_alarm
        } else {
            decide_alarm
        };
        decide_alarm
    }
}
