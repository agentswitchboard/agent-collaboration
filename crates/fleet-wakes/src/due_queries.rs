#[doc(hidden)]
pub use fleet_engine::{
    controlled_machine as __controlled_machine, machine_rows as __machine_rows,
};

#[macro_export]
#[doc(hidden)]
macro_rules! __wake_rows {
    () => {
        $crate::__machine_rows!("automation_wakes", "wake_id", "s.due_at", "wake_timer")
    };
}

#[macro_export]
macro_rules! wake_alarm_input {
    () => {
        $crate::__controlled_machine!("wake_timer", $crate::__wake_rows!())
    };
}

pub const WAKE_LIST: &str = concat!(
    "SELECT subject_key,occurrence_ms,effective_due_ms FROM (",
    __wake_rows!(),
    ") WHERE effective_due_ms<=?1 ORDER BY effective_due_ms,subject_key LIMIT ?2"
);

pub async fn due_wakes(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, WAKE_LIST, cutoff, limit).await
}

#[cfg(test)]
#[path = "due_queries_test.rs"]
mod tests;
