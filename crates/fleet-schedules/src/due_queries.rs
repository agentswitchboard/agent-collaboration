#[doc(hidden)]
pub use fleet_engine::{
    controlled_machine as __controlled_machine, machine_rows as __machine_rows,
};

#[macro_export]
#[doc(hidden)]
macro_rules! __schedule_rows {
    () => {
        $crate::__machine_rows!(
            "automation_schedules",
            "automation_id",
            "s.next_due_at",
            "automation_timer"
        )
    };
}

#[macro_export]
macro_rules! schedule_alarm_input {
    () => {
        $crate::__controlled_machine!("automation_timer", $crate::__schedule_rows!())
    };
}

pub const AUTOMATION_LIST: &str = concat!(
    "SELECT subject_key,occurrence_ms,effective_due_ms FROM (",
    __schedule_rows!(),
    ") WHERE effective_due_ms<=?1 ORDER BY effective_due_ms,subject_key LIMIT ?2"
);

pub async fn due_schedules(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, AUTOMATION_LIST, cutoff, limit).await
}
