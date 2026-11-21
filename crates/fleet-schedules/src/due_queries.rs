const AUTOMATION_LIST: &str = include_str!("queries/read_due_schedules.sql");

pub async fn due_schedules(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, AUTOMATION_LIST, cutoff, limit).await
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "due_queries_test.rs"]
mod tests;
