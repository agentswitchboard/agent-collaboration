const WAKE_LIST: &str = include_str!("queries/read_due_wakes.sql");

pub async fn due_wakes(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, WAKE_LIST, cutoff, limit).await
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "due_queries_test.rs"]
mod tests;
