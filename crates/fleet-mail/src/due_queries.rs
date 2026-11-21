const DIRECT_MESSAGE_LIST: &str = include_str!("queries/read_due_direct_messages.sql");

pub async fn due_direct_messages(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, DIRECT_MESSAGE_LIST, cutoff, limit).await
}

const SYSTEM_MESSAGE_EXPIRY_LIST: &str = include_str!("queries/read_due_system_expiries.sql");

pub async fn due_system_message_expiries(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, SYSTEM_MESSAGE_EXPIRY_LIST, cutoff, limit).await
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "due_queries_test.rs"]
mod tests;
