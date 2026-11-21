pub async fn due_direct_messages(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(
        host,
        central_engine_model::DueListKind::DirectMessages,
        cutoff,
        limit,
    )
    .await
}

pub async fn due_system_message_expiries(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(
        host,
        central_engine_model::DueListKind::SystemExpiries,
        cutoff,
        limit,
    )
    .await
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "due_queries_test.rs"]
mod tests;
