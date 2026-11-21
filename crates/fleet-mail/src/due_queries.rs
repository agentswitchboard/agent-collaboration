//! The two mail due lists and their controlled alarm inputs.
#[macro_export]
#[doc(hidden)]
macro_rules! __direct_message_rows {
    () => {
        $crate::__machine_rows!(
            "mail_messages",
            "message_id",
            "s.cancel_due_at",
            "direct_message",
            "'queued'",
            "'delivered','cancelled','failed'",
            "s.state='queued' AND s.cancel_due_at IS NOT NULL"
        )
    };
}
#[macro_export]
#[doc(hidden)]
macro_rules! __system_message_expiry_rows {
    () => {
        $crate::__machine_rows!(
            "mail_messages",
            "message_id",
            "s.delivery_window_ends_at",
            "system_message_expiry",
            "'queued'",
            "'delivered','cancelled','failed'",
            "s.state='queued' AND s.message_kind IN ('notice','deliveryOutcome') AND s.delivery_window_ends_at IS NOT NULL"
        )
    };
}

#[macro_export]
macro_rules! direct_message_alarm_input {
    () => {
        $crate::__controlled_machine!("direct_message", $crate::__direct_message_rows!())
    };
}

#[macro_export]
macro_rules! system_message_expiry_alarm_input {
    () => {
        $crate::__controlled_machine!(
            "system_message_expiry",
            $crate::__system_message_expiry_rows!()
        )
    };
}

/// Direct messages still queued at their cancel mark (design 2026-11-17).
pub const DIRECT_MESSAGE_LIST: &str = concat!(
    "SELECT subject_key,occurrence_ms,effective_due_ms FROM (",
    __direct_message_rows!(),
    ") WHERE effective_due_ms<=?1 ORDER BY effective_due_ms,subject_key LIMIT ?2"
);

pub const SYSTEM_MESSAGE_EXPIRY_LIST: &str = concat!(
    "SELECT subject_key,occurrence_ms,effective_due_ms FROM (",
    __system_message_expiry_rows!(),
    ") WHERE effective_due_ms<=?1 ORDER BY effective_due_ms,subject_key LIMIT ?2"
);

pub async fn due_direct_messages(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, DIRECT_MESSAGE_LIST, cutoff, limit).await
}

pub async fn due_system_message_expiries(
    host: &impl fleet_runtime_ports::CentralStore,
    cutoff: central_engine_model::EngineTime,
    limit: usize,
) -> Result<Vec<central_engine_model::DueRow>, fleet_engine::DueDiscoveryError> {
    fleet_engine::read_due_rows(host, SYSTEM_MESSAGE_EXPIRY_LIST, cutoff, limit).await
}
