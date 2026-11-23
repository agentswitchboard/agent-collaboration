//! Read the approval applying-time floor for deadline discovery and application.
use central_engine_model::{EngineTime, supported_time};
use central_storage::{ReadOutcome, SqlValue, Statement, StoreFailure};
use fleet_runtime_ports::workspace_decode_error::corrupt;

pub fn approval_time_floor_statement(organization: &str) -> Statement {
    crate::checked_engine_statement!(
        query_file,
        [],
        "../fleet-approvals/src/queries/read_approval_applying_time.sql",
        organization = organization.to_owned(),
    )
}

pub fn decode_approval_time_floor(
    result: &ReadOutcome,
) -> Result<Option<EngineTime>, StoreFailure> {
    let stored = match result.rows.as_slice() {
        [] => return Ok(None),
        [row] => match row.as_slice() {
            [SqlValue::Integer(value)] => value.get(),
            _ => return Err(corrupt("approval_applying_time.observed_ms")),
        },
        _ => return Err(corrupt("approval_applying_time.organization_id")),
    };
    let observed = EngineTime::from_timestamp_millis(stored)
        .filter(|time| supported_time(*time))
        .ok_or_else(|| corrupt("approval_applying_time.observed_ms"))?;
    Ok(Some(observed))
}
