//! The approval lifecycle's durable observation of Central applying time.
use central_engine_model::{EngineTime, supported_time};
use central_storage::{SafeInteger, StoreFailure};
use fleet_approvals::{ApprovalStoreTransaction, advance_approval_time_statement};
use fleet_engine::{approval_time_floor_statement, decode_approval_time_floor};
use fleet_runtime_ports::{SyncTime, workspace_decode_error::corrupt};

pub(crate) async fn observe_applying_time(
    transaction: &mut impl ApprovalStoreTransaction,
    sampled: SyncTime,
) -> Result<SyncTime, StoreFailure> {
    let organization = transaction.organization_id().clone();
    let stored = transaction
        .read_approval_statement(approval_time_floor_statement(organization.as_str()))
        .await?;
    let observed = EngineTime::from_timestamp_millis(sampled.unix_millis())
        .filter(|time| supported_time(*time))
        .ok_or_else(|| corrupt("approval_applying_time.observed_ms"))?;
    if let Some(previous) = decode_approval_time_floor(&stored)?
        && previous >= observed
    {
        return Ok(SyncTime::from_unix_millis(previous.timestamp_millis()));
    }
    let rows = transaction
        .execute_approval_statement(advance_approval_time_statement(
            &organization,
            SafeInteger::try_from(observed.timestamp_millis())
                .map_err(|_| corrupt("approval_applying_time.observed_ms"))?,
            observed.to_rfc3339(),
        ))
        .await?;
    if rows != 1 {
        return Err(corrupt("approval_applying_time.organization_id"));
    }
    Ok(sampled)
}
