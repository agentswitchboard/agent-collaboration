//! The approval lifecycle's durable observation of Central applying time.
use central_engine_model::{EngineTime, supported_time};
use central_storage::{SafeInteger, StoreFailure};
use fleet_approvals::{ApprovalStoreTransaction, advance_approval_time_statement};
use fleet_engine::{approval_time_floor_statement, decode_approval_time_floor};
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::{SyncTime, workspace_decode_error::corrupt};

pub(crate) async fn observe_applying_time(
    transaction: &mut impl ApprovalStoreTransaction,
    sampled: SyncTime,
) -> Result<SyncTime, StoreFailure> {
    let observation = read_applying_time(transaction, sampled).await?;
    let now = observation.effective();
    observation.publish(transaction).await?;
    Ok(now)
}

pub(crate) struct ApprovalTimeObservation {
    organization: OrganizationId,
    sampled: EngineTime,
    floor: Option<EngineTime>,
}

impl ApprovalTimeObservation {
    pub(crate) fn effective(&self) -> SyncTime {
        let time = self
            .floor
            .map_or(self.sampled, |floor| floor.max(self.sampled));
        SyncTime::from_unix_millis(time.timestamp_millis())
    }

    pub(crate) async fn publish(
        self,
        transaction: &mut impl ApprovalStoreTransaction,
    ) -> Result<(), StoreFailure> {
        if transaction.organization_id() != &self.organization {
            return Err(corrupt("approval_applying_time.organization_id"));
        }
        if self.floor.is_some_and(|floor| floor >= self.sampled) {
            return Ok(());
        }
        let rows = transaction
            .execute_approval_statement(advance_approval_time_statement(
                &self.organization,
                SafeInteger::try_from(self.sampled.timestamp_millis())
                    .map_err(|_| corrupt("approval_applying_time.observed_ms"))?,
                self.sampled.to_rfc3339(),
            ))
            .await?;
        if rows != 1 {
            return Err(corrupt("approval_applying_time.organization_id"));
        }
        Ok(())
    }
}

pub(crate) async fn read_applying_time(
    transaction: &mut impl ApprovalStoreTransaction,
    sampled: SyncTime,
) -> Result<ApprovalTimeObservation, StoreFailure> {
    let organization = transaction.organization_id().clone();
    let stored = transaction
        .read_approval_statement(approval_time_floor_statement(organization.as_str()))
        .await?;
    let sampled = EngineTime::from_timestamp_millis(sampled.unix_millis())
        .filter(|time| supported_time(*time))
        .ok_or_else(|| corrupt("approval_applying_time.observed_ms"))?;
    Ok(ApprovalTimeObservation {
        organization,
        sampled,
        floor: decode_approval_time_floor(&stored)?,
    })
}
