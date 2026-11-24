//! The owning approval transaction advances its durable logical time.
use central_engine_model::{EngineTime, supported_time};
use central_storage::{SafeInteger, Statement, StoreFailure};
use fleet_engine::{
    approval_time_floor_statement, checked_engine_statement, decode_approval_time_floor,
};
use fleet_runtime_ports::{Clock, SyncTime, workspace_decode_error::corrupt};

use crate::ApprovalStoreTransaction;

/// The approval lifecycle may observe further than the current clock sample.
pub struct ApprovalApplyingTime {
    effective: SyncTime,
    advance: Option<Statement>,
}

impl ApprovalApplyingTime {
    pub async fn read(
        transaction: &mut impl ApprovalStoreTransaction,
        sampled: SyncTime,
    ) -> Result<Self, StoreFailure> {
        let organization = transaction.organization_id().clone();
        let row = transaction
            .read_approval_statement(approval_time_floor_statement(organization.as_str()))
            .await?;
        let at = EngineTime::from_timestamp_millis(sampled.unix_millis())
            .filter(|time| supported_time(*time))
            .ok_or_else(|| corrupt("approval_applying_time.observed_ms"))?;
        if let Some(previous) = decode_approval_time_floor(&row)?
            && previous >= at
        {
            return Ok(Self {
                effective: SyncTime::from_unix_millis(previous.timestamp_millis()),
                advance: None,
            });
        }
        let statement = advance_approval_time_statement(
            &organization,
            SafeInteger::try_from(at.timestamp_millis())
                .map_err(|_| corrupt("approval_applying_time.observed_ms"))?,
            at.to_rfc3339(),
        );
        Ok(Self {
            effective: sampled,
            advance: Some(statement),
        })
    }

    /// Apply the proposed monotonic advance in this transaction.
    pub async fn persist(
        self,
        transaction: &mut impl ApprovalStoreTransaction,
    ) -> Result<(), StoreFailure> {
        if let Some(statement) = self.advance
            && transaction.execute_approval_statement(statement).await? > 1
        {
            return Err(corrupt("approval_applying_time.organization_id"));
        }
        Ok(())
    }
}

impl Clock for ApprovalApplyingTime {
    fn now(&self) -> SyncTime {
        self.effective
    }
}

fn advance_approval_time_statement(
    organization: &fleet_identity_protocol::OrganizationId,
    observed_ms: SafeInteger,
    observed_at: String,
) -> Statement {
    checked_engine_statement!(
        query_file,
        [],
        "src/queries/advance_approval_applying_time.sql",
        organization = organization.as_str().to_owned(),
        observed_ms = observed_ms,
        observed_at = observed_at,
    )
}
