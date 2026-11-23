//! The holder path observes and persists its logical applying time.
use central_storage::StoreFailure;
use fleet_approvals::{ApprovalApplyingTime, ApprovalStoreTransaction};
use fleet_runtime_ports::{Clock, SyncTime};

pub(crate) async fn observe_applying_time(
    transaction: &mut impl ApprovalStoreTransaction,
    sampled: SyncTime,
) -> Result<SyncTime, StoreFailure> {
    let time = ApprovalApplyingTime::read(transaction, sampled).await?;
    let effective = time.now();
    time.persist(transaction).await?;
    Ok(effective)
}
