//! The provisioned sign-in stamp is central's sole durable organization identity.
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::CentralStore;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CentralOrganizationIdentity {
    Unprovisioned,
    Stamped { organization_id: OrganizationId },
}
/// Reads the row verified by provisioning; alarms do not verify its signature again.
pub async fn read_central_organization(
    store: &dyn fleet_signin::stamp_store::StampStore,
) -> Result<CentralOrganizationIdentity, StoreFailure> {
    use fleet_signin::organization_authority::StoredStampRead;
    Ok(match store.read_stamp().await? {
        StoredStampRead::Unprovisioned => CentralOrganizationIdentity::Unprovisioned,
        StoredStampRead::Stamped(stamp) => CentralOrganizationIdentity::Stamped {
            organization_id: stamp.organization_id().clone(),
        },
    })
}
/// Prepares a cold alarm without fabricating an identity for an unprovisioned store.
pub async fn prepare_central_alarm(
    host: &impl CentralStore,
    store: &dyn fleet_signin::stamp_store::StampStore,
) -> Result<CentralOrganizationIdentity, StoreFailure> {
    let identity = read_central_organization(store).await?;
    if identity == CentralOrganizationIdentity::Unprovisioned {
        crate::central_statement_values::applied(
            host.run_batch(central_storage::StatementBatch {
                statements: vec![],
                alarm: crate::engine::query_registry::alarm_update(),
            })
            .await?,
        )?;
    }
    Ok(identity)
}
