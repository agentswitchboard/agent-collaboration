use super::{Fixture, Result};
use collaboration_central_conformance::approval_link_scenarios::{
    HolderRecoveryOrder, capability_ending_recovery,
};

#[tokio::test]
async fn workerd_ending_recovery_waits_for_both_capability_and_run_orders() -> Result<()> {
    for order in [
        HolderRecoveryOrder::RunBeforeCapabilities,
        HolderRecoveryOrder::CapabilitiesBeforeRun,
    ] {
        let fixture = Fixture::with_production_dispatcher().await?;
        let observed = capability_ending_recovery(&fixture, order, 1008).await;
        fixture.close().await?;
        observed?;
    }
    Ok(())
}

#[tokio::test]
async fn workerd_both_owed_kinds_follow_the_socket_capabilities() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = collaboration_central_conformance::parent_approval_scenarios::both_owed_kinds_follow_the_socket_capabilities(&fixture).await;
    fixture.close().await?;
    result
}
