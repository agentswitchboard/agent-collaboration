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

use collaboration_central_conformance::parent_approval_scenarios::{
    LedgerFixture, MacDeliveryEvent, both_live_and_recovered_items_have_one_attempt,
    full_mac_call_ledger_recovers_on_committed_read, unsupported_mac_call_registers_no_wait,
};

impl LedgerFixture for Fixture {
    async fn hold_mac_event(&self, event: MacDeliveryEvent) -> Result<()> {
        assert!(
            self.runtime()
                .control::<bool>("hold-central", &event.control())
                .await?
        );
        Ok(())
    }
    async fn wait_mac_event_hold(&self, event: MacDeliveryEvent) -> Result<()> {
        assert!(
            self.runtime()
                .control::<bool>("until-held-central", &event.control())
                .await?
        );
        Ok(())
    }
    async fn release_mac_event(&self, event: MacDeliveryEvent) -> Result<()> {
        assert!(
            self.runtime()
                .control::<bool>("release-central", &event.control())
                .await?
        );
        Ok(())
    }
    async fn mac_event_count(&self, event: MacDeliveryEvent) -> Result<u64> {
        Ok(self
            .runtime()
            .control::<u64>("mac-delivery-count", &event.control())
            .await?)
    }
    async fn wait_mac_event_count(&self, event: MacDeliveryEvent, count: u64) -> Result<()> {
        assert!(
            self.runtime()
                .control::<bool>("until-mac-delivery-count", &(event.control(), count))
                .await?
        );
        Ok(())
    }
    async fn hibernate_ledger_socket(&self) -> Result<()> {
        <Self as collaboration_central_conformance::approval_link_scenarios::ApprovalLinkFixture>::reopen_after_clock_rollback(self).await
    }
    async fn mac_timer_waits(&self) -> Result<u64> {
        Ok(self.runtime().control::<u64>("mac-call-waits", &()).await?)
    }
}

#[tokio::test]
async fn workerd_live_and_recovered_mac_calls_share_one_attempt() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = both_live_and_recovered_items_have_one_attempt(&fixture, 1008).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_full_mac_call_ledger_recovers_after_a_committed_read() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = full_mac_call_ledger_recovers_on_committed_read(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_unsupported_mac_call_starts_no_controlled_timer_wait() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = unsupported_mac_call_registers_no_wait(&fixture).await;
    fixture.close().await?;
    observed
}
