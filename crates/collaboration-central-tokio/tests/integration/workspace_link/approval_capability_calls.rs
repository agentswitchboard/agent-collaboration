use super::{
    Cell, NativeApprovalFixture, RefCell, Result, signed_clock, start_session_link_fixture,
};
use collaboration_central_conformance::approval_link_scenarios::{
    HolderRecoveryOrder, capability_ending_recovery,
};

#[tokio::test]
async fn tokio_ending_recovery_waits_for_both_capability_and_run_orders() -> Result<()> {
    for order in [
        HolderRecoveryOrder::RunBeforeCapabilities,
        HolderRecoveryOrder::CapabilitiesBeforeRun,
    ] {
        let fixture = NativeApprovalFixture {
            link: start_session_link_fixture(signed_clock()?).await?,
            before_alarm: Cell::new(0),
            delivery_answer: RefCell::default(),
            before_write: RefCell::default(),
            notice_recovery: RefCell::default(),
        };
        let observed = capability_ending_recovery(&fixture, order, 1002).await;
        fixture.link.close().await?;
        observed?;
    }
    Ok(())
}

#[tokio::test]
async fn tokio_both_owed_kinds_follow_the_socket_capabilities() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = collaboration_central_conformance::parent_approval_scenarios::both_owed_kinds_follow_the_socket_capabilities(&fixture).await;
    fixture.link.close().await?;
    result
}

use collaboration_central_conformance::parent_approval_scenarios::{
    LedgerFixture, MacDeliveryEvent, both_live_and_recovered_items_have_one_attempt,
    full_mac_call_ledger_recovers_on_committed_read,
};
use fleet_runtime::PausePoint as LedgerPause;

fn native_mac_event(event: MacDeliveryEvent) -> LedgerPause {
    match event {
        MacDeliveryEvent::BeforeEndingOffer => LedgerPause::BeforeMacEndingOffer,
        MacDeliveryEvent::BeforeEndingSocketWrite => LedgerPause::BeforeApprovalEndingWrite,
        MacDeliveryEvent::AfterEndingOffer => LedgerPause::AfterMacEndingOffer,
        MacDeliveryEvent::BeforeNoticeOffer => LedgerPause::BeforeMacNoticeOffer,
        MacDeliveryEvent::AfterNoticeOffer => LedgerPause::AfterMacNoticeOffer,
        MacDeliveryEvent::EndingRecoveryFinished => LedgerPause::MacEndingRecoveryFinished,
        MacDeliveryEvent::RegistrationRecoveryScheduled => {
            LedgerPause::MacEndingRegistrationRecoveryScheduled
        }
    }
}

impl LedgerFixture for NativeApprovalFixture {
    async fn hold_mac_event(&self, event: MacDeliveryEvent) -> Result<()> {
        let runtime = self.link.host.central(&self.link.organization_id).await?;
        self.before_write.replace(Some(
            runtime
                .runtime()
                .pause_points()
                .hold_central(native_mac_event(event)),
        ));
        Ok(())
    }
    async fn wait_mac_event_hold(&self, _event: MacDeliveryEvent) -> Result<()> {
        let mut held = self
            .before_write
            .take()
            .ok_or("missing Mac delivery hold")?;
        held.until_held().await;
        self.before_write.replace(Some(held));
        Ok(())
    }
    async fn release_mac_event(&self, _event: MacDeliveryEvent) -> Result<()> {
        self.before_write
            .take()
            .ok_or("missing Mac delivery hold")?
            .release();
        Ok(())
    }
    async fn mac_event_count(&self, event: MacDeliveryEvent) -> Result<u64> {
        let runtime = self.link.host.central(&self.link.organization_id).await?;
        Ok(runtime
            .runtime()
            .pause_points()
            .central_count(native_mac_event(event)))
    }
    async fn wait_mac_event_count(&self, event: MacDeliveryEvent, count: u64) -> Result<()> {
        let runtime = self.link.host.central(&self.link.organization_id).await?;
        runtime
            .runtime()
            .pause_points()
            .until_central_count(native_mac_event(event), count)
            .await;
        Ok(())
    }
    async fn mac_timer_waits(&self) -> Result<u64> {
        Ok(0)
    }
}

async fn ledger_fixture() -> Result<NativeApprovalFixture> {
    Ok(NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    })
}

#[tokio::test]
async fn tokio_live_and_recovered_mac_calls_share_one_attempt() -> Result<()> {
    let fixture = ledger_fixture().await?;
    let observed = both_live_and_recovered_items_have_one_attempt(&fixture, 1002).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_full_mac_call_ledger_recovers_after_a_committed_read() -> Result<()> {
    let fixture = ledger_fixture().await?;
    let observed = full_mac_call_ledger_recovers_on_committed_read(&fixture).await;
    fixture.link.close().await?;
    observed
}
