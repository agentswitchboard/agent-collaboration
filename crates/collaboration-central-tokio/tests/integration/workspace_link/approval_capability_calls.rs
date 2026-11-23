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
        };
        let observed = capability_ending_recovery(&fixture, order, 1002).await;
        fixture.link.close().await?;
        observed?;
    }
    Ok(())
}
