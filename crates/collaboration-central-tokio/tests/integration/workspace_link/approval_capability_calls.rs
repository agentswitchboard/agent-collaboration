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
