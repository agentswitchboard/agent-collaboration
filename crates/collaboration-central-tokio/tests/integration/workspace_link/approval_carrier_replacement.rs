//! Force replacement after a call reaches its old physical socket.
use super::{
    ApprovalLinkFixture, Cell, NativeApprovalFixture, PausePoint, RefCell, Result, signed_clock,
    start_session_link_fixture,
};
use collaboration_central_conformance::approval_authority_scenarios::displaced_carrier_writes_nothing;

#[tokio::test]
async fn tokio_displaced_approval_carrier_cannot_apply() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
    };
    let runtime = fixture
        .link
        .host
        .central(&fixture.link.organization_id)
        .await?;
    let held = RefCell::new(None);
    let arm = async {
        held.replace(Some(
            runtime
                .runtime()
                .pause_points()
                .hold_central(PausePoint::BeforeApprovalCarrierTurn),
        ));
        Ok(())
    };
    let replace = async {
        let mut pause = held.take().ok_or("approval carrier pause not armed")?;
        pause.until_held().await;
        let replacement = fixture.connect().await;
        pause.release();
        replacement
    };
    let result = displaced_carrier_writes_nothing(&fixture, arm, replace).await;
    fixture.link.close().await?;
    result
}
