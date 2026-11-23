use super::{
    Cell, NativeApprovalFixture, RefCell, Result, signed_clock, start_session_link_fixture,
};
use collaboration_central_conformance::approval_erasure_scenarios::{
    cutoff_and_correlated_marker, erase_batches_isolate_corruption_and_retain_only_live_markers,
};

#[tokio::test]
async fn tokio_approval_cutoff_and_marker_on_the_real_link() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = cutoff_and_correlated_marker(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_approval_erasure_batches_and_retired_markers() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result = erase_batches_isolate_corruption_and_retain_only_live_markers(&fixture).await;
    fixture.link.close().await?;
    result
}

#[tokio::test]
async fn tokio_approval_erasure_hides_replays_and_releases_owed_capacity() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let result =
        collaboration_central_conformance::parent_approval_scenarios::destination_replays_and_unpaid_ending_at_cutoff(&fixture).await;
    fixture.link.close().await?;
    result
}
