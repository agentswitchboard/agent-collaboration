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

#[tokio::test]
async fn tokio_session_page_and_durable_cutoff() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::parent_approval_scenarios::session_page_and_durable_cutoff(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_queued_ending_uses_persisted_cutoff() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::parent_approval_scenarios::queued_ending_uses_persisted_cutoff(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_tied_cleanup_work_yields_the_approval_lane() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::approval_cleanup_fairness::tied_cleanup_work_yields_the_approval_lane(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_corrupt_cleanup_is_charged_without_stopping_healthy_work() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::approval_cleanup_fairness::corrupt_cleanup_is_charged_without_stopping_healthy_work(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_session_page_uses_the_ending_when_stored_cutoff_is_corrupt() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::parent_approval_scenarios::session_page_uses_the_ending_when_stored_cutoff_is_corrupt(&fixture).await;
    fixture.link.close().await?;
    observed
}

#[tokio::test]
async fn tokio_corrupt_parked_anchor_does_not_undo_the_marker_page() -> Result<()> {
    let fixture = NativeApprovalFixture {
        link: start_session_link_fixture(signed_clock()?).await?,
        before_alarm: Cell::new(0),
        delivery_answer: RefCell::default(),
        before_write: RefCell::default(),
        notice_recovery: RefCell::default(),
    };
    let observed = collaboration_central_conformance::approval_cleanup_fairness::corrupt_parked_anchor_does_not_undo_the_marker_page(&fixture).await;
    fixture.link.close().await?;
    observed
}
