use super::{Fixture, Result};
use collaboration_central_conformance::approval_erasure_scenarios::{
    MissedApprovalCleanupFixture, cutoff_and_correlated_marker,
    erase_batches_isolate_corruption_and_retain_only_live_markers,
    missed_cleanup_is_recovered_by_the_next_mark,
};
use collaboration_central_conformance::approval_link_scenarios::ApprovalLinkFixture;

impl MissedApprovalCleanupFixture for Fixture {
    async fn miss_cleanup_at(&self, mark: i64) -> Result<()> {
        let runtime = self.runtime();
        let alarm: Option<i64> = runtime.control("raw-alarm-time", &()).await?;
        assert_eq!(alarm, Some(mark * 1000));
        let completed: u64 = runtime.control("raw-alarm-count", &()).await?;
        // Advancing the injected clock leaves the actual Workerd alarm undispatched.
        self.advance(u64::try_from(mark + 1 - self.now_seconds())?)
            .await?;
        let after: u64 = runtime.control("raw-alarm-count", &()).await?;
        assert_eq!(after, completed);
        Ok(())
    }

    fn prepare_catch_up(
        &self,
        socket: reqwest_websocket::WebSocket,
        _mark: i64,
    ) -> impl std::future::Future<Output = Result<reqwest_websocket::WebSocket>> {
        // The scenario dispatches the retained platform alarm at the next mark.
        std::future::ready(Ok(socket))
    }
}

#[tokio::test]
async fn workerd_missed_approval_cleanup_is_recovered_by_the_next_mark() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = missed_cleanup_is_recovered_by_the_next_mark(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_approval_cutoff_and_marker_on_the_real_link() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = cutoff_and_correlated_marker(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_approval_erasure_batches_and_retired_markers() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result = erase_batches_isolate_corruption_and_retain_only_live_markers(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_approval_erasure_hides_replays_and_releases_owed_capacity() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let result =
        collaboration_central_conformance::parent_approval_scenarios::destination_replays_and_unpaid_ending_at_cutoff(&fixture).await;
    fixture.close().await?;
    result
}

#[tokio::test]
async fn workerd_session_page_and_durable_cutoff() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = collaboration_central_conformance::parent_approval_scenarios::session_page_and_durable_cutoff(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_queued_ending_uses_persisted_cutoff() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = collaboration_central_conformance::parent_approval_scenarios::queued_ending_uses_persisted_cutoff(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_tied_cleanup_work_yields_the_approval_lane() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = collaboration_central_conformance::approval_cleanup_fairness::tied_cleanup_work_yields_the_approval_lane(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_corrupt_cleanup_is_charged_without_stopping_healthy_work() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = collaboration_central_conformance::approval_cleanup_fairness::corrupt_cleanup_is_charged_without_stopping_healthy_work(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_session_page_uses_the_ending_when_stored_cutoff_is_corrupt() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = collaboration_central_conformance::parent_approval_scenarios::session_page_uses_the_ending_when_stored_cutoff_is_corrupt(&fixture).await;
    fixture.close().await?;
    observed
}

#[tokio::test]
async fn workerd_corrupt_parked_anchor_does_not_undo_the_marker_page() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let observed = collaboration_central_conformance::approval_cleanup_fairness::corrupt_parked_anchor_does_not_undo_the_marker_page(&fixture).await;
    fixture.close().await?;
    observed
}
