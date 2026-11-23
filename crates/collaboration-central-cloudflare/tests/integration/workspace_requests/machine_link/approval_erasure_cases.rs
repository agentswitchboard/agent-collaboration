use super::{Fixture, Result};
use collaboration_central_conformance::approval_erasure_scenarios::{
    cutoff_and_correlated_marker, erase_batches_isolate_corruption_and_retain_only_live_markers,
};

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
