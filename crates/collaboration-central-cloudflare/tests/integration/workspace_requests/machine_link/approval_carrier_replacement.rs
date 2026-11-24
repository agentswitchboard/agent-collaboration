//! Force replacement after a call reaches its old physical socket.
use super::{ApprovalLinkFixture, Fixture, Result};
use collaboration_central_conformance::approval_authority_scenarios::displaced_carrier_writes_nothing;

#[tokio::test]
async fn workerd_displaced_approval_carrier_cannot_apply() -> Result<()> {
    let fixture = Fixture::with_production_dispatcher().await?;
    let runtime = fixture.runtime();
    let point = "beforeApprovalCarrierTurn";
    let arm = async {
        if !runtime.control::<bool>("hold-central", &point).await? {
            return Err("approval carrier pause not armed".into());
        }
        Ok(())
    };
    let replace = async {
        if !runtime
            .control::<bool>("until-held-central", &point)
            .await?
        {
            return Err("approval carrier did not reach the turn".into());
        }
        let replacement = fixture.connect().await;
        if !runtime.control::<bool>("release-central", &point).await? {
            return Err("approval carrier pause did not release".into());
        }
        replacement
    };
    let result = displaced_carrier_writes_nothing(&fixture, arm, replace).await;
    fixture.close().await?;
    result
}
