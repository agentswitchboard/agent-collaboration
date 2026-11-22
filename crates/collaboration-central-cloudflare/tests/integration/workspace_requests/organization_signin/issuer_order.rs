use super::support::{DIGEST, Fixture, ORG, PROVISION};
use collaboration_central_conformance::{
    ConformanceResult as Result,
    signin_fixtures::{IssuerOrder, IssuerOrderHost, SignInKeys, prove_issuer_completion_order},
};
use serde_json::json;

impl IssuerOrderHost for Fixture {
    fn issuer_fixture(&self) -> &SignInKeys {
        &self.keys
    }

    async fn set_receipt_time(&self, millis: i64) -> Result<()> {
        self.clock
            .set_utc(chrono::DateTime::from_timestamp_millis(millis).ok_or("fixture clock")?);
        self.runtime().await;
        Ok(())
    }

    async fn offer_signed_stamp(&self, compact: &str) -> Result<u16> {
        Ok(self
            .post(
                PROVISION,
                &json!({"organizationId":ORG,"organizationStatement":compact}),
            )
            .await
            .0)
    }

    async fn digest_status(&self) -> Result<u16> {
        Ok(self.post(DIGEST, &json!({"organizationId":ORG})).await.0)
    }
}

#[tokio::test]
async fn issuer_retention_follows_both_completion_orders_and_keeps_the_last_success() -> Result<()>
{
    for order in [
        IssuerOrder::EarlierDispatchCompletesLast,
        IssuerOrder::LaterDispatchCompletesLast,
        IssuerOrder::MalformedCompletionArrivesLast,
    ] {
        let fixture = Fixture::new().await;
        let result = prove_issuer_completion_order(&fixture, ORG, order).await;
        fixture.close().await;
        result?;
    }
    Ok(())
}
