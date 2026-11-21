use super::support::{A, Fixture, Result, signed};
use signed::{IssuerOrder, IssuerOrderHost, SignInKeys, prove_issuer_completion_order};

impl IssuerOrderHost for Fixture {
    fn issuer(&self) -> &SignInKeys {
        &self.keys
    }

    async fn set_receipt_time(&self, millis: i64) -> Result {
        self.clock
            .set_utc(chrono::DateTime::from_timestamp_millis(millis).ok_or("fixture clock")?)
            .await;
        Ok(())
    }

    async fn offer(&self, compact: &str) -> Result<u16> {
        Ok(self.provision(A, compact).await?.status)
    }

    async fn digest_status(&self) -> Result<u16> {
        Ok(self.digest(A).await?.status)
    }
}

#[tokio::test]
async fn issuer_retention_follows_both_completion_orders_and_keeps_the_last_success() -> Result {
    for order in [
        IssuerOrder::EarlierDispatchCompletesLast,
        IssuerOrder::LaterDispatchCompletesLast,
        IssuerOrder::MalformedCompletionArrivesLast,
    ] {
        let fixture = Fixture::new().await?;
        let result = prove_issuer_completion_order(&fixture, A, order).await;
        fixture.close().await?;
        result?;
    }
    Ok(())
}
