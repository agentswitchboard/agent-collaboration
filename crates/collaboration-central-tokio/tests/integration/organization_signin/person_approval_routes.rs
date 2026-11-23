use central_storage::SqlValue;
use collaboration_central_conformance::{
    person_approval_cases,
    signin_fixtures::approval_routes::{
        AdmissionCase, AuthenticationCase, Case, PersonApprovalHarness, ReplayCase, check,
    },
};
use fleet_runtime::PausePoint;
use serde_json::Value;
use std::time::Duration;

use super::support::{A, Fixture, Result, post};

impl PersonApprovalHarness for Fixture {
    async fn post(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> Result<(u16, Vec<u8>)> {
        let reply = post(&self.client(), path, serde_json::to_string(body)?, headers).await?;
        Ok((reply.status, reply.body))
    }

    async fn post_after_verification(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
        advance_ms: u64,
    ) -> Result<(u16, Vec<u8>)> {
        let runtime = self.runtime(A).await?;
        let mut held = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::BeforeSignInSample);
        let send = PersonApprovalHarness::post(self, path, body, headers);
        let advance = async {
            held.until_held().await;
            self.clock
                .advance(Duration::from_millis(advance_ms))
                .await?;
            held.release();
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        tokio::pin!(send);
        tokio::select! {
            early = &mut send => Err(format!("C3 request completed before the post-verification hold: {}", early?.0).into()),
            advanced = advance => {
                advanced?;
                send.await
            }
        }
    }

    async fn write(&self, statement: &'static str, args: Vec<SqlValue>) -> Result {
        self.apply(A, statement, args).await
    }

    async fn lose_reply_after_apply(&self, operation: &str) -> Result {
        self.runtime(A)
            .await?
            .runtime()
            .lose_reply_after_apply(operation.parse()?);
        Ok(())
    }

    async fn read_with_args(
        &self,
        statement: &'static str,
        args: Vec<SqlValue>,
    ) -> Result<Vec<Vec<SqlValue>>> {
        self.rows(A, statement, args).await
    }
}

macro_rules! approval_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> Result {
            let fixture = Fixture::new().await?;
            fixture.ready().await?;
            check(&fixture, $case).await?;
            fixture.close().await
        }
    };
}

person_approval_cases!(approval_case);

#[tokio::test]
async fn approval_preflight_refuses_method_then_headers_then_address() -> Result {
    let fixture = Fixture::new().await?;
    let observed = collaboration_central_conformance::signin_fixtures::check_approval_preflight(
        &fixture.client(),
        "http://central.local",
        None,
    )
    .await;
    fixture.close().await?;
    observed
}
