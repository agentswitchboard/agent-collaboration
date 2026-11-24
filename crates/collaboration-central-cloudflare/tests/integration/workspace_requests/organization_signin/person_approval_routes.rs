use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::{
    ConformanceResult, person_approval_cases,
    signin_fixtures::approval_routes::{
        AdmissionCase, AuthenticationCase, Case, PersonApprovalHarness, ReplayCase, check,
    },
};
use serde_json::Value;

use super::support::Fixture;

impl PersonApprovalHarness for Fixture {
    async fn post(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> ConformanceResult<(u16, Vec<u8>)> {
        Ok(self.post_bytes(path, body, &headers).await)
    }

    async fn post_after_verification(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
        advance_ms: u64,
    ) -> ConformanceResult<(u16, Vec<u8>)> {
        use collaboration_central_conformance::signin_fixtures::NOW_SECONDS;

        let runtime = self.runtime().await;
        let point = "beforeSignInSample";
        if !runtime.control::<bool>("hold-central", &point).await? {
            return Err("could not arm the C3 post-verification hold".into());
        }
        let send = self.post_bytes(path, body, &headers);
        let advance = async {
            if !runtime
                .control::<bool>("until-held-central", &point)
                .await?
            {
                return Err("C3 request never reached the post-verification hold".into());
            }
            let clock_ms = NOW_SECONDS * 1000 + i64::try_from(advance_ms)?;
            self.clock.set_utc(
                chrono::DateTime::from_timestamp_millis(clock_ms)
                    .ok_or("fixture clock after C3 verification")?,
            );
            self.runtime().await;
            if !runtime.control::<bool>("release-central", &point).await? {
                return Err("could not release the C3 post-verification hold".into());
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        tokio::pin!(send);
        tokio::select! {
            early = &mut send => Err(format!("C3 request completed before the post-verification hold: {}", early.0).into()),
            advanced = advance => {
                advanced?;
                Ok(send.await)
            }
        }
    }

    async fn write(&self, statement: &'static str, args: Vec<SqlValue>) -> ConformanceResult<()> {
        self.execute(statement, args).await;
        Ok(())
    }

    async fn lose_reply_after_apply(&self, operation: &str) -> ConformanceResult<()> {
        if !self
            .runtime()
            .await
            .control::<bool>("lose-reply", &operation)
            .await?
        {
            return Err("post-commit fault was not armed".into());
        }
        Ok(())
    }

    async fn read_with_args(
        &self,
        statement: &'static str,
        args: Vec<SqlValue>,
    ) -> ConformanceResult<Vec<Vec<SqlValue>>> {
        Ok(self
            .runtime()
            .await
            .read(Statement {
                sql: statement,
                args,
            })
            .await?
            .rows)
    }
}

macro_rules! approval_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> ConformanceResult<()> {
            let fixture = Fixture::new().await;
            if fixture.provision().await.0 != 200 {
                return Err("fixture provisioning refused".into());
            }
            fixture.floors().await;
            check(&fixture, $case).await?;
            fixture.close().await;
            Ok(())
        }
    };
}

person_approval_cases!(approval_case);
approval_case!(
    person_page_uses_the_ending_when_stored_cutoff_is_corrupt,
    Case::ListWithCorruptStoredCutoff
);
approval_case!(
    ended_person_approval_is_hidden_at_the_retention_cutoff,
    Case::ErasureCutoff
);

#[tokio::test]
async fn approval_preflight_refuses_method_then_headers_then_address() -> ConformanceResult<()> {
    let fixture = Fixture::new().await;
    let client = reqwest::Client::builder().no_proxy().build()?;
    let observed = collaboration_central_conformance::signin_fixtures::check_approval_preflight(
        &client,
        &fixture.host.signin_origin(),
        Some("collaboration-central-link-entry"),
    )
    .await;
    fixture.close().await;
    observed
}
