use central_storage::SqlValue;
use collaboration_central_conformance::{
    ConformanceResult, person_approval_cases,
    signin_fixtures::approval_routes::{
        AdmissionCase, AnswerClass, AuthenticationCase, Case, PersonApprovalHarness, ReplayCase,
        check, factor,
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

    async fn read(&self, statement: &'static str) -> ConformanceResult<Vec<Vec<SqlValue>>> {
        Ok(self.read(statement).await.rows)
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
