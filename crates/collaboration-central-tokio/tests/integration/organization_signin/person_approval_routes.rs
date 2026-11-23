use central_storage::SqlValue;
use collaboration_central_conformance::{
    person_approval_cases,
    signin_fixtures::approval_routes::{
        AdmissionCase, AnswerClass, AuthenticationCase, Case, PersonApprovalHarness, ReplayCase,
        check, factor,
    },
};
use serde_json::Value;

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

    async fn read(&self, statement: &'static str) -> Result<Vec<Vec<SqlValue>>> {
        self.rows(A, statement, Vec::new()).await
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
