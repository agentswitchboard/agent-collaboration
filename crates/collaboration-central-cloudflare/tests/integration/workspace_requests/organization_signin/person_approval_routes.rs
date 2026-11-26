use central_storage::{SqlValue, Statement};
use collaboration_central_conformance::{
    ConformanceResult, person_approval_cases,
    signin_fixtures::{
        approval_routes::{AdmissionCase, Case, PersonApprovalHarness, ReplayCase, check},
        approval_socket::PersonApprovalEndpoint,
    },
};
use serde_json::Value;

use super::support::{Fixture, REVISIONS};

struct ApprovalFixture<'fixture> {
    fixture: &'fixture Fixture,
    socket_client: reqwest::Client,
}

impl PersonApprovalHarness for ApprovalFixture<'_> {
    fn person_socket(&self) -> PersonApprovalEndpoint<'_> {
        PersonApprovalEndpoint {
            client: &self.socket_client,
            origin: self.fixture.host.public_origin(),
            now_seconds: self.fixture.host.now_seconds(),
        }
    }

    async fn post_revision(&self, body: &Value) -> ConformanceResult<(u16, Vec<u8>)> {
        Ok(self.fixture.post_bytes(REVISIONS, body, &[]).await)
    }

    async fn advance_at_approval_sample<Reply>(
        &self,
        send: impl std::future::Future<Output = ConformanceResult<Reply>>,
        advance_ms: u64,
    ) -> ConformanceResult<Reply> {
        use collaboration_central_conformance::signin_fixtures::NOW_SECONDS;

        let runtime = self.fixture.runtime().await;
        let point = "beforeSignInSample";
        if !runtime.control::<bool>("hold-central", &point).await? {
            return Err("could not arm the approval applying sample hold".into());
        }
        tokio::pin!(send);
        let reached = tokio::select! {
            early = &mut send => {
                early?;
                return Err("approval request completed before the applying sample hold".into());
            },
            reached = runtime.control::<bool>("until-held-central", &point) => reached?,
        };
        if !reached {
            return Err("approval request never reached the applying sample hold".into());
        }
        let clock_ms = NOW_SECONDS * 1000 + i64::try_from(advance_ms)?;
        self.fixture.clock.set_utc(
            chrono::DateTime::from_timestamp_millis(clock_ms)
                .ok_or("fixture clock before the approval applying sample")?,
        );
        self.fixture.runtime().await;
        if !runtime.control::<bool>("release-central", &point).await? {
            return Err("could not release the approval applying sample hold".into());
        }
        send.await
    }

    async fn write(&self, statement: &'static str, args: Vec<SqlValue>) -> ConformanceResult<()> {
        self.fixture.execute(statement, args).await;
        Ok(())
    }

    async fn lose_reply_after_apply(&self, operation: &str) -> ConformanceResult<()> {
        if !self
            .fixture
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
            .fixture
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

async fn run(case: Case) -> ConformanceResult<()> {
    let fixture = Fixture::new().await;
    if fixture.provision().await.0 != 200 {
        return Err("fixture provisioning refused".into());
    }
    fixture.floors().await;
    let observed = check(
        &ApprovalFixture {
            fixture: &fixture,
            socket_client: reqwest::Client::builder().no_proxy().build()?,
        },
        case,
    )
    .await;
    fixture.close().await;
    observed
}

macro_rules! socket_approval_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> ConformanceResult<()> {
            run($case).await
        }
    };
}

person_approval_cases!(socket_approval_case);
socket_approval_case!(
    socket_page_uses_the_ending_when_stored_cutoff_is_corrupt,
    Case::ListWithCorruptStoredCutoff
);
socket_approval_case!(
    socket_hides_ended_approval_at_the_retention_cutoff,
    Case::ErasureCutoff
);

#[tokio::test]
async fn private_entry_returns_404_for_removed_approval_routes() -> ConformanceResult<()> {
    let fixture = Fixture::new().await;
    if fixture.provision().await.0 != 200 {
        return Err("fixture provisioning refused".into());
    }
    fixture.floors().await;
    let client = reqwest::Client::builder().no_proxy().build()?;
    let observed =
        collaboration_central_conformance::signin_fixtures::check_removed_approval_routes(
            &client,
            &fixture.host.signin_origin(),
            Some("collaboration-central-link-entry"),
        )
        .await;
    fixture.close().await;
    observed
}
