use central_storage::SqlValue;
use collaboration_central_conformance::{
    person_approval_cases,
    signin_fixtures::{
        approval_routes::{
            AdmissionCase, AuthenticationCase, Case, PersonApprovalHarness, ReplayCase, check,
        },
        approval_socket::PersonApprovalEndpoint,
    },
};
use collaboration_central_tokio::WorkspaceLink;
use fleet_runtime::PausePoint;
use serde_json::Value;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::support::{A, Fixture, Result, post};

struct ApprovalFixture<'fixture> {
    fixture: &'fixture Fixture,
    public: Option<(&'fixture reqwest::Client, &'fixture str)>,
}

impl PersonApprovalHarness for ApprovalFixture<'_> {
    fn person_socket(&self) -> Option<PersonApprovalEndpoint<'_>> {
        self.public.map(|(client, origin)| PersonApprovalEndpoint {
            client,
            origin,
            now_seconds: self.fixture.clock_time(),
        })
    }

    async fn post(
        &self,
        path: &str,
        body: &Value,
        headers: Vec<(&'static str, String)>,
    ) -> Result<(u16, Vec<u8>)> {
        let reply = post(
            &self.fixture.client(),
            path,
            serde_json::to_string(body)?,
            headers,
        )
        .await?;
        Ok((reply.status, reply.body))
    }

    async fn advance_at_approval_sample<Reply>(
        &self,
        send: impl std::future::Future<Output = Result<Reply>>,
        advance_ms: u64,
    ) -> Result<Reply> {
        let runtime = self.fixture.runtime(A).await?;
        let mut held = runtime
            .runtime()
            .pause_points()
            .hold_central(PausePoint::BeforeSignInSample);
        tokio::pin!(send);
        tokio::select! {
            early = &mut send => {
                early?;
                return Err("approval request completed before the applying sample hold".into());
            },
            () = held.until_held() => {}
        }
        self.fixture
            .clock
            .advance(Duration::from_millis(advance_ms))
            .await?;
        held.release();
        send.await
    }

    async fn write(&self, statement: &'static str, args: Vec<SqlValue>) -> Result {
        self.fixture.apply(A, statement, args).await
    }

    async fn lose_reply_after_apply(&self, operation: &str) -> Result {
        self.fixture
            .runtime(A)
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
        self.fixture.rows(A, statement, args).await
    }
}

async fn run_signed(case: Case) -> Result {
    let fixture = Fixture::new().await?;
    fixture.ready().await?;
    let observed = check(
        &ApprovalFixture {
            fixture: &fixture,
            public: None,
        },
        case,
    )
    .await;
    fixture.close().await?;
    observed
}

async fn run_socket(case: Case) -> Result {
    let fixture = Fixture::new().await?;
    fixture.ready().await?;
    let link = WorkspaceLink::new(fixture.host.clone());
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let client = reqwest::Client::builder().no_proxy().build()?;
    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    let router = link.router();
    let serving = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    let observed = check(
        &ApprovalFixture {
            fixture: &fixture,
            public: Some((&client, &origin)),
        },
        case,
    )
    .await;
    link.begin_shutdown();
    cancel.cancel();
    serving.await??;
    link.shutdown().await;
    fixture.close().await?;
    observed
}

macro_rules! approval_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> Result {
            run_signed($case).await
        }
    };
}

macro_rules! socket_approval_case {
    ($name:ident, $case:expr) => {
        #[tokio::test]
        async fn $name() -> Result {
            run_socket($case).await
        }
    };
}

person_approval_cases!(approval_case, socket_approval_case);
socket_approval_case!(
    socket_page_uses_the_ending_when_stored_cutoff_is_corrupt,
    Case::ListWithCorruptStoredCutoff
);
approval_case!(
    ended_person_approval_is_hidden_at_the_retention_cutoff,
    Case::ErasureCutoff
);

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
