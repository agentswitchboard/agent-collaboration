use std::{cell::RefCell, num::NonZeroUsize};

use central_link_protocol::{DataFrame, LinkFrame, approval_rpc::ApprovalHolderTarget};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, ReadOutcome, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_conformance::{
    ConformanceResult as Result,
    approval_dispatch_rollback::{
        ApprovalDispatchCase, ApprovalDispatchRollbackHost, ApprovalWriteTrace, approval_snapshot,
        verify_approval_dispatch_rollback,
    },
    approval_link_scenarios::ROUTER_RUN,
    signin_fixtures::{
        Person,
        approval_socket::{PersonApprovalEndpoint, PersonApprovalSocket},
    },
};
use collaboration_central_tokio::test_support::start_session_link_fixture;
use fleet_runtime::{HeldPause, PausePoint};
use reqwest_websocket::{RequestBuilderExt, WebSocket};
use router_clock::RuntimeClock;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use super::fixture::{Fixture, INSTALLATION, receive, signed_clock};

struct ApprovalRollbackFixture {
    machine: Fixture,
    notice_recovery: RefCell<Option<HeldPause>>,
}

impl ApprovalRollbackFixture {
    async fn open() -> Result<Self> {
        Ok(Self {
            machine: start_session_link_fixture(signed_clock()?).await?,
            notice_recovery: RefCell::default(),
        })
    }
}

impl ApprovalDispatchRollbackHost for ApprovalRollbackFixture {
    fn target(&self) -> Result<ApprovalHolderTarget> {
        Ok(ApprovalHolderTarget {
            organization_id: self.machine.organization_id.clone(),
            machine_id: INSTALLATION.parse()?,
            enrollment_id: self.machine.enrollment_id.clone(),
            router_run: ROUTER_RUN.parse()?,
        })
    }

    fn now_ms(&self) -> i64 {
        self.machine.clock.utc_now().timestamp_millis()
    }

    async fn set_time(&self, millis: i64) -> Result<()> {
        self.machine.clock.set_utc(
            chrono::DateTime::from_timestamp_millis(millis).ok_or("invalid fixture time")?,
        );
        Ok(())
    }

    async fn connect_machine(&self) -> Result<WebSocket> {
        let token = self.machine.mint_machine_token().await?;
        let mut socket = self
            .machine
            .client()?
            .get(self.machine.url(self.machine.organization_id.as_str()))
            .bearer_auth(token)
            .upgrade()
            .send()
            .await?
            .into_websocket()
            .await?;
        assert!(matches!(
            receive(&mut socket).await?,
            LinkFrame::Data(DataFrame::Welcome { .. })
        ));
        Ok(socket)
    }

    async fn connect_person(&self, person: &Person) -> Result<PersonApprovalSocket> {
        PersonApprovalEndpoint {
            client: &self.machine.client()?,
            origin: &self.machine.origin(),
            now_seconds: self.now_ms().div_euclid(1_000),
        }
        .connect(person)
        .await
    }

    async fn arm(&self, cut: Option<NonZeroUsize>) -> Result<()> {
        let owner = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        owner
            .runtime()
            .statement_host()
            .approval_write_probe()
            .arm(cut);
        Ok(())
    }

    async fn trace(&self) -> Result<ApprovalWriteTrace> {
        let owner = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        Ok(owner
            .runtime()
            .statement_host()
            .approval_write_probe()
            .trace()
            .ok_or("no applying loan took the write probe")?)
    }

    async fn prepare_notice_recovery(&self) -> Result<()> {
        let owner = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        let held = owner
            .runtime()
            .pause_points()
            .hold_central(PausePoint::AfterApprovalNoticeRecovery);
        assert!(self.notice_recovery.replace(Some(held)).is_none());
        Ok(())
    }

    async fn finish_notice_recovery(&self) -> Result<()> {
        let mut held = self
            .notice_recovery
            .take()
            .ok_or("no notice recovery barrier")?;
        held.until_held().await;
        held.release();
        Ok(())
    }

    async fn ending_recovery_count(&self) -> Result<u64> {
        let central = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        Ok(central
            .runtime()
            .pause_points()
            .central_count(PausePoint::MacEndingRecoveryFinished))
    }

    async fn wait_ending_recovery(&self, minimum: u64) -> Result<()> {
        let central = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        central
            .runtime()
            .pause_points()
            .until_central_count(PausePoint::MacEndingRecoveryFinished, minimum)
            .await;
        Ok(())
    }

    async fn scheduled_delivery_count(&self) -> Result<u64> {
        let central = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        Ok(central
            .runtime()
            .pause_points()
            .approval_delivery_scheduled_count())
    }

    async fn drive_due(&self) -> Result<()> {
        let owner = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        let _completion = owner.after_event().await?;
        Ok(())
    }

    async fn read(&self, statement: Statement) -> Result<ReadOutcome> {
        let owner = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        Ok(owner.runtime().statement_host().read(statement).await?)
    }

    async fn write(&self, statement: Statement) -> Result<()> {
        let owner = self
            .machine
            .host
            .central(&self.machine.organization_id)
            .await?;
        let result = owner
            .runtime()
            .statement_host()
            .run_batch(StatementBatch {
                statements: vec![GuardedStatement {
                    statement,
                    expect: RowExpectation::Exactly(1),
                }],
                alarm: AlarmUpdate::Unchanged,
            })
            .await?;
        assert!(matches!(result, BatchOutcome::Applied { .. }));
        Ok(())
    }

    async fn snapshot(&self, reopen: bool) -> Result<Vec<Vec<Vec<SqlValue>>>> {
        if !reopen {
            return approval_snapshot(async |statement| self.read(statement).await).await;
        }
        let file = self
            .machine
            .root_path()
            .join("modern")
            .join("central")
            .join(format!("{}.sqlite", self.machine.organization_id.as_str()));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::new().filename(file).read_only(true))
            .await?;
        let result = approval_snapshot(async |statement| {
            let mut connection = pool.acquire().await?;
            Ok(fleet_store_sqlx::read_lent_statement(&mut connection, statement).await?)
        })
        .await;
        pool.close().await;
        result
    }

    async fn close(self) -> Result<()> {
        self.machine.close().await
    }
}

#[tokio::test]
async fn production_due_approval_rolls_back_every_write() -> Result<()> {
    verify_approval_dispatch_rollback(
        ApprovalRollbackFixture::open,
        ApprovalDispatchCase::CorruptAttempt,
    )
    .await?;
    verify_approval_dispatch_rollback(
        ApprovalRollbackFixture::open,
        ApprovalDispatchCase::DeadlineFloor,
    )
    .await
}

#[tokio::test]
async fn production_first_person_decisions_roll_back_every_write() -> Result<()> {
    for case in [
        ApprovalDispatchCase::FirstPersonSaved,
        ApprovalDispatchCase::FirstPersonLate,
        ApprovalDispatchCase::FirstPersonInvalid,
    ] {
        verify_approval_dispatch_rollback(ApprovalRollbackFixture::open, case).await?;
    }
    Ok(())
}

#[tokio::test]
async fn production_link_approval_floors_roll_back_with_each_operation() -> Result<()> {
    for case in [
        ApprovalDispatchCase::HolderFloor,
        ApprovalDispatchCase::SessionReadFloor,
        ApprovalDispatchCase::NoticeRecoveryFloor,
    ] {
        verify_approval_dispatch_rollback(ApprovalRollbackFixture::open, case).await?;
    }
    Ok(())
}
