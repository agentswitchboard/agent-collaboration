use std::num::NonZeroUsize;

use central_link_protocol::approval_rpc::ApprovalHolderTarget;
use central_storage::{ReadOutcome, SqlValue, Statement};
use collaboration_central_conformance::{
    ConformanceResult as Result,
    approval_dispatch_rollback::{
        ApprovalDispatchCase, ApprovalDispatchRollbackHost, ApprovalWriteTrace, approval_snapshot,
        verify_approval_dispatch_rollback,
    },
    approval_link_scenarios::ApprovalLinkFixture,
    signin_fixtures::{
        Person,
        approval_socket::{PersonApprovalEndpoint, PersonApprovalSocket},
    },
};
use reqwest_websocket::WebSocket;
use router_clock::RuntimeClock;

use super::fixture::Fixture;

struct ApprovalRollbackFixture(Fixture);

impl ApprovalRollbackFixture {
    async fn open() -> Result<Self> {
        Ok(Self(Fixture::with_production_dispatcher().await?))
    }
}

impl ApprovalDispatchRollbackHost for ApprovalRollbackFixture {
    fn target(&self) -> Result<ApprovalHolderTarget> {
        self.0.target()
    }

    fn now_ms(&self) -> i64 {
        self.0.clock.utc_now().timestamp_millis()
    }

    async fn set_time(&self, millis: i64) -> Result<()> {
        self.0.set_wall_millis(millis).await
    }

    async fn connect_machine(&self) -> Result<WebSocket> {
        self.0.connect_without_capabilities().await
    }

    async fn connect_person(&self, person: &Person) -> Result<PersonApprovalSocket> {
        PersonApprovalEndpoint {
            client: &self.0.client,
            origin: self.0.host.public_origin(),
            now_seconds: self.now_ms().div_euclid(1_000),
        }
        .connect(person)
        .await
    }

    async fn arm(&self, cut: Option<NonZeroUsize>) -> Result<()> {
        assert!(
            self.0
                .runtime()
                .control::<bool>("approval-write-probe-arm", &cut)
                .await?
        );
        Ok(())
    }

    async fn trace(&self) -> Result<ApprovalWriteTrace> {
        Ok(self
            .0
            .runtime()
            .control::<Option<ApprovalWriteTrace>>("approval-write-probe-trace", &())
            .await?
            .ok_or("no applying loan took the write probe")?)
    }

    async fn prepare_notice_recovery(&self) -> Result<()> {
        self.0.arm_notice_recovery().await
    }

    async fn finish_notice_recovery(&self) -> Result<()> {
        self.0.await_notice_recovery().await
    }

    async fn ending_recovery_count(&self) -> Result<u64> {
        Ok(self
            .0
            .runtime()
            .control("mac-delivery-count", &"macEndingRecoveryFinished")
            .await?)
    }

    async fn wait_ending_recovery(&self, minimum: u64) -> Result<()> {
        if !self
            .0
            .runtime()
            .control::<bool>(
                "until-mac-delivery-count",
                &("macEndingRecoveryFinished", minimum),
            )
            .await?
        {
            return Err("ending recovery barrier did not complete".into());
        }
        Ok(())
    }

    async fn scheduled_delivery_count(&self) -> Result<u64> {
        Ok(self
            .0
            .runtime()
            .control("approval-delivery-scheduled-count", &())
            .await?)
    }

    async fn drive_due(&self) -> Result<()> {
        let _completion = self.0.runtime().after_event().await?;
        Ok(())
    }

    async fn read(&self, statement: Statement) -> Result<ReadOutcome> {
        self.0.read(statement).await
    }

    async fn write(&self, statement: Statement) -> Result<()> {
        self.0.fixture_write(statement).await
    }

    async fn snapshot(&self, reopen: bool) -> Result<Vec<Vec<Vec<SqlValue>>>> {
        if reopen {
            self.0.host.c2_evict(&self.0.organization).await?;
        }
        approval_snapshot(async |statement| self.read(statement).await).await
    }

    async fn close(self) -> Result<()> {
        self.0.close().await
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
