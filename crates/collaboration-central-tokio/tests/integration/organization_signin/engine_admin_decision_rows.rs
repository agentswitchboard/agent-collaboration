//! Route-by-route decision cases through the native private Sign-in listener.

use super::{engine_admin_support::EngineOrg, support::Result};
use central_storage::SqlValue;
use collaboration_central_conformance::{
    engine_admin_decision_rows, engine_admin_shared_decision_rows,
    signin_fixtures::engine_admin_decisions::{
        AdminBatchLoss, ApplyingControl, EngineAdminCase, EngineAdminHarness, EngineAdminRequest,
        EngineAdminRoute,
        boundary_decisions::{BoundaryCase, check_boundary_case},
        check_engine_admin_case,
    },
};
use fleet_runtime::PausePoint;

impl EngineOrg {
    async fn send_engine_admin(&self, request: &EngineAdminRequest) -> Result<(u16, Vec<u8>)> {
        let mut call = self
            .f
            .client()
            .request(
                request.method.clone(),
                format!("http://central.local{}", request.path),
            )
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&request.body)?);
        for (name, value) in &request.headers {
            call = call.header(*name, value);
        }
        let response = call.send().await?;
        Ok((response.status().as_u16(), response.bytes().await?.to_vec()))
    }
}

impl EngineAdminHarness for EngineOrg {
    async fn send_admin(
        &self,
        request: &EngineAdminRequest,
        control: ApplyingControl,
    ) -> Result<(u16, Vec<u8>)> {
        let pause = match control {
            ApplyingControl::Immediate => return self.send_engine_admin(request).await,
            ApplyingControl::AdvanceAfterVerification => PausePoint::BeforeSignInSample,
            ApplyingControl::MoveFloorAfterSampling
            | ApplyingControl::MoveAttemptAfterSampling { .. } => PausePoint::AfterSignInSample,
        };
        let runtime = self.runtime().await;
        let mut held = runtime.runtime().pause_points().hold_central(pause);
        let send = self.send_engine_admin(request);
        let release = async {
            held.until_held().await;
            match control {
                ApplyingControl::Immediate => {}
                ApplyingControl::AdvanceAfterVerification => {
                    self.set(self.now() + chrono::Duration::seconds(1)).await;
                }
                ApplyingControl::MoveAttemptAfterSampling { attempt_id } => {
                    self.raw(
                        "DELETE FROM engine_work_attempts WHERE attempt_id=?1",
                        vec![SqlValue::Text(attempt_id.into())],
                    )
                    .await;
                }
                ApplyingControl::MoveFloorAfterSampling => {
                    self.raw(
                        "UPDATE signin_authorization_floors SET minimum_revision=4",
                        vec![],
                    )
                    .await;
                }
            }
            held.release();
        };
        tokio::pin!(send);
        tokio::select! {
            early = &mut send => Err(format!("request completed before the expected hold: {}", early?.0).into()),
            () = release => send.await,
        }
    }

    async fn read_admin_rows(
        &self,
        sql: &'static str,
        args: Vec<SqlValue>,
    ) -> Result<Vec<Vec<SqlValue>>> {
        Ok(self.rows(sql, args).await)
    }

    async fn apply_admin_fixture(&self, sql: &'static str, args: Vec<SqlValue>) -> Result {
        self.raw(sql, args).await;
        Ok(())
    }

    async fn create_fixture_wake(&self, due_at: chrono::DateTime<chrono::Utc>) -> Result<String> {
        Ok(self.wake(due_at, "engine admin test wake").await)
    }

    async fn arm_admin_batch_loss(&self, proof_id: &str, loss: AdminBatchLoss) -> Result {
        let kind = match loss {
            AdminBatchLoss::BeforeApply => {
                fleet_runtime::EngineCommitFaultKind::NotAppliedAnswerLost
            }
            AdminBatchLoss::AfterCommit => fleet_runtime::EngineCommitFaultKind::AppliedAnswerLost,
        };
        let key = serde_json::from_value(serde_json::json!(proof_id))?;
        let fault = fleet_runtime::EngineCommitFault { key, kind };
        self.runtime()
            .await
            .runtime()
            .statement_host()
            .inject_engine_commit_fault(fault);
        Ok(())
    }

    fn assert_oversized_reply(&self, bytes: &[u8]) {
        assert_eq!(
            bytes,
            b"Failed to buffer the request body: length limit exceeded"
        );
    }

    async fn clear_fixture_alarm(&self) -> Result {
        let batch = central_storage::StatementBatch {
            statements: vec![],
            alarm: central_storage::AlarmUpdate::Engine(central_storage::EngineAlarmPlan::Clear),
        };
        self.runtime()
            .await
            .runtime()
            .statement_host()
            .run_batch(batch)
            .await?;
        Ok(())
    }

    async fn published_alarm_millis(&self) -> Option<i64> {
        self.alarm_ms().await
    }
}

macro_rules! declare_case {
    ($route:ident, $case_name:ident, $case:ident) => {
        #[tokio::test]
        async fn $case_name() -> Result {
            let fixture = EngineOrg::new().await;
            let outcome =
                check_engine_admin_case(&fixture, EngineAdminRoute::$route, EngineAdminCase::$case)
                    .await;
            fixture.close().await;
            outcome
        }
    };
}

macro_rules! declare_route {
    ($module:ident, $route:ident, [$($case_name:ident => $case:ident,)*]) => {
        mod $module {
            use super::*;
            $(declare_case!($route, $case_name, $case);)*
            engine_admin_shared_decision_rows!(declare_case, $route);
            collaboration_central_conformance::engine_admin_boundary_rows!(declare_boundary, $route);
        }
    };
}

macro_rules! declare_boundary {
    ($route:ident, $case_name:ident, $case:ident) => {
        #[tokio::test]
        async fn $case_name() -> Result {
            let fixture = EngineOrg::new().await;
            let outcome =
                check_boundary_case(&fixture, EngineAdminRoute::$route, BoundaryCase::$case).await;
            fixture.close().await;
            outcome
        }
    };
}

engine_admin_decision_rows!(declare_route);
