//! One org turn owns machine-login reads, applying time and the guarded batch.
use crate::WorkspaceRuntime;
use crate::organization_authority_cache::StampStanding;
use central_storage::{AlarmUpdate, BatchOutcome, StatementBatch, StoreFailure};
use fleet_contracts::{
    DecideDeviceLogin, DecideMachineToken, DeviceLoginDecision, MachineTokenDecision,
};
use fleet_machine_login::{MachineLoginPlan, MachineLoginService, policy::DecisionTime};
use fleet_runtime_ports::SyncTime;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};

impl<H: CentralStore, S: WorkspaceStores> WorkspaceRuntime<H, S> {
    pub async fn decide_device_login(
        &self,
        request: &DecideDeviceLogin,
    ) -> Result<DeviceLoginDecision, StoreFailure> {
        let received_at_ms = self.execution.clock().now().unix_millis();
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeSignInSample)
            .await;
        let _turn = self.execution.take_central_turn().await;
        let time = self.machine_login_time(&request.organization_id).await?;
        let plan = self
            .machine_login_service()
            .prepare_device_login(request, received_at_ms, time)
            .await?;
        self.apply_machine_login(plan).await
    }

    pub async fn decide_machine_token(
        &self,
        request: &DecideMachineToken,
    ) -> Result<MachineTokenDecision, StoreFailure> {
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::BeforeSignInSample)
            .await;
        let _turn = self.execution.take_central_turn().await;
        let time = self.machine_login_time(&request.organization_id).await?;
        let plan = self
            .machine_login_service()
            .prepare_machine_token(request, time)
            .await?;
        self.apply_machine_login(plan).await
    }

    pub(crate) async fn machine_login_time(
        &self,
        organization: &fleet_identity_protocol::OrganizationId,
    ) -> Result<DecisionTime, StoreFailure> {
        if organization != self.execution.organization() {
            return Err(StoreFailure::Unavailable);
        }
        self.prepare_signin_authority().await?;
        let time = DecisionTime {
            now_ms: self.execution.clock().now().unix_millis(),
        };
        if self
            .signin_authority
            .standing(SyncTime::from_unix_millis(time.now_ms))
            != StampStanding::Valid
            || self
                .signin_authority
                .check()
                .and_then(|check| check.sign_in_service())
                .is_none()
        {
            return Err(StoreFailure::Unavailable);
        }
        Ok(time)
    }

    pub(crate) fn machine_login_service(&self) -> MachineLoginService<'_, H> {
        MachineLoginService::for_organization(
            self.execution.statement_host(),
            self.execution.organization(),
            self.execution.ids(),
        )
    }

    pub(crate) async fn apply_machine_login<Answer>(
        &self,
        plan: MachineLoginPlan<Answer>,
    ) -> Result<Answer, StoreFailure> {
        let MachineLoginPlan { answer, statements } = plan;
        #[cfg(feature = "test-support")]
        self.execution
            .pause_points()
            .reached_central(fleet_runtime::PausePoint::AfterSignInSample)
            .await;
        if !statements.is_empty() {
            match self
                .execution
                .statement_host()
                .run_batch(StatementBatch {
                    statements,
                    alarm: AlarmUpdate::Unchanged,
                })
                .await?
            {
                BatchOutcome::Applied { .. } => {}
                _ => return Err(StoreFailure::Unavailable),
            }
        }
        Ok(answer)
    }
}
