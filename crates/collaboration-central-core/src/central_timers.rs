//! Organization gating and startup re-publication; the engine owns due work.
use crate::WorkspaceRuntime;
use central_storage::{EngineAlarmPlan, StoreFailure};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub(crate) async fn alarm_organization_is_provisioned(&self) -> Result<bool, StoreFailure> {
        match crate::prepare_central_alarm(self.execution.statement_host()).await? {
            crate::CentralOrganizationIdentity::Unprovisioned => Ok(false),
            crate::CentralOrganizationIdentity::Stamped { organization_id }
                if &organization_id == self.execution.organization() =>
            {
                Ok(true)
            }
            crate::CentralOrganizationIdentity::Stamped { .. } => {
                Err(corrupt("signin_organization_identity.id"))
            }
        }
    }
    pub async fn recompute_alarm(&self) -> Result<(), StoreFailure> {
        let plan = if self.alarm_organization_is_provisioned().await? {
            EngineAlarmPlan::Recompute
        } else {
            EngineAlarmPlan::Clear
        };
        fleet_engine::Engine::new(self)
            .publish_engine_alarm(plan)
            .await
            .map(|_| ())
            .map_err(|error| error.failure)
    }
}
