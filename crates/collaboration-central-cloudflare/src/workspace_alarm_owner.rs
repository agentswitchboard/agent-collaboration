//! Own the Workerd alarm callback and its organization recovery.
use super::{CentralObject, OrgRuntime};
#[cfg(feature = "test-support")]
use super::{signin_launcher_controls, workspace_cases};
use central_storage::StoreFailure;
use fleet_identity_protocol::OrganizationId;
use std::sync::Arc;
use worker::{Response, Result};

pub(super) enum AlarmFailure {
    Store(StoreFailure),
    Platform(worker::Error),
}

impl CentralObject {
    pub(super) async fn alarm_runtime(
        &self,
    ) -> std::result::Result<Option<(OrganizationId, Arc<OrgRuntime>)>, AlarmFailure> {
        self.migrate().await.map_err(AlarmFailure::Store)?;
        let host = self.statement_host();
        let identity =
            collaboration_central_core::prepare_central_alarm(host.as_ref(), host.as_ref())
                .await
                .map_err(AlarmFailure::Store)?;
        let collaboration_central_core::CentralOrganizationIdentity::Stamped { organization_id } =
            identity
        else {
            return Ok(None);
        };
        self.check_physical_organization(&organization_id)
            .map_err(AlarmFailure::Platform)?;
        #[cfg(feature = "test-support")]
        signin_launcher_controls::sample_clock(self, &organization_id)
            .await
            .map_err(AlarmFailure::Platform)?;
        let runtime = self
            .runtime_checked(organization_id.clone())
            .await
            .map_err(AlarmFailure::Store)?;
        Ok(Some((organization_id, runtime)))
    }

    pub(super) async fn drive_workspace_alarm(&self) -> Result<Response> {
        #[cfg(feature = "test-support")]
        return workspace_cases::alarm_with_receipt(self).await;
        #[cfg(not(feature = "test-support"))]
        {
            let runtime = match self.alarm_runtime().await {
                Ok(Some((_, runtime))) => runtime,
                Ok(None) => return Response::ok("unprovisioned"),
                Err(AlarmFailure::Store(error)) => {
                    return Err(worker::Error::RustError(error.to_string()));
                }
                Err(AlarmFailure::Platform(error)) => return Err(error),
            };
            let result = runtime.after_alarm().await;
            self.flush_ready_mail(&runtime).await;
            result.map_err(|error| worker::Error::RustError(error.to_string()))?;
            Response::ok("alarm applied")
        }
    }
}
