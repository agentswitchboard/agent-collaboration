//! Own the Workerd alarm callback and its organization recovery.
use super::{CentralObject, ColdAlarmStart, OrgRuntime};
#[cfg(feature = "test-support")]
use super::{signin_launcher_controls, workspace_cases};
use central_engine_model::{AlarmDecisionError, EngineTime, next_five_minute_mark};
use central_storage::StoreFailure;
use fleet_contracts::EventCompletion;
use fleet_identity_protocol::OrganizationId;
use fleet_runtime_ports::Clock;
use std::sync::Arc;
use worker::{Response, Result};

#[derive(Debug, thiserror::Error)]
pub(super) enum AlarmFailure {
    #[error(transparent)]
    Store(StoreFailure),
    #[error(transparent)]
    Platform(worker::Error),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AlarmRearmFailure {
    #[error(transparent)]
    Mark(AlarmDecisionError),
    #[error(transparent)]
    Storage(worker::Error),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AlarmDriveFailure {
    #[error("alarm drive failed: {drive}. Retry armed for {mark}")]
    Rearmed {
        drive: StoreFailure,
        mark: EngineTime,
    },
    #[error("alarm drive failed: {drive}. Alarm re-arm failed: {rearm}")]
    RearmFailed {
        drive: StoreFailure,
        rearm: AlarmRearmFailure,
    },
    #[error("alarm drive failed: {drive}")]
    Unrecoverable { drive: AlarmFailure },
}

impl AlarmDriveFailure {
    #[cfg(feature = "test-support")]
    pub(super) fn store_failure(&self) -> Option<StoreFailure> {
        match self {
            Self::Rearmed { drive, .. }
            | Self::RearmFailed { drive, .. }
            | Self::Unrecoverable {
                drive: AlarmFailure::Store(drive),
            } => Some(drive.clone()),
            Self::Unrecoverable {
                drive: AlarmFailure::Platform(_),
            } => None,
        }
    }
}

pub(super) struct AlarmDrive {
    #[cfg(feature = "test-support")]
    pub(super) organization: Option<OrganizationId>,
    pub(super) completion: std::result::Result<Option<EventCompletion>, AlarmDriveFailure>,
}

impl AlarmDrive {
    pub(super) fn into_response(self) -> Result<Response> {
        match self.completion {
            Ok(Some(_)) => Response::ok("alarm applied"),
            Ok(None) => Response::ok("unprovisioned"),
            // Workerd must acknowledge a failed drive once a grid retry is armed.
            // Returning an error here would also request an off-grid platform retry.
            Err(AlarmDriveFailure::Rearmed { .. }) => Response::ok("alarm rearmed"),
            Err(AlarmDriveFailure::Unrecoverable {
                drive: AlarmFailure::Platform(error),
            }) => Err(error),
            Err(error) => Err(worker::Error::RustError(error.to_string())),
        }
    }
}

impl CentralObject {
    async fn alarm_runtime(
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
            .runtime_checked(organization_id.clone(), ColdAlarmStart::DriveFollows)
            .await
            .map_err(AlarmFailure::Store)?;
        Ok(Some((organization_id, runtime)))
    }

    pub(super) async fn drive_workspace_alarm(&self) -> Result<Response> {
        #[cfg(feature = "test-support")]
        return workspace_cases::alarm_with_receipt(self).await;
        #[cfg(not(feature = "test-support"))]
        self.run_workspace_alarm().await.into_response()
    }

    pub(super) async fn run_workspace_alarm(&self) -> AlarmDrive {
        #[cfg(feature = "test-support")]
        let mut organization = None;
        let completion = async {
            let Some((_organization, runtime)) = self.alarm_runtime().await? else {
                return Ok(None);
            };
            #[cfg(feature = "test-support")]
            {
                organization = Some(_organization);
            }
            let completion = runtime.after_alarm().await;
            self.flush_ready_mail(&runtime).await;
            completion
                .map(Some)
                .map_err(|error| AlarmFailure::Store(error.failure))
        }
        .await;
        let completion = match completion {
            Ok(completion) => Ok(completion),
            Err(AlarmFailure::Store(
                drive @ (StoreFailure::Unavailable
                | StoreFailure::AnswerLost
                | StoreFailure::AlarmUnpublishable { .. }),
            )) => {
                let failure = match self.rearm_failed_alarm().await {
                    Ok(mark) => AlarmDriveFailure::Rearmed { drive, mark },
                    Err(rearm) => AlarmDriveFailure::RearmFailed { drive, rearm },
                };
                worker::console_warn!("{failure}");
                Err(failure)
            }
            Err(drive) => Err(AlarmDriveFailure::Unrecoverable { drive }),
        };
        AlarmDrive {
            #[cfg(feature = "test-support")]
            organization,
            completion,
        }
    }

    async fn rearm_failed_alarm(&self) -> std::result::Result<EngineTime, AlarmRearmFailure> {
        let storage = self.state.storage();
        let retained = storage
            .get_alarm()
            .await
            .map_err(AlarmRearmFailure::Storage)?;
        let now = EngineTime::from_timestamp_millis(self.clock.now().unix_millis())
            .ok_or(AlarmRearmFailure::Mark(AlarmDecisionError::OutOfRange))?;
        let mark = next_five_minute_mark(now).map_err(AlarmRearmFailure::Mark)?;
        // An executing alarm is absent here. An earlier valid mark may have
        // been published by another event while this callback was in flight.
        if let Some(retained) = retained.and_then(EngineTime::from_timestamp_millis)
            && central_engine_model::supported_time(retained)
            && central_engine_model::on_five_minute_grid(retained)
            && retained <= mark
        {
            return Ok(retained);
        }
        storage
            .set_alarm(mark)
            .await
            .map_err(AlarmRearmFailure::Storage)?;
        Ok(mark)
    }
}
