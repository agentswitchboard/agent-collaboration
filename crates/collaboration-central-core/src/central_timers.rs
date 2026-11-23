//! Organization gating and startup re-publication; the engine owns due work.
use crate::WorkspaceRuntime;
use central_engine_model::{DueRow, EngineTime, InstanceKey};
use central_storage::{AlarmPublicationFailure, EngineAlarmPlan, StoreFailure};
use fleet_engine::{DueDiscoveryError, FleetEngine};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use fleet_wakes::{WakeStore, WakeTimer};

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub(crate) async fn alarm_organization_is_provisioned(&self) -> Result<bool, StoreFailure> {
        match crate::prepare_central_alarm(
            self.execution.statement_host(),
            self.stamp_store.as_ref(),
        )
        .await?
        {
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
    pub(crate) async fn dispatch_due_wake(
        &self,
        engine: &FleetEngine<'_, Self>,
        row: DueRow,
    ) -> (bool, bool) {
        engine
            .run_due_machine_instance::<WakeTimer, _>(
                &WakeStore::new(self, fleet_mail::insert_message),
                row,
                "automation_wakes.due_at",
                |occurrence| fleet_wakes::Event::Due { occurrence },
            )
            .await
    }

    pub(crate) async fn fire_committed_overdue_wake(
        &self,
        wake_id: &workspace_ids::WakeId,
        due_at: EngineTime,
    ) -> Result<(), StoreFailure> {
        if self.execution.now()? < due_at || !self.alarm_organization_is_provisioned().await? {
            return Ok(());
        }
        if self
            .execution
            .due_batches()
            .admit_overdue_wake(due_at, || self.execution.now())
            .await?
        {
            let row = DueRow {
                key: InstanceKey(wake_id.to_string()),
                occurrence_ms: due_at.timestamp_millis(),
                effective_due_ms: due_at.timestamp_millis(),
            };
            #[cfg(feature = "test-support")]
            self.record_overdue_wake_attempt(self.execution.now()?, &row);
            self.dispatch_due_wake(&FleetEngine::new(self), row).await;
        }
        Ok(())
    }

    /// Recover due accepted wakes on a cold open that is not itself an alarm drive.
    pub async fn recover_cold_alarm(&self) -> Result<(), StoreFailure> {
        if !self.alarm_organization_is_provisioned().await? {
            return self.publish_alarm_plan(EngineAlarmPlan::Clear).await;
        }
        let now = self.execution.now()?;
        if self.overdue_wake_rows(now, 1).await?.is_empty() {
            return self.publish_alarm_plan(EngineAlarmPlan::Recompute).await;
        }
        // A startup event that needs to take due work uses the same full claim
        // as any other event. It takes responsibility for the retained mark.
        self.after_event()
            .await
            .map(|_| ())
            .map_err(|error| error.failure)
    }

    #[cfg(feature = "test-support")]
    fn record_overdue_wake_attempt(&self, started_at: EngineTime, row: &DueRow) {
        self.execution
            .due_batches()
            .record_unit_start(fleet_runtime::DueUnitStart {
                batch_start_unix_millis: started_at.timestamp_millis(),
                machine_kind: "wake_timer".to_owned(),
                subject_key: row.key.0.clone(),
            });
    }

    async fn overdue_wake_rows(
        &self,
        cutoff: EngineTime,
        limit: usize,
    ) -> Result<Vec<DueRow>, StoreFailure> {
        fleet_wakes::due_wakes(self.execution.statement_host(), cutoff, limit)
            .await
            .map_err(|error| match error {
                DueDiscoveryError::ReadFailed(failure) => failure,
                DueDiscoveryError::InvalidBounds(_) => StoreFailure::AlarmUnpublishable {
                    reason: AlarmPublicationFailure::NowOutOfRange,
                },
            })
    }

    pub async fn recompute_alarm(&self) -> Result<(), StoreFailure> {
        let plan = if self.alarm_organization_is_provisioned().await? {
            EngineAlarmPlan::Recompute
        } else {
            EngineAlarmPlan::Clear
        };
        self.publish_alarm_plan(plan).await
    }

    async fn publish_alarm_plan(&self, plan: EngineAlarmPlan) -> Result<(), StoreFailure> {
        FleetEngine::new(self)
            .publish_engine_alarm(plan)
            .await
            .map(|_| ())
            .map_err(|error| error.failure)
    }
}
