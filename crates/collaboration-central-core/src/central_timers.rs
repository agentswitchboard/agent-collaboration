//! Organization gating and startup re-publication; the engine owns due work.
use crate::WorkspaceRuntime;
use central_engine_model::{DueBatchPolicy, DueRow, EngineTime, InstanceKey};
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
            self.dispatch_due_wake(&FleetEngine::new(self), row).await;
        }
        Ok(())
    }

    /// Recover due accepted wakes on a cold open that is not itself an alarm drive.
    pub async fn recover_cold_alarm(&self) -> Result<(), StoreFailure> {
        if !self.alarm_organization_is_provisioned().await? {
            return self.publish_alarm_plan(EngineAlarmPlan::Clear).await;
        }
        let sample = self.execution.now()?;
        let cap = DueBatchPolicy::DEFAULT.per_kind_cap;
        let mut rows = self.overdue_wake_rows(sample, cap + 1).await?;
        if !rows.is_empty()
            && let Some(claim) = self
                .execution
                .due_batches()
                .begin_batch(
                    || self.execution.now(),
                    DueBatchPolicy::DEFAULT.spacing,
                    fleet_runtime::DueBatchTrigger::Request,
                )
                .await?
        {
            if claim.started_at() != sample {
                rows = self.overdue_wake_rows(claim.started_at(), cap + 1).await?;
            }
            claim.record_selection(rows.len() > cap);
            let engine = FleetEngine::new(self);
            for row in rows.into_iter().take(cap) {
                if self.dispatch_due_wake(&engine, row).await.1 {
                    break;
                }
            }
        }
        self.publish_alarm_plan(EngineAlarmPlan::Recompute).await
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
