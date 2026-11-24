use super::{
    due_batch_execution::{DuePass, DuePassEnding},
    engine_host_binding::{EngineHost, FleetEngine},
};
use central_engine_model::{
    DueBatchPolicy, DueDrainEffect, DueDrainEvent, DueDrainWakeId, EngineTime, supported_time,
};
use central_storage::{AlarmPublicationFailure, EngineAlarmPlan, StoreFailure};
use fleet_contracts::{AlarmNotPublished, AlarmResult, EventCompletion};
use fleet_runtime::{AlarmDrainAdmission, DueBatchClaim};
use fleet_runtime_ports::CentralStore as _;

impl<Host: EngineHost> FleetEngine<'_, Host> {
    pub(crate) async fn complete_event_with_observation(
        &self,
    ) -> Result<EventCompletion, AlarmNotPublished> {
        self.record_completion_entry().await;
        let result = self.drain_inline().await;
        self.observe_completion_result(&result);
        result
    }

    /// Arm a durable fallback before the reply can earn its deferred task.
    pub async fn prepare_reply_due_work(&self) -> Option<DueDrainWakeId> {
        self.record_completion_entry().await;
        match self.prepare_reply_fallback().await {
            Ok(true) => Some(self.new_wake_id()),
            Ok(false) => None,
            Err(error) => {
                self.observe_alarm_not_published(error.failure);
                None
            }
        }
    }

    #[cfg(feature = "test-support")]
    async fn record_completion_entry(&self) {
        self.0.record_engine_completion_entry();
        self.engine_test_pause_points()
            .reached_central(fleet_runtime::PausePoint::EngineCompletionEntered)
            .await;
    }

    #[cfg(not(feature = "test-support"))]
    fn record_completion_entry(&self) -> std::future::Ready<()> {
        std::future::ready(())
    }

    fn new_wake_id(&self) -> DueDrainWakeId {
        DueDrainWakeId(self.host_id_source().new_id())
    }

    async fn prepare_reply_fallback(&self) -> Result<bool, AlarmNotPublished> {
        if !self
            .0
            .organization_is_provisioned()
            .await
            .map_err(|failure| AlarmNotPublished { failure })?
        {
            self.publish_engine_alarm(EngineAlarmPlan::Clear).await?;
            return Ok(false);
        }
        match self.due_discovery_window().await {
            Ok((now, _, true)) => {
                let existing = self
                    .central_store()
                    .published_alarm()
                    .await
                    .map_err(|failure| AlarmNotPublished { failure })?;
                if existing.is_none_or(|deadline| deadline <= now) {
                    self.publish_engine_alarm(EngineAlarmPlan::Recompute)
                        .await?;
                }
                Ok(true)
            }
            Ok((_, _, false)) => {
                self.publish_engine_alarm(EngineAlarmPlan::Recompute)
                    .await?;
                Ok(false)
            }
            Err(failure) => {
                self.observe_due_read_failure(failure);
                self.publish_engine_alarm(EngineAlarmPlan::Recompute)
                    .await?;
                Ok(false)
            }
        }
    }

    pub async fn complete_reply_due_work(&self, claim: DueBatchClaim) {
        if let Some(first_step) = claim.transition(DueDrainEvent::HandoffStarted) {
            let result = self.run_due_batch_claim(&claim, first_step).await;
            self.observe_completion_result(&result);
        }
    }

    fn observe_completion_result(&self, result: &Result<EventCompletion, AlarmNotPublished>) {
        if let Err(error) = result {
            self.observe_alarm_not_published(error.failure.clone());
        }
    }

    async fn drain_inline(&self) -> Result<EventCompletion, AlarmNotPublished> {
        match self.due_batch_state().alarm_fired(self.new_wake_id()) {
            Some(AlarmDrainAdmission::RunPass { claim, pass }) => {
                let first_step = DueDrainEffect::RunPass {
                    wake_id: claim.wake_id(),
                    pass,
                };
                self.run_due_batch_claim(&claim, first_step).await
            }
            Some(AlarmDrainAdmission::PublishWhileOwned) | None => {
                let provisioned = self
                    .0
                    .organization_is_provisioned()
                    .await
                    .map_err(|failure| AlarmNotPublished { failure })?;
                self.publish_drain_completion(AlarmResult::default(), provisioned)
                    .await
            }
        }
    }

    async fn run_due_batch_claim(
        &self,
        claim: &DueBatchClaim,
        first_step: DueDrainEffect,
    ) -> Result<EventCompletion, AlarmNotPublished> {
        let mut step = Some(first_step);
        let mut work = AlarmResult::default();
        let mut provisioned = true;
        while let Some(effect) = step {
            step = match effect {
                DueDrainEffect::RunPass { .. } => {
                    #[cfg(feature = "test-support")]
                    self.due_batch_state().record_discovery_start();
                    // The owner can change after the reply, and again during a
                    // yield. Both the alarm and deferred paths verify it here.
                    match self.0.organization_is_provisioned().await {
                        Ok(true) => {
                            let pass = self.discover_due_pass().await;
                            work += pass.work;
                            #[cfg(feature = "test-support")]
                            self.engine_test_pause_points().pass_finished();
                            claim.transition(match pass.ending {
                                DuePassEnding::FullWithProgress => {
                                    DueDrainEvent::FullPassMadeProgress
                                }
                                DuePassEnding::FullWithoutProgress => {
                                    DueDrainEvent::FullPassMadeNoProgress
                                }
                                DuePassEnding::Short => DueDrainEvent::ShortPassFinished,
                                DuePassEnding::StoreFault => DueDrainEvent::DrainFaulted,
                            })
                        }
                        Ok(false) => {
                            provisioned = false;
                            claim.transition(DueDrainEvent::DrainFaulted)
                        }
                        Err(failure) => {
                            claim.transition(DueDrainEvent::DrainFaulted);
                            claim.transition(DueDrainEvent::AlarmPublicationSettled);
                            return Err(AlarmNotPublished { failure });
                        }
                    }
                }
                DueDrainEffect::YieldToHost(_) => {
                    if self.0.yield_due_batches().await.is_err() {
                        claim.transition(DueDrainEvent::HostYieldFailed)
                    } else {
                        #[cfg(feature = "test-support")]
                        self.engine_test_pause_points()
                            .reached_central(fleet_runtime::PausePoint::BeforeDueContinuation)
                            .await;
                        claim.transition(DueDrainEvent::HostYielded)
                    }
                }
                DueDrainEffect::PublishDurableAlarm(_) => {
                    #[cfg(feature = "test-support")]
                    self.engine_test_pause_points()
                        .reached_central(fleet_runtime::PausePoint::BeforeDueAlarmPublication)
                        .await;
                    let result = self.publish_drain_completion(work, provisioned).await;
                    claim.transition(DueDrainEvent::AlarmPublicationSettled);
                    return result;
                }
                DueDrainEffect::ScheduleHandoff(_)
                | DueDrainEffect::TransferHandoff { .. }
                | DueDrainEffect::ReleaseClaim(_) => None,
            };
        }
        Err(AlarmNotPublished {
            failure: fleet_runtime_ports::workspace_decode_error::corrupt("engine.due_drain"),
        })
    }

    async fn publish_drain_completion(
        &self,
        work: AlarmResult,
        provisioned: bool,
    ) -> Result<EventCompletion, AlarmNotPublished> {
        let plan = if provisioned {
            EngineAlarmPlan::Recompute
        } else {
            EngineAlarmPlan::Clear
        };
        let publication = self.publish_engine_alarm(plan).await?;
        Ok(EventCompletion { work, publication })
    }

    async fn due_discovery_window(&self) -> Result<(EngineTime, EngineTime, bool), StoreFailure> {
        let now = self.current_engine_time()?;
        let horizon = now
            .checked_add_signed(DueBatchPolicy::DEFAULT.look_ahead)
            .filter(|time| supported_time(*time))
            .ok_or(StoreFailure::AlarmUnpublishable {
                reason: AlarmPublicationFailure::NowOutOfRange,
            })?;
        self.due_candidates_exist(now, horizon)
            .await
            .map(|found| (now, horizon, found))
    }

    fn observe_due_read_failure(&self, failure: StoreFailure) {
        self.observe_engine_store_down(
            &fleet_contracts::EngineSubject {
                kind: central_engine_model::MachineKind("engine_source"),
                key: central_engine_model::InstanceKey("due_candidates".to_owned()),
            },
            failure,
        );
    }

    async fn discover_due_pass(&self) -> DuePass {
        match self.due_discovery_window().await {
            Ok((now, horizon, true)) => self.run_due_batch(now, horizon).await,
            Ok((_, _, false)) => DuePass {
                work: AlarmResult::default(),
                ending: DuePassEnding::Short,
            },
            Err(failure) => {
                self.observe_due_read_failure(failure);
                DuePass {
                    work: AlarmResult::default(),
                    ending: DuePassEnding::StoreFault,
                }
            }
        }
    }

    async fn due_candidates_exist(
        &self,
        now: EngineTime,
        horizon: EngineTime,
    ) -> Result<bool, StoreFailure> {
        let indexed = self.central_store().engine_due_times().await?;
        if indexed
            .exact_due_ms
            .is_some_and(|due| due <= now.timestamp_millis())
            || indexed
                .look_ahead_due_ms
                .is_some_and(|due| due <= horizon.timestamp_millis())
        {
            return Ok(true);
        }
        let Some(approval_due) = indexed.approval_due_ms else {
            return Ok(false);
        };
        Ok(approval_due <= now.timestamp_millis()
            || approval_due <= self.0.exact_due_cutoff(now).await?.timestamp_millis())
    }
}
