use super::{
    due_batch_execution::{DuePass, DuePassEnding},
    engine_host_binding::{EngineHost, FleetEngine},
};
use central_engine_model::{DueBatchPolicy, EngineTime, supported_time};
use central_storage::{AlarmPublicationFailure, EngineAlarmPlan, StoreFailure};
use fleet_contracts::{AlarmNotPublished, AlarmResult, EventCompletion};
use fleet_runtime_ports::CentralStore as _;

impl<Host: EngineHost> FleetEngine<'_, Host> {
    pub(crate) async fn complete_event_with_observation(
        &self,
    ) -> Result<EventCompletion, AlarmNotPublished> {
        #[cfg(feature = "test-support")]
        {
            self.0.record_engine_completion_entry();
            self.engine_test_pause_points()
                .reached_central(fleet_runtime::PausePoint::EngineCompletionEntered)
                .await;
        }
        let result = self.drain_due_work().await;
        if let Err(error) = &result {
            self.observe_alarm_not_published(error.failure.clone());
        }
        result
    }

    /// A reply owns a task only when indexed durable work may be eligible.
    pub async fn prepare_reply_due_work(&self) -> Option<fleet_runtime::DueBatchClaim> {
        #[cfg(feature = "test-support")]
        {
            self.0.record_engine_completion_entry();
            self.engine_test_pause_points()
                .reached_central(fleet_runtime::PausePoint::EngineCompletionEntered)
                .await;
        }
        match self.prepare_reply_claim().await {
            Ok(claim) => claim,
            Err(error) => {
                self.observe_alarm_not_published(error.failure);
                None
            }
        }
    }

    async fn prepare_reply_claim(
        &self,
    ) -> Result<Option<fleet_runtime::DueBatchClaim>, AlarmNotPublished> {
        let Some(mut claim) = self.due_batch_state().claim() else {
            return Ok(None);
        };
        if !self
            .0
            .organization_is_provisioned()
            .await
            .map_err(|failure| AlarmNotPublished { failure })?
        {
            drop(claim);
            self.publish_engine_alarm(EngineAlarmPlan::Clear).await?;
            return Ok(None);
        }
        for _ in 0..DueBatchPolicy::DEFAULT.max_batches_per_wake {
            claim.start_discovery();
            match self.due_discovery_window().await {
                Ok((_, _, false)) if claim.release_if_current() => break,
                Ok((_, _, false)) => {}
                Ok((now, _, true)) => {
                    // Preserve a durable fallback independently of the reply task.
                    let existing = self
                        .central_store()
                        .published_alarm()
                        .await
                        .map_err(|failure| AlarmNotPublished { failure })?;
                    if existing.is_none_or(|deadline| deadline <= now) {
                        self.publish_engine_alarm(EngineAlarmPlan::Recompute)
                            .await?;
                    }
                    return Ok(Some(claim));
                }
                Err(failure) => {
                    self.observe_due_read_failure(failure);
                    break;
                }
            }
        }
        drop(claim);
        self.publish_engine_alarm(EngineAlarmPlan::Recompute)
            .await?;
        Ok(None)
    }

    pub async fn complete_reply_due_work(&self, claim: fleet_runtime::DueBatchClaim) {
        let _ = self.run_claimed_due_work(claim).await;
        if let Err(error) = self.publish_engine_alarm(EngineAlarmPlan::Recompute).await {
            self.observe_alarm_not_published(error.failure);
        }
    }

    async fn drain_due_work(&self) -> Result<EventCompletion, AlarmNotPublished> {
        let claim = self.due_batch_state().claim();
        let provisioned = self
            .0
            .organization_is_provisioned()
            .await
            .map_err(|failure| AlarmNotPublished { failure })?;
        let work = match (provisioned, claim) {
            (true, Some(claim)) => self.run_claimed_due_work(claim).await,
            (true, None) | (false, _) => AlarmResult::default(),
        };
        let plan = if provisioned {
            EngineAlarmPlan::Recompute
        } else {
            EngineAlarmPlan::Clear
        };
        let publication = self.publish_engine_alarm(plan).await?;
        Ok(EventCompletion { work, publication })
    }

    async fn run_claimed_due_work(&self, mut claim: fleet_runtime::DueBatchClaim) -> AlarmResult {
        let mut work = AlarmResult::default();
        for batch_number in 1..=DueBatchPolicy::DEFAULT.max_batches_per_wake {
            if batch_number > 1 {
                if self.0.yield_due_batches().await.is_err() {
                    break;
                }
                #[cfg(feature = "test-support")]
                self.engine_test_pause_points()
                    .reached_central(fleet_runtime::PausePoint::BeforeDueContinuation)
                    .await;
            }
            claim.start_discovery();
            let pass = self.discover_due_pass().await;
            work += pass.work;
            match pass.ending {
                DuePassEnding::CannotProgress => break,
                DuePassEnding::SelectedTail => {}
                DuePassEnding::Drained if claim.release_if_current() => break,
                DuePassEnding::Drained => {}
            }
        }
        work
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
                ending: DuePassEnding::Drained,
            },
            Err(failure) => {
                self.observe_due_read_failure(failure);
                DuePass {
                    work: AlarmResult::default(),
                    ending: DuePassEnding::CannotProgress,
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
