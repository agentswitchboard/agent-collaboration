use super::checked_attempts::{CheckedDueFailure, CheckedDueUnit};
use super::{
    engine_host_binding::{EngineHost, FleetEngine},
    machine_event_execution::MachineEventOutcome,
    machine_store_adapter::classify_corrupt_subject_row,
    transition_batch_writes::BatchApplyFailure,
};
use central_engine_model::{
    DueBatchPolicy, DueListKind, DueRow, EngineTime, InstanceKey, MachineKind, RetirementThreshold,
};
use central_storage::{
    AlarmPublication, AlarmUpdate, BatchOutcome, EngineAlarmPlan, StatementBatch, StoreFailure,
};
use fleet_contracts::{
    AlarmNotPublished, AlarmResult, ControlGate, EngineSubject, EventCompletion, FailureOutcome,
    LegacyUnitOutcome,
};
use fleet_runtime_ports::{CentralStore, workspace_decode_error::corrupt};
use futures_util::{StreamExt, stream};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};

pub enum DueCutoff {
    BatchStart,
    LookAheadHorizon,
}
pub struct TimedSourceSchedule {
    pub machine_kind: MachineKind,
    pub due_cutoff_policy: DueCutoff,
}
pub enum DueDiscoveryError {
    InvalidBounds(central_engine_model::DueQueryError),
    ReadFailed(StoreFailure),
}

pub enum TimedRowOutcome {
    Handled(AlarmResult),
    CentralStoreFailed,
}
pub trait TimedDueDispatch<Host: EngineHost>: Send + Sync {
    type TimedSourceKind: Copy + Send + Sync + 'static;
    fn ordered_timed_source_kinds(&self) -> &'static [Self::TimedSourceKind];
    fn timed_source_schedule(&self, kind: Self::TimedSourceKind) -> TimedSourceSchedule;
    fn discover_due_rows(
        &self,
        host: &Host,
        kind: Self::TimedSourceKind,
        cutoff: EngineTime,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<DueRow>, DueDiscoveryError>> + Send;
    fn dispatch_timed_due_row(
        &self,
        engine: &FleetEngine<'_, Host>,
        host: &Host,
        kind: Self::TimedSourceKind,
        row: DueRow,
    ) -> impl Future<Output = TimedRowOutcome> + Send;
}
pub struct WholeDueUnitControl {
    pub attempt_subject: EngineSubject,
    pub retirement_threshold: RetirementThreshold,
}
pub enum WholeUnitOutcome {
    Skipped,
    Completed(AlarmResult),
}

enum DueBatchUnit<TimedKind, WholeKind> {
    TimedMachineRow(
        TimedKind,
        #[cfg_attr(
            not(feature = "test-support"),
            expect(
                dead_code,
                reason = "the source kind labels only the test-support visit receipt"
            )
        )]
        MachineKind,
        DueRow,
    ),
    Whole(WholeKind, WholeDueUnitControl),
}
#[cfg(feature = "test-support")]
impl<TimedKind, WholeKind> DueBatchUnit<TimedKind, WholeKind> {
    fn build_due_unit_start_receipt(&self, now: EngineTime) -> fleet_runtime::DueUnitStart {
        let (kind, key) = match self {
            Self::TimedMachineRow(_, kind, row) => (kind.0, row.key.0.clone()),
            Self::Whole(_, unit) => (
                unit.attempt_subject.kind.0,
                unit.attempt_subject.key.0.clone(),
            ),
        };
        fleet_runtime::DueUnitStart {
            batch_start_unix_millis: now.timestamp_millis(),
            machine_kind: kind.to_owned(),
            subject_key: key,
        }
    }
}

fn timed_source_attempt_subject(kind: MachineKind) -> EngineSubject {
    EngineSubject {
        kind: MachineKind("engine_source"),
        key: InstanceKey(kind.0.into()),
    }
}
pub async fn read_due_rows(
    host: &impl CentralStore,
    kind: DueListKind,
    cutoff: EngineTime,
    limit: usize,
) -> Result<Vec<DueRow>, DueDiscoveryError> {
    let query = central_engine_model::due_list_query(kind, cutoff, limit)
        .map_err(DueDiscoveryError::InvalidBounds)?;
    host.read_timed_due_rows(query)
        .await
        .map_err(DueDiscoveryError::ReadFailed)
}

impl<Host: EngineHost> FleetEngine<'_, Host> {
    /// Every event reaches this after its own work. The claim occurs before the
    /// first await; only individual units perform async work, at most eight wide.
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
        let result = self.complete_due_work_and_alarm().await;
        if let Err(error) = &result {
            self.observe_alarm_not_published(error.failure.clone());
        }
        result
    }

    async fn complete_due_work_and_alarm(&self) -> Result<EventCompletion, AlarmNotPublished> {
        let now = self
            .current_engine_time()
            .map_err(|failure| AlarmNotPublished { failure })?;
        let due_batch_claimed = self
            .due_batch_state()
            .try_claim(now.timestamp_millis(), DueBatchPolicy::DEFAULT.spacing)
            .map_err(|reason| AlarmNotPublished {
                failure: StoreFailure::AlarmUnpublishable { reason },
            })?;
        let provisioned = self
            .0
            .organization_is_provisioned()
            .await
            .map_err(|failure| AlarmNotPublished { failure })?;
        if !provisioned {
            let publication = self.publish_engine_alarm(EngineAlarmPlan::Clear).await?;
            return Ok(EventCompletion {
                work: AlarmResult::default(),
                publication,
            });
        }
        let mut due_work_result = AlarmResult::default();
        if due_batch_claimed {
            due_work_result = self.run_due_batch(now).await;
        }
        let publication = self
            .publish_engine_alarm(EngineAlarmPlan::Recompute)
            .await?;
        Ok(EventCompletion {
            work: due_work_result,
            publication,
        })
    }

    pub async fn publish_engine_alarm(
        &self,
        plan: EngineAlarmPlan,
    ) -> Result<AlarmPublication, AlarmNotPublished> {
        match self
            .central_store()
            .run_batch(StatementBatch {
                statements: vec![],
                alarm: AlarmUpdate::Engine(plan),
            })
            .await
        {
            Ok(BatchOutcome::Applied { publication, .. }) => match publication {
                AlarmPublication::Set { .. } | AlarmPublication::Cleared => Ok(publication),
                AlarmPublication::Failed { reason } => Err(AlarmNotPublished {
                    failure: StoreFailure::AlarmUnpublishable { reason },
                }),
                AlarmPublication::Unchanged => Err(AlarmNotPublished {
                    failure: corrupt("engine.alarm_publication"),
                }),
            },
            Ok(
                BatchOutcome::GuardFailed { .. }
                | BatchOutcome::ConstraintViolated { .. }
                | BatchOutcome::StatementRejected { .. },
            ) => Err(AlarmNotPublished {
                failure: corrupt("engine.alarm_batch"),
            }),
            Err(failure) => Err(AlarmNotPublished { failure }),
        }
    }

    async fn run_due_batch(&self, now: EngineTime) -> AlarmResult {
        let policy = DueBatchPolicy::DEFAULT;
        let Some(horizon) = now
            .checked_add_signed(policy.look_ahead)
            .filter(|time| central_engine_model::supported_time(*time))
        else {
            return AlarmResult::default();
        };
        let mut units = Vec::new();
        let mut more_timed_rows_due = false;
        let mut central_down = false;
        let timed_due_dispatch = self.0.timed_due_dispatch();
        for &kind in timed_due_dispatch.ordered_timed_source_kinds() {
            let timed_source_schedule = timed_due_dispatch.timed_source_schedule(kind);
            let subject = timed_source_attempt_subject(timed_source_schedule.machine_kind);
            let observed = match self
                .check_attempt_control_gate(&subject, RetirementThreshold::DEFAULT)
                .await
            {
                ControlGate::Run(observed) => observed,
                ControlGate::Skip | ControlGate::Uncertain => continue,
                ControlGate::CentralDown(failure) => {
                    self.observe_engine_store_down(&subject, failure);
                    central_down = true;
                    break;
                }
            };
            let cutoff = match timed_source_schedule.due_cutoff_policy {
                DueCutoff::BatchStart => now,
                DueCutoff::LookAheadHorizon => horizon,
            };
            match timed_due_dispatch
                .discover_due_rows(self.0, kind, cutoff, policy.per_kind_cap + 1)
                .await
            {
                Ok(mut rows) => {
                    more_timed_rows_due |= rows.len() > policy.per_kind_cap;
                    rows.truncate(policy.per_kind_cap);
                    units.extend(rows.into_iter().map(|row| {
                        DueBatchUnit::TimedMachineRow(kind, timed_source_schedule.machine_kind, row)
                    }));
                    match self
                        .clear_observed_attempt_control(&subject, &observed)
                        .await
                    {
                        Ok(())
                        | Err(
                            BatchApplyFailure::Superseded | BatchApplyFailure::StorageAnswerLost,
                        ) => {}
                        Err(
                            BatchApplyFailure::StorageFailed(_) | BatchApplyFailure::Rejected(_),
                        ) => {
                            central_down = true;
                            break;
                        }
                    }
                }
                Err(DueDiscoveryError::InvalidBounds(_)) => {
                    central_down = true;
                    break;
                }
                Err(DueDiscoveryError::ReadFailed(StoreFailure::AnswerLost)) => {}
                Err(DueDiscoveryError::ReadFailed(StoreFailure::Corrupt { reason })) => {
                    if matches!(
                        self.record_control_attempt_failure_for_workspaces(
                            &subject,
                            classify_corrupt_subject_row(reason),
                            RetirementThreshold::DEFAULT,
                            &[]
                        )
                        .await,
                        FailureOutcome::StoreDown(_)
                    ) {
                        central_down = true;
                        break;
                    }
                }
                Err(DueDiscoveryError::ReadFailed(
                    failure @ (StoreFailure::Unavailable | StoreFailure::AlarmUnpublishable { .. }),
                )) => {
                    self.observe_engine_store_down(&subject, failure);
                    central_down = true;
                    break;
                }
            }
        }
        let mut due_work_result = AlarmResult::default();
        if !central_down {
            units.extend(
                self.0
                    .ordered_whole_units()
                    .iter()
                    .map(|&kind| DueBatchUnit::Whole(kind, self.0.whole_unit(kind))),
            );
            let stop_after_store_failure = AtomicBool::new(false);
            let mut completed = stream::iter(units)
                .map(|unit| {
                    let stop_after_store_failure = &stop_after_store_failure;
                    async move {
                        if stop_after_store_failure.load(Ordering::Acquire) {
                            return AlarmResult::default();
                        }
                        #[cfg(feature = "test-support")]
                        self.due_batch_state()
                            .record_unit_start(unit.build_due_unit_start_receipt(now));
                        let (due_work_result, central_store_failed) =
                            self.dispatch_due_batch_unit(unit, now).await;
                        if central_store_failed {
                            stop_after_store_failure.store(true, Ordering::Release);
                        }
                        due_work_result
                    }
                })
                .buffer_unordered(8);
            while let Some(done) = completed.next().await {
                due_work_result += done;
            }
        }
        self.due_batch_state()
            .finish(now.timestamp_millis(), more_timed_rows_due);
        due_work_result
    }

    async fn dispatch_due_batch_unit(
        &self,
        unit: DueBatchUnit<
            <Host::TimedDueDispatch as TimedDueDispatch<Host>>::TimedSourceKind,
            Host::WholeUnitKind,
        >,
        now: EngineTime,
    ) -> (AlarmResult, bool) {
        match unit {
            DueBatchUnit::TimedMachineRow(kind, _, row) => {
                match self
                    .0
                    .timed_due_dispatch()
                    .dispatch_timed_due_row(self, self.0, kind, row)
                    .await
                {
                    TimedRowOutcome::Handled(work) => (work, false),
                    TimedRowOutcome::CentralStoreFailed => (AlarmResult::default(), true),
                }
            }
            DueBatchUnit::Whole(kind, unit) => {
                self.run_whole_unit_with_attempt_control(kind, unit, now)
                    .await
            }
        }
    }

    async fn run_whole_unit_with_attempt_control(
        &self,
        kind: Host::WholeUnitKind,
        unit: WholeDueUnitControl,
        now: EngineTime,
    ) -> (AlarmResult, bool) {
        let WholeDueUnitControl {
            attempt_subject: subject,
            retirement_threshold: threshold,
        } = unit;
        let empty = AlarmResult::default();
        let observed = match self.check_attempt_control_gate(&subject, threshold).await {
            ControlGate::Run(observed) => observed,
            ControlGate::Skip | ControlGate::Uncertain => return (empty, false),
            ControlGate::CentralDown(failure) => {
                self.observe_engine_store_down(&subject, failure);
                return (empty, true);
            }
        };
        match self.0.run_whole_unit(kind, now, &subject, &observed).await {
            Ok(WholeUnitOutcome::Skipped) => (empty, false),
            Ok(WholeUnitOutcome::Completed(work)) => {
                #[cfg(feature = "test-support")]
                self.engine_test_pause_points()
                    .reached_engine(
                        fleet_runtime::PausePoint::BeforeControlClear,
                        subject.kind.0,
                    )
                    .await;
                match self
                    .clear_observed_attempt_control(&subject, &observed)
                    .await
                {
                    Err(BatchApplyFailure::StorageFailed(_) | BatchApplyFailure::Rejected(_)) => {
                        (empty, true)
                    }
                    Ok(())
                    | Err(BatchApplyFailure::Superseded | BatchApplyFailure::StorageAnswerLost) => {
                        (work, false)
                    }
                }
            }
            Err(LegacyUnitOutcome::CentralDown(failure)) => {
                self.observe_engine_store_down(&subject, failure);
                (empty, true)
            }
            Err(outcome) => match outcome.failure_class() {
                Some(class) => (
                    empty,
                    matches!(
                        self.record_control_attempt_failure_for_workspaces(
                            &subject,
                            class,
                            threshold,
                            outcome.affected_workspaces(),
                        )
                        .await,
                        FailureOutcome::StoreDown(_)
                    ),
                ),
                None => (empty, false),
            },
        }
    }

    /// Direct sends and changing direct receipts request an alarm recomputation
    /// with their applying write. The due mark and the failure charge each settle
    /// independently, then this batch recomputes once to avoid paying for the
    /// alarm query (318 rows in the sparse fixture) per due item. System receipts
    /// keep their existing post-event completion path.
    /// Returns the completion and Central failure flags for the due dispatch.
    pub async fn run_checked_due_machine_instance<Rejected, Charge>(
        &self,
        subject: &EngineSubject,
        apply: impl std::future::Future<Output = Result<CheckedDueUnit<Rejected>, CheckedDueFailure>>,
        charge: impl FnOnce(Rejected, EngineTime) -> Charge,
    ) -> (bool, bool)
    where
        Charge: std::future::Future<
                Output = Result<Option<central_engine_model::FailureRecord>, CheckedDueFailure>,
            >,
    {
        let failure = match apply.await {
            Ok(CheckedDueUnit::Applied { ready }) => {
                if let Some(ready) = ready {
                    self.0.offer_ready_mail(ready);
                }
                #[cfg(feature = "test-support")]
                self.engine_test_pause_points()
                    .reached_engine(fleet_runtime::PausePoint::AfterEngineCommit, &subject.key.0)
                    .await;
                return (true, false);
            }
            Ok(CheckedDueUnit::Stale) => return (false, false),
            Ok(CheckedDueUnit::Failed(record)) => Ok(Some(record)),
            Ok(CheckedDueUnit::Rejected(rejected)) => {
                #[cfg(feature = "test-support")]
                self.engine_test_pause_points()
                    .reached_engine(
                        fleet_runtime::PausePoint::BeforeEngineFailureWrite,
                        &subject.key.0,
                    )
                    .await;
                match self.current_engine_time() {
                    Ok(now) => charge(rejected, now).await,
                    Err(failure) => Err(CheckedDueFailure::from_store(failure)),
                }
            }
            Err(failure) => Err(failure),
        };
        let failure = match failure {
            Ok(Some(record)) => {
                self.observe_recorded_attempt_failure(subject, &record, &[]);
                return (false, false);
            }
            Ok(None) | Err(CheckedDueFailure::AnswerLost) => return (false, false),
            Err(CheckedDueFailure::Unavailable) => StoreFailure::Unavailable,
            Err(CheckedDueFailure::Corrupt { reason }) => StoreFailure::Corrupt { reason },
        };
        self.observe_engine_store_down(subject, failure);
        (false, true)
    }

    pub async fn run_due_machine_instance<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        row: DueRow,
        due_column: &'static str,
        due_event: fn(EngineTime) -> SubjectMachine::Event,
    ) -> (bool, bool)
    where
        SubjectMachine: central_engine_model::Machine,
        SubjectMachine::State: Send + Sync,
        SubjectMachine::Event: Send + Sync,
        SubjectMachine::Effect: Send + Sync,
        SubjectMachine::Error: Send + Sync,
        SubjectStore: super::machine_store_adapter::MachineStoreAdapter<SubjectMachine>,
    {
        let subject = adapter.failure_attempt_subject(&row.key);
        let outcome = match EngineTime::from_timestamp_millis(row.occurrence_ms) {
            Some(occurrence) => {
                self.apply_machine_event::<SubjectMachine, SubjectStore, _>(
                    adapter,
                    row.key.clone(),
                    due_event(occurrence),
                    &super::event_source_policy::DueEventSource,
                )
                .await
            }
            // The due SQL clamps unreadable times, so this is defensive: the item
            // fails alone as CorruptRow and the batch continues (P3).
            None => {
                self.record_invalid_due_occurrence(adapter, &row.key, due_column)
                    .await
            }
        };
        match outcome {
            MachineEventOutcome::TransitionCommitted(_) => (true, false),
            MachineEventOutcome::StorageFailed(failure) => {
                self.observe_engine_store_down(&subject, failure);
                (false, true)
            }
            MachineEventOutcome::AttemptFailureRecorded(_)
            | MachineEventOutcome::RequestItemFailed(_)
            | MachineEventOutcome::SubjectMissing
            | MachineEventOutcome::TransitionRefused(_)
            | MachineEventOutcome::Superseded
            | MachineEventOutcome::StorageAnswerLost => (false, false),
        }
    }

    async fn record_invalid_due_occurrence<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        key: &InstanceKey,
        due_column: &'static str,
    ) -> MachineEventOutcome<SubjectMachine::State, SubjectMachine::Error>
    where
        SubjectMachine: central_engine_model::Machine,
        SubjectStore: super::machine_store_adapter::MachineStoreAdapter<SubjectMachine>,
    {
        let version = match adapter.load_subject_state(key).await {
            Ok(Some(loaded)) => loaded.observed_subject_version,
            Ok(None) => return MachineEventOutcome::SubjectMissing,
            Err(super::machine_store_adapter::SubjectLoadFailure::CorruptSubjectRow {
                observed_subject_version: version,
                ..
            }) => version,
            Err(super::machine_store_adapter::SubjectLoadFailure::StorageAnswerLost) => {
                return MachineEventOutcome::StorageAnswerLost;
            }
            Err(super::machine_store_adapter::SubjectLoadFailure::StoreReadFailed(failure)) => {
                return MachineEventOutcome::StorageFailed(failure);
            }
        };
        self.record_attempt_failure(
            &adapter.failure_attempt_subject(key),
            adapter.write_attempt_failure_sql(),
            &version,
            central_engine_model::ItemFailureClass::CorruptRow {
                column: due_column.into(),
                raw_value: None,
            },
            SubjectMachine::retirement_threshold(),
        )
        .await
        .into()
    }
}
