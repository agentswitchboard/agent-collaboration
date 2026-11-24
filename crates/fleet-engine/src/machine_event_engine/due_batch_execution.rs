use super::{
    engine_host_binding::{EngineHost, FleetEngine},
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
    AlarmNotPublished, AlarmResult, ControlGate, EngineSubject, FailureOutcome, LegacyUnitOutcome,
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
    Handled { work: AlarmResult, progressed: bool },
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
        retry_cutoff: EngineTime,
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

pub(super) struct DuePass {
    pub(super) work: AlarmResult,
    pub(super) ending: DuePassEnding,
}

#[derive(Clone, Copy)]
pub(super) enum DuePassEnding {
    FullWithProgress,
    FullWithoutProgress,
    Short,
    StoreFault,
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
    read_due_rows_with_retry_cutoff(host, kind, cutoff, cutoff, limit).await
}

pub async fn read_due_rows_with_retry_cutoff(
    host: &impl CentralStore,
    kind: DueListKind,
    cutoff: EngineTime,
    retry_cutoff: EngineTime,
    limit: usize,
) -> Result<Vec<DueRow>, DueDiscoveryError> {
    let query =
        central_engine_model::due_list_query_with_retry_cutoff(kind, cutoff, retry_cutoff, limit)
            .map_err(DueDiscoveryError::InvalidBounds)?;
    host.read_timed_due_rows(query)
        .await
        .map_err(DueDiscoveryError::ReadFailed)
}

impl<Host: EngineHost> FleetEngine<'_, Host> {
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

    pub(super) async fn run_due_batch(&self, now: EngineTime, horizon: EngineTime) -> DuePass {
        let policy = DueBatchPolicy::DEFAULT;
        #[cfg(feature = "test-support")]
        self.due_batch_state()
            .record_batch_start(now.timestamp_millis());
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
                .discover_due_rows(self.0, kind, cutoff, now, policy.per_kind_cap + 1)
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
        let mut timed_progress = false;
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
                            return (AlarmResult::default(), false);
                        }
                        #[cfg(feature = "test-support")]
                        self.due_batch_state()
                            .record_unit_start(unit.build_due_unit_start_receipt(now));
                        let (due_work_result, progress, central_store_failed) =
                            self.dispatch_due_batch_unit(unit, now).await;
                        if central_store_failed {
                            stop_after_store_failure.store(true, Ordering::Release);
                        }
                        (due_work_result, progress)
                    }
                })
                .buffer_unordered(8);
            while let Some((done, progressed)) = completed.next().await {
                due_work_result += done;
                timed_progress |= progressed;
            }
            central_down = stop_after_store_failure.load(Ordering::Acquire);
        }
        let ending = match (central_down, more_timed_rows_due, timed_progress) {
            (true, _, _) => DuePassEnding::StoreFault,
            (false, true, false) => DuePassEnding::FullWithoutProgress,
            (false, true, true) => DuePassEnding::FullWithProgress,
            (false, false, _) => DuePassEnding::Short,
        };
        DuePass {
            work: due_work_result,
            ending,
        }
    }

    async fn dispatch_due_batch_unit(
        &self,
        unit: DueBatchUnit<
            <Host::TimedDueDispatch as TimedDueDispatch<Host>>::TimedSourceKind,
            Host::WholeUnitKind,
        >,
        now: EngineTime,
    ) -> (AlarmResult, bool, bool) {
        match unit {
            DueBatchUnit::TimedMachineRow(kind, _, row) => {
                match self
                    .0
                    .timed_due_dispatch()
                    .dispatch_timed_due_row(self, self.0, kind, row)
                    .await
                {
                    TimedRowOutcome::Handled { work, progressed } => (work, progressed, false),
                    TimedRowOutcome::CentralStoreFailed => (AlarmResult::default(), false, true),
                }
            }
            DueBatchUnit::Whole(kind, unit) => {
                let (work, failed) = self
                    .run_whole_unit_with_attempt_control(kind, unit, now)
                    .await;
                (work, false, failed)
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
}
