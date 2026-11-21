use crate::{
    attempt_statements::UPSERT_CONTROL_ATTEMPT_SQL,
    engine_host_binding::{EngineHost, FleetEngine},
    machine_event_execution::MachineEventOutcome,
    machine_store_adapter::corrupt_class,
    transition_batch_writes::ApplyFailure,
};
use central_engine_model::{
    DueBatchPolicy, DueListKind, DueRow, EngineTime, InstanceKey, MachineKind, RetirementThreshold,
};
use central_storage::{
    AlarmPublication, AlarmUpdate, BatchOutcome, EngineAlarmPlan, StatementBatch, StoreFailure,
};
use fleet_contracts::{
    AlarmNotPublished, AlarmResult, ControlGate, EngineSubject, EventCompletion, FailureOutcome,
    LegacyUnitOutcome, SubjectVersion,
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
pub trait TimedDueDispatch<R: EngineHost>: Send + Sync {
    type TimedSourceKind: Copy + Send + Sync + 'static;
    fn ordered_timed_source_kinds(&self) -> &'static [Self::TimedSourceKind];
    fn timed_source_schedule(&self, kind: Self::TimedSourceKind) -> TimedSourceSchedule;
    fn discover_due_rows(
        &self,
        runtime: &R,
        kind: Self::TimedSourceKind,
        cutoff: EngineTime,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<DueRow>, DueDiscoveryError>> + Send;
    fn dispatch_timed_due_row(
        &self,
        engine: &FleetEngine<'_, R>,
        runtime: &R,
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

enum Unit<TimedKind, WholeKind> {
    Machine(
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
impl<TimedKind, WholeKind> Unit<TimedKind, WholeKind> {
    fn start_receipt(&self, now: EngineTime) -> fleet_runtime::DueUnitStart {
        let (kind, key) = match self {
            Self::Machine(_, kind, row) => (kind.0, row.key.0.clone()),
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

fn source(kind: MachineKind) -> EngineSubject {
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

impl<R: EngineHost> FleetEngine<'_, R> {
    /// Every event reaches this after its own work. The claim occurs before the
    /// first await; only individual units perform async work, at most eight wide.
    pub(crate) async fn after_event(&self) -> Result<EventCompletion, AlarmNotPublished> {
        #[cfg(feature = "test-support")]
        {
            self.0.record_engine_completion_entry();
            self.pauses()
                .reached_central(fleet_runtime::PausePoint::EngineCompletionEntered)
                .await;
        }
        let result = self.complete_event().await;
        if let Err(error) = &result {
            self.observe_alarm_not_published(error.failure.clone());
        }
        result
    }

    async fn complete_event(&self) -> Result<EventCompletion, AlarmNotPublished> {
        let now = self
            .now()
            .map_err(|failure| AlarmNotPublished { failure })?;
        let claimed = self
            .due_batches()
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
        let mut work = AlarmResult::default();
        if claimed {
            work = self.run_due_batch(now).await;
        }
        let publication = self
            .publish_engine_alarm(EngineAlarmPlan::Recompute)
            .await?;
        Ok(EventCompletion { work, publication })
    }

    pub async fn publish_engine_alarm(
        &self,
        plan: EngineAlarmPlan,
    ) -> Result<AlarmPublication, AlarmNotPublished> {
        match self
            .host()
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
        let mut leftovers = false;
        let mut central_down = false;
        let timed = self.0.timed_due_dispatch();
        for &kind in timed.ordered_timed_source_kinds() {
            let description = timed.timed_source_schedule(kind);
            let subject = source(description.machine_kind);
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
            let cutoff = match description.due_cutoff_policy {
                DueCutoff::BatchStart => now,
                DueCutoff::LookAheadHorizon => horizon,
            };
            match timed
                .discover_due_rows(self.0, kind, cutoff, policy.per_kind_cap + 1)
                .await
            {
                Ok(mut rows) => {
                    leftovers |= rows.len() > policy.per_kind_cap;
                    rows.truncate(policy.per_kind_cap);
                    units.extend(
                        rows.into_iter()
                            .map(|row| Unit::Machine(kind, description.machine_kind, row)),
                    );
                    match self.clear_control(&subject, &observed).await {
                        Ok(()) | Err(ApplyFailure::Superseded | ApplyFailure::Uncertain) => {}
                        Err(ApplyFailure::StoreDown(_) | ApplyFailure::Rejected(_)) => {
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
                        self.record_failure(
                            &subject,
                            UPSERT_CONTROL_ATTEMPT_SQL,
                            &SubjectVersion(vec![]),
                            corrupt_class(reason),
                            RetirementThreshold::DEFAULT
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
        let mut work = AlarmResult::default();
        if !central_down {
            units.extend(
                self.0
                    .ordered_whole_units()
                    .iter()
                    .map(|&kind| Unit::Whole(kind, self.0.whole_unit(kind))),
            );
            let stop = AtomicBool::new(false);
            let mut completed = stream::iter(units)
                .map(|unit| {
                    let stop = &stop;
                    async move {
                        if stop.load(Ordering::Acquire) {
                            return AlarmResult::default();
                        }
                        #[cfg(feature = "test-support")]
                        self.due_batches()
                            .record_unit_start(unit.start_receipt(now));
                        let (work, down) = self.run_engine_unit(unit, now).await;
                        if down {
                            stop.store(true, Ordering::Release);
                        }
                        work
                    }
                })
                .buffer_unordered(8);
            while let Some(done) = completed.next().await {
                work += done;
            }
        }
        self.due_batches().finish(now.timestamp_millis(), leftovers);
        work
    }

    async fn run_engine_unit(
        &self,
        unit: Unit<<R::TimedDueDispatch as TimedDueDispatch<R>>::TimedSourceKind, R::WholeUnitKind>,
        now: EngineTime,
    ) -> (AlarmResult, bool) {
        match unit {
            Unit::Machine(kind, _, row) => {
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
            Unit::Whole(kind, unit) => self.run_whole_unit(kind, unit, now).await,
        }
    }

    async fn run_whole_unit(
        &self,
        kind: R::WholeUnitKind,
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
                self.pauses()
                    .reached_engine(
                        fleet_runtime::PausePoint::BeforeControlClear,
                        subject.kind.0,
                    )
                    .await;
                match self.clear_control(&subject, &observed).await {
                    Err(ApplyFailure::StoreDown(_) | ApplyFailure::Rejected(_)) => (empty, true),
                    Ok(()) | Err(ApplyFailure::Superseded | ApplyFailure::Uncertain) => {
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
                        self.record_attempt_failure_for_workspaces(
                            &subject,
                            UPSERT_CONTROL_ATTEMPT_SQL,
                            &SubjectVersion(vec![]),
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

    /// One due machine instance as its own unit: `(done, central_down)`.
    pub async fn run_due_machine_instance<M, Adapter>(
        &self,
        adapter: &Adapter,
        row: DueRow,
        due_column: &'static str,
        due_event: fn(EngineTime) -> M::Event,
    ) -> (bool, bool)
    where
        M: central_engine_model::Machine,
        M::State: Send + Sync,
        M::Event: Send + Sync,
        M::Effect: Send + Sync,
        M::Error: Send + Sync,
        Adapter: crate::machine_store_adapter::MachineStoreAdapter<M>,
    {
        let subject = adapter.failure_attempt_subject(&row.key);
        let outcome = match EngineTime::from_timestamp_millis(row.occurrence_ms) {
            Some(occurrence) => {
                self.handle::<M, Adapter, _>(
                    adapter,
                    row.key.clone(),
                    due_event(occurrence),
                    &crate::event_source_policy::DueSource,
                )
                .await
            }
            // The due SQL clamps unreadable times, so this is defensive: the item
            // fails alone as CorruptRow and the batch continues (P3).
            None => {
                self.record_unrepresentable_due(adapter, &row.key, due_column)
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
            | MachineEventOutcome::EventSuperseded
            | MachineEventOutcome::StorageAnswerLost => (false, false),
        }
    }

    async fn record_unrepresentable_due<M, Adapter>(
        &self,
        adapter: &Adapter,
        key: &InstanceKey,
        due_column: &'static str,
    ) -> MachineEventOutcome<M::State, M::Error>
    where
        M: central_engine_model::Machine,
        Adapter: crate::machine_store_adapter::MachineStoreAdapter<M>,
    {
        let version = match adapter.load_subject_state(key).await {
            Ok(Some(loaded)) => loaded.observed_subject_version,
            Ok(None) => return MachineEventOutcome::SubjectMissing,
            Err(crate::machine_store_adapter::SubjectLoadFailure::CorruptSubjectRow {
                observed_subject_version: version,
                ..
            }) => version,
            Err(crate::machine_store_adapter::SubjectLoadFailure::StorageAnswerLost) => {
                return MachineEventOutcome::StorageAnswerLost;
            }
            Err(crate::machine_store_adapter::SubjectLoadFailure::StoreReadFailed(failure)) => {
                return MachineEventOutcome::StorageFailed(failure);
            }
        };
        self.record_failure(
            &adapter.failure_attempt_subject(key),
            adapter.write_attempt_failure_sql(),
            &version,
            central_engine_model::ItemFailureClass::CorruptRow {
                column: due_column.into(),
                raw_value: None,
            },
            M::retirement_threshold(),
        )
        .await
        .into()
    }
}
