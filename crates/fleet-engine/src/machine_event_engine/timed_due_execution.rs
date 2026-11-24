use super::checked_attempts::{CheckedDueFailure, CheckedDueUnit};
use super::{
    engine_host_binding::{EngineHost, FleetEngine},
    machine_event_execution::MachineEventOutcome,
};
use central_engine_model::{DueRow, EngineTime, InstanceKey};
use central_storage::StoreFailure;
use fleet_contracts::EngineSubject;

pub enum TimedUnitOutcome {
    OccurrenceCommitted,
    AttemptRecorded,
    NoChange,
    CentralStoreFailed,
}

impl<Host: EngineHost> FleetEngine<'_, Host> {
    /// Direct sends and changing direct receipts request an alarm recomputation
    /// with their applying write. The due mark and the failure charge each settle
    /// independently, then this batch recomputes once to avoid paying for the
    /// alarm query (318 rows in the sparse fixture) per due item. System receipts
    /// keep their existing post-event completion path.
    /// Reports whether the occurrence or its attempt changed the actionable set.
    pub async fn run_checked_due_machine_instance<Rejected, Charge>(
        &self,
        subject: &EngineSubject,
        apply: impl std::future::Future<Output = Result<CheckedDueUnit<Rejected>, CheckedDueFailure>>,
        charge: impl FnOnce(Rejected, EngineTime) -> Charge,
    ) -> TimedUnitOutcome
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
                return TimedUnitOutcome::OccurrenceCommitted;
            }
            Ok(CheckedDueUnit::Stale) => return TimedUnitOutcome::NoChange,
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
                return TimedUnitOutcome::AttemptRecorded;
            }
            Ok(None) | Err(CheckedDueFailure::AnswerLost) => return TimedUnitOutcome::NoChange,
            Err(CheckedDueFailure::Unavailable) => StoreFailure::Unavailable,
            Err(CheckedDueFailure::Corrupt { reason }) => StoreFailure::Corrupt { reason },
        };
        self.observe_engine_store_down(subject, failure);
        TimedUnitOutcome::CentralStoreFailed
    }

    pub async fn run_due_machine_instance<SubjectMachine, SubjectStore>(
        &self,
        adapter: &SubjectStore,
        row: DueRow,
        due_column: &'static str,
        due_event: fn(EngineTime) -> SubjectMachine::Event,
    ) -> TimedUnitOutcome
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
            MachineEventOutcome::TransitionCommitted(_) => TimedUnitOutcome::OccurrenceCommitted,
            MachineEventOutcome::StorageFailed(failure) => {
                self.observe_engine_store_down(&subject, failure);
                TimedUnitOutcome::CentralStoreFailed
            }
            MachineEventOutcome::AttemptFailureRecorded(_) => TimedUnitOutcome::AttemptRecorded,
            MachineEventOutcome::RequestItemFailed(_)
            | MachineEventOutcome::SubjectMissing
            | MachineEventOutcome::TransitionRefused(_)
            | MachineEventOutcome::Superseded
            | MachineEventOutcome::StorageAnswerLost => TimedUnitOutcome::NoChange,
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
            }) => *version,
            Err(super::machine_store_adapter::SubjectLoadFailure::StorageAnswerLost) => {
                return MachineEventOutcome::StorageAnswerLost;
            }
            Err(super::machine_store_adapter::SubjectLoadFailure::StoreReadFailed(failure)) => {
                return MachineEventOutcome::StorageFailed(failure);
            }
        };
        self.record_attempt_failure(
            &adapter.failure_attempt_subject(key),
            adapter.attempt_failure_writer(),
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
