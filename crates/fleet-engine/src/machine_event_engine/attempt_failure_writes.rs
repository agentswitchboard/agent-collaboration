use super::engine_host_binding::{EngineHost, FleetEngine};
use super::{
    attempt_records::StoredAttempt,
    attempt_statements::plan_control_attempt_upsert,
    attempt_store::read_guarded_attempt,
    machine_store_adapter::SubjectAttemptWriter,
    transition_batch_writes::{BatchApplyFailure, classify_batch_application},
};
use central_engine_model::{
    FailureTransition, ItemFailureClass, RetirementThreshold, after_failure,
};
use central_storage::{AlarmUpdate, StatementBatch, StoreFailure};
use fleet_contracts::{EngineSubject, FailureOutcome};
use fleet_runtime_ports::CentralStore as _;
use workspace_model::WorkspaceId;

enum AttemptFailureWrite<'a, Version> {
    Control,
    Subject {
        writer: SubjectAttemptWriter<Version>,
        version: &'a Version,
    },
}

impl<Host: EngineHost> FleetEngine<'_, Host> {
    pub fn observe_engine_store_down(&self, subject: &EngineSubject, failure: StoreFailure) {
        self.emit_central_observation(fleet_contracts::CentralObservation::EngineStoreDown {
            kind: subject.kind,
            key: subject.key.clone(),
            failure,
        });
    }

    pub(crate) fn observe_recorded_attempt_failure(
        &self,
        subject: &EngineSubject,
        record: &central_engine_model::FailureRecord,
        workspaces: &[WorkspaceId],
    ) {
        self.emit_central_observation(fleet_contracts::CentralObservation::EngineItemFailed {
            kind: subject.kind,
            key: subject.key.clone(),
            failure: record.last_failure.clone(),
            count: record.failure_count.get(),
            retired: matches!(
                record.state,
                central_engine_model::AttemptState::Retired { .. }
            ),
            workspaces: workspaces.to_vec(),
        });
    }

    pub(crate) fn observe_alarm_not_published(&self, failure: StoreFailure) {
        self.emit_central_observation(
            fleet_contracts::CentralObservation::EngineAlarmNotPublished { failure },
        );
    }
    pub(crate) async fn record_attempt_failure<Version>(
        &self,
        subject: &EngineSubject,
        writer: SubjectAttemptWriter<Version>,
        version: &Version,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
    ) -> FailureOutcome {
        self.read_and_record_attempt_failure(
            subject,
            AttemptFailureWrite::Subject { writer, version },
            class,
            threshold,
            &[],
        )
        .await
    }

    /// Records a control failure whose observation names the affected workspaces.
    pub async fn record_control_attempt_failure_for_workspaces(
        &self,
        subject: &EngineSubject,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
        workspaces: &[WorkspaceId],
    ) -> FailureOutcome {
        self.read_and_record_attempt_failure(
            subject,
            AttemptFailureWrite::<()>::Control,
            class,
            threshold,
            workspaces,
        )
        .await
    }

    async fn read_and_record_attempt_failure<Version>(
        &self,
        subject: &EngineSubject,
        write: AttemptFailureWrite<'_, Version>,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
        workspaces: &[WorkspaceId],
    ) -> FailureOutcome {
        let prior = match read_guarded_attempt(self.central_store(), subject).await {
            Ok(prior) => prior,
            Err(StoreFailure::AnswerLost) => return FailureOutcome::Uncertain,
            Err(failure) => return FailureOutcome::StoreDown(failure),
        };
        self.write_attempt_failure_and_observe(
            subject,
            write,
            class,
            threshold,
            prior.as_ref(),
            workspaces,
        )
        .await
    }

    pub(crate) async fn write_observed_attempt_failure<Version>(
        &self,
        subject: &EngineSubject,
        writer: SubjectAttemptWriter<Version>,
        version: &Version,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
        prior: Option<&StoredAttempt>,
    ) -> FailureOutcome {
        self.write_attempt_failure_and_observe(
            subject,
            AttemptFailureWrite::Subject { writer, version },
            class,
            threshold,
            prior,
            &[],
        )
        .await
    }

    pub(super) async fn write_observed_control_attempt_failure(
        &self,
        subject: &EngineSubject,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
        prior: &StoredAttempt,
    ) -> FailureOutcome {
        self.write_attempt_failure_and_observe(
            subject,
            AttemptFailureWrite::<()>::Control,
            class,
            threshold,
            Some(prior),
            &[],
        )
        .await
    }

    async fn write_attempt_failure_and_observe<Version>(
        &self,
        subject: &EngineSubject,
        write: AttemptFailureWrite<'_, Version>,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
        prior: Option<&StoredAttempt>,
        workspaces: &[WorkspaceId],
    ) -> FailureOutcome {
        #[cfg(feature = "test-support")]
        self.engine_test_pause_points()
            .reached_engine(
                fleet_runtime::PausePoint::BeforeEngineFailureWrite,
                &subject.key.0,
            )
            .await;
        let (record, class) = match prior.map(|stored| &stored.decoded_failure_record) {
            Some(Ok(record)) => (Some(record), class),
            Some(Err(corruption)) => (None, corruption.to_corrupt_row_failure()),
            None => (None, class),
        };
        let now = match self.current_engine_time() {
            Ok(now) => now,
            Err(failure) => return FailureOutcome::StoreDown(failure),
        };
        // The pure transition decides against the record just read, and the
        // write below is guarded by that same record: a failure seen before the
        // next try, or after retirement, writes nothing (R4).
        let record = match after_failure(
            record,
            class,
            now,
            threshold,
            self.host_id_source().new_id(),
        ) {
            Ok(FailureTransition::Record(record)) => record,
            Ok(FailureTransition::Unchanged) => return FailureOutcome::Superseded,
            Err(_) => {
                return FailureOutcome::StoreDown(
                    fleet_runtime_ports::workspace_decode_error::corrupt(
                        "engine_work_attempts.failure_update",
                    ),
                );
            }
        };
        let observed = prior.map(|stored| &stored.observed_attempt_guard);
        let statement = match write {
            AttemptFailureWrite::Control => plan_control_attempt_upsert(subject, &record, observed),
            AttemptFailureWrite::Subject { writer, version } => {
                writer(subject, &record, observed, version)
            }
        };
        let statement = match statement {
            Ok(statement) => statement,
            Err(failure) => return FailureOutcome::StoreDown(failure),
        };
        match classify_batch_application(
            self.central_store()
                .run_batch(StatementBatch {
                    statements: vec![statement],
                    alarm: AlarmUpdate::Unchanged,
                })
                .await,
        ) {
            Ok(()) => {
                self.observe_recorded_attempt_failure(subject, &record, workspaces);
                FailureOutcome::Recorded(record)
            }
            Err(BatchApplyFailure::Superseded) => FailureOutcome::Superseded,
            Err(BatchApplyFailure::StorageAnswerLost) => FailureOutcome::Uncertain,
            Err(BatchApplyFailure::StorageFailed(failure)) => FailureOutcome::StoreDown(failure),
            // A rejected attempt write cannot charge another attempt or recurse.
            Err(BatchApplyFailure::Rejected(_)) => FailureOutcome::StoreDown(
                fleet_runtime_ports::workspace_decode_error::corrupt("engine_work_attempts.write"),
            ),
        }
    }
}
