use super::engine_host_binding::{EngineHost, FleetEngine};
use super::{
    attempt_records::StoredAttempt,
    attempt_statements::plan_guarded_attempt_upsert,
    attempt_store::read_guarded_attempt,
    transition_batch_writes::{BatchApplyFailure, classify_batch_application},
};
use central_engine_model::{
    FailureTransition, ItemFailureClass, RetirementThreshold, after_failure,
};
use central_storage::{AlarmUpdate, StatementBatch, StoreFailure};
use fleet_contracts::{EngineSubject, FailureOutcome, SubjectVersion};
use fleet_runtime_ports::CentralStore as _;
use workspace_model::WorkspaceId;

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
    pub(crate) async fn record_attempt_failure(
        &self,
        subject: &EngineSubject,
        failure_sql: &'static str,
        version: &SubjectVersion,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
    ) -> FailureOutcome {
        self.record_attempt_failure_for_workspaces(
            subject,
            failure_sql,
            version,
            class,
            threshold,
            &[],
        )
        .await
    }

    /// Records a failure whose observation names the workspaces a notice unit
    /// affected; the stored record keeps only its class (P2, P5 change 4).
    pub async fn record_attempt_failure_for_workspaces(
        &self,
        subject: &EngineSubject,
        failure_sql: &'static str,
        version: &SubjectVersion,
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
            failure_sql,
            version,
            class,
            threshold,
            prior.as_ref(),
            workspaces,
        )
        .await
    }

    pub(crate) async fn write_observed_attempt_failure(
        &self,
        subject: &EngineSubject,
        failure_sql: &'static str,
        version: &SubjectVersion,
        class: ItemFailureClass,
        threshold: RetirementThreshold,
        prior: Option<&StoredAttempt>,
    ) -> FailureOutcome {
        self.write_attempt_failure_and_observe(
            subject,
            failure_sql,
            version,
            class,
            threshold,
            prior,
            &[],
        )
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "one guarded failure write needs the subject, its guard and the record inputs together"
    )]
    async fn write_attempt_failure_and_observe(
        &self,
        subject: &EngineSubject,
        failure_sql: &'static str,
        version: &SubjectVersion,
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
        let statement = match plan_guarded_attempt_upsert(
            failure_sql,
            subject,
            &record,
            prior.map(|stored| &stored.observed_attempt_guard),
            version.0.clone(),
        ) {
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
