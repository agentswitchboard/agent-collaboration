use super::engine_host_binding::{EngineHost, FleetEngine};
use super::{
    attempt_statements::{UPSERT_CONTROL_ATTEMPT_SQL, plan_observed_attempt_delete},
    attempt_store::read_guarded_attempt,
    transition_batch_writes::{BatchApplyFailure, classify_batch_application},
};
use central_engine_model::{AttemptState, RetirementThreshold};
use central_storage::{AlarmUpdate, StatementBatch, StoreFailure};
use fleet_contracts::{
    ControlGate, EngineSubject, FailureOutcome, ObservedControl, SubjectVersion,
};
use fleet_runtime_ports::CentralStore as _;

impl<Host: EngineHost> FleetEngine<'_, Host> {
    pub async fn check_attempt_control_gate(
        &self,
        subject: &EngineSubject,
        threshold: RetirementThreshold,
    ) -> ControlGate {
        let stored = match read_guarded_attempt(self.central_store(), subject).await {
            Ok(None) => return ControlGate::Run(ObservedControl::Absent),
            Ok(Some(stored)) => stored,
            Err(StoreFailure::AnswerLost) => return ControlGate::Uncertain,
            Err(failure) => return ControlGate::CentralDown(failure),
        };
        match &stored.decoded_failure_record {
            Ok(record) => match record.state {
                AttemptState::Retired { .. } => ControlGate::Skip,
                AttemptState::Failing { next_try_at } => match self.current_engine_time() {
                    Ok(now) if next_try_at <= now => ControlGate::Run(ObservedControl::Present(
                        stored.observed_attempt_guard.clone(),
                    )),
                    Ok(_) => ControlGate::Skip,
                    Err(failure) => ControlGate::CentralDown(failure),
                },
            },
            Err(corruption) => match self
                .write_observed_attempt_failure(
                    subject,
                    UPSERT_CONTROL_ATTEMPT_SQL,
                    &SubjectVersion(vec![]),
                    corruption.to_corrupt_row_failure(),
                    threshold,
                    Some(&stored),
                )
                .await
            {
                FailureOutcome::Recorded(_) | FailureOutcome::Superseded => ControlGate::Skip,
                FailureOutcome::Uncertain => ControlGate::Uncertain,
                FailureOutcome::StoreDown(failure) => ControlGate::CentralDown(failure),
            },
        }
    }

    /// Clears the record this run observed at its gate, and nothing when it
    /// observed none: a newer failure written meanwhile survives (C3).
    pub(crate) async fn clear_observed_attempt_control(
        &self,
        subject: &EngineSubject,
        observed: &ObservedControl,
    ) -> Result<(), BatchApplyFailure> {
        let guard = match observed {
            ObservedControl::Absent => return Ok(()),
            ObservedControl::Present(guard) => guard,
        };
        classify_batch_application(
            self.central_store()
                .run_batch(StatementBatch {
                    statements: vec![plan_observed_attempt_delete(subject, guard)],
                    alarm: AlarmUpdate::Unchanged,
                })
                .await,
        )
    }
}
