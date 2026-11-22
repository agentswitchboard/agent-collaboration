use crate::{
    stored_rows::{StoredWake, plan_wake_read},
    wake_timer as wake,
};
use central_engine_model::{EngineTime, FailureRecord, InstanceKey, StateTag};
use central_storage::{GuardedStatement, RowExpectation, StoreFailure};
use fleet_contracts::{AttemptGuard, EngineSubject, SubjectVersion};
use fleet_engine::{
    EngineHost, MachineStoreAdapter, SubjectAttemptWriter, SubjectLoadFailure,
    VersionedSubjectState, checked_engine_statement, checked_subject_attempt,
    classify_subject_decode_failure, classify_subject_read_failure, owner_sql::observed_text_binds,
};
use fleet_runtime_ports::CentralStore as _;
use fleet_runtime_ports::stored_values::format_utc_millis;
use workspace_model::central_mail as mail;

pub struct WakeStore<'a, R: EngineHost, PlanQueued> {
    runtime: &'a R,
    plan_queued: PlanQueued,
}
impl<'a, R, PlanQueued> WakeStore<'a, R, PlanQueued>
where
    R: EngineHost,
    PlanQueued: Fn(&mail::MessageRecord) -> Result<GuardedStatement, StoreFailure> + Send + Sync,
{
    pub fn new(runtime: &'a R, plan_queued: PlanQueued) -> Self {
        Self {
            runtime,
            plan_queued,
        }
    }
}

impl<R, PlanQueued> MachineStoreAdapter<wake::WakeTimer> for WakeStore<'_, R, PlanQueued>
where
    R: EngineHost,
    PlanQueued: Fn(&mail::MessageRecord) -> Result<GuardedStatement, StoreFailure> + Send + Sync,
{
    async fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<VersionedSubjectState<wake::State>>, SubjectLoadFailure> {
        let rows = self
            .runtime
            .central_store()
            .read(plan_wake_read(key))
            .await
            .map_err(classify_subject_read_failure)?;
        let Some((stored, version)) = StoredWake::observe(&rows)? else {
            return Ok(None);
        };
        let row = stored
            .decode_wake()
            .map_err(|failure| classify_subject_decode_failure(failure, version.clone()))?;
        let state = match row.state {
            mail::WakeState::Active => wake::State::Active(row),
            mail::WakeState::Done => wake::State::Done,
            mail::WakeState::Disabled => wake::State::Disabled,
        };
        Ok(Some(VersionedSubjectState {
            decoded_state: state,
            observed_subject_version: version,
        }))
    }
    fn plan_subject_state_write(
        &self,
        _key: &InstanceKey,
        prior: &VersionedSubjectState<wake::State>,
        next: &wake::State,
    ) -> Result<GuardedStatement, StoreFailure> {
        plan_wake_transition(
            &prior.observed_subject_version,
            next.state_tag(),
            self.runtime.current_engine_time()?,
        )
    }
    fn plan_machine_effect_writes(
        &self,
        _key: &InstanceKey,
        effect: &wake::Effect,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        match effect {
            wake::Effect::QueueMessage(message) => Ok(vec![(self.plan_queued)(message)?]),
        }
    }
    fn attempt_failure_writer(&self) -> SubjectAttemptWriter {
        plan_wake_attempt
    }
}

pub(super) fn plan_wake_transition(
    version: &SubjectVersion,
    state: &str,
    now: EngineTime,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        wake_id,
        target,
        due_at,
        body,
        prior_state,
        created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(GuardedStatement {
        statement: checked_engine_statement!(
            query_file,
            [],
            "src/queries/advance_wake.sql",
            next_state = state.to_owned(),
            updated_at = format_utc_millis(now),
            wake_id = wake_id,
            target = target,
            due_at = due_at,
            body = body,
            prior_state = prior_state,
            created_at = created_at,
            prior_updated_at = prior_updated_at,
        ),
        expect: RowExpectation::Exactly(1),
    })
}

pub(super) fn plan_wake_attempt(
    subject: &EngineSubject,
    record: &FailureRecord,
    prior: Option<&AttemptGuard>,
    version: &SubjectVersion,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        wake_id,
        target,
        due_at,
        body,
        prior_state,
        created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(checked_subject_attempt!(
        "src/queries/upsert_wake_attempt.sql",
        subject,
        record,
        prior,
        wake_id = wake_id,
        target = target,
        due_at = due_at,
        body = body,
        prior_state = prior_state,
        owner_created_at = created_at,
        prior_updated_at = prior_updated_at,
    ))
}
