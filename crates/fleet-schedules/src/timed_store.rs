use crate::{
    automation_timer as automation,
    stored_rows::{StoredSchedule, plan_schedule_read},
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

pub struct AutomationStore<'a, R: EngineHost, PlanQueued> {
    runtime: &'a R,
    plan_queued: PlanQueued,
}
impl<'a, R, PlanQueued> AutomationStore<'a, R, PlanQueued>
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

impl<R, PlanQueued> MachineStoreAdapter<automation::AutomationTimer>
    for AutomationStore<'_, R, PlanQueued>
where
    R: EngineHost,
    PlanQueued: Fn(&mail::MessageRecord) -> Result<GuardedStatement, StoreFailure> + Send + Sync,
{
    async fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<VersionedSubjectState<automation::State>>, SubjectLoadFailure> {
        let rows = self
            .runtime
            .central_store()
            .read(plan_schedule_read(key))
            .await
            .map_err(classify_subject_read_failure)?;
        let Some((stored, version)) = StoredSchedule::observe(&rows)? else {
            return Ok(None);
        };
        let row = stored
            .decode_schedule()
            .map_err(|failure| classify_subject_decode_failure(failure, version.clone()))?;
        let state = match row.state {
            mail::AutomationState::Active => automation::State::Active(row),
            mail::AutomationState::Done => automation::State::Done {
                evaluated: row.evaluated,
            },
            mail::AutomationState::Disabled => automation::State::Disabled {
                evaluated: row.evaluated,
            },
        };
        Ok(Some(VersionedSubjectState {
            decoded_state: state,
            observed_subject_version: version,
        }))
    }
    fn plan_subject_state_write(
        &self,
        _key: &InstanceKey,
        prior: &VersionedSubjectState<automation::State>,
        next: &automation::State,
    ) -> Result<GuardedStatement, StoreFailure> {
        let evaluated = match next {
            automation::State::Active(row) => row.evaluated,
            automation::State::Done { evaluated } | automation::State::Disabled { evaluated } => {
                *evaluated
            }
        };
        plan_schedule_transition(
            &prior.observed_subject_version,
            evaluated,
            next.state_tag(),
            self.runtime.current_engine_time()?,
        )
    }
    fn plan_machine_effect_writes(
        &self,
        key: &InstanceKey,
        effect: &automation::Effect,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        Ok(match effect {
            automation::Effect::QueueMessage(message) => vec![(self.plan_queued)(message)?],
            automation::Effect::ScheduleAt(due) => vec![GuardedStatement {
                statement: checked_engine_statement!(
                    query_file,
                    [],
                    "src/queries/set_schedule_due.sql",
                    next_due_at = format_utc_millis(*due),
                    automation_id = key.0.clone(),
                ),
                expect: RowExpectation::Exactly(1),
            }],
            automation::Effect::ClearSchedule => vec![GuardedStatement {
                statement: checked_engine_statement!(
                    query_file,
                    [],
                    "src/queries/clear_schedule_due.sql",
                    automation_id = key.0.clone(),
                ),
                expect: RowExpectation::Exactly(1),
            }],
        })
    }
    fn attempt_failure_writer(&self) -> SubjectAttemptWriter {
        plan_schedule_attempt
    }
}

fn plan_schedule_transition(
    version: &SubjectVersion,
    evaluated: Option<EngineTime>,
    state: &str,
    now: EngineTime,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        automation_id,
        target,
        timing_rule,
        anchor_at,
        expires_at,
        evaluated_through_at,
        body,
        next_due_at,
        prior_state,
        owner_created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(GuardedStatement {
        statement: checked_engine_statement!(
            query_file,
            [],
            "src/queries/advance_schedule.sql",
            next_evaluated = evaluated.map(format_utc_millis),
            next_state = state.to_owned(),
            updated_at = format_utc_millis(now),
            automation_id = automation_id,
            target = target,
            timing_rule = timing_rule,
            anchor_at = anchor_at,
            expires_at = expires_at,
            evaluated_through_at = evaluated_through_at,
            body = body,
            next_due_at = next_due_at,
            prior_state = prior_state,
            owner_created_at = owner_created_at,
            prior_updated_at = prior_updated_at,
        ),
        expect: RowExpectation::Exactly(1),
    })
}

pub(super) fn plan_schedule_finish(
    version: &SubjectVersion,
    state: &str,
    now: EngineTime,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        automation_id,
        target,
        timing_rule,
        anchor_at,
        expires_at,
        evaluated_through_at,
        body,
        next_due_at,
        prior_state,
        owner_created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(GuardedStatement {
        statement: checked_engine_statement!(
            query_file,
            [],
            "src/queries/finish_schedule.sql",
            next_state = state.to_owned(),
            updated_at = format_utc_millis(now),
            automation_id = automation_id,
            target = target,
            timing_rule = timing_rule,
            anchor_at = anchor_at,
            expires_at = expires_at,
            evaluated_through_at = evaluated_through_at,
            body = body,
            next_due_at = next_due_at,
            prior_state = prior_state,
            owner_created_at = owner_created_at,
            prior_updated_at = prior_updated_at,
        ),
        expect: RowExpectation::Exactly(1),
    })
}

pub(super) fn plan_schedule_attempt(
    subject: &EngineSubject,
    record: &FailureRecord,
    prior: Option<&AttemptGuard>,
    version: &SubjectVersion,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        automation_id,
        target,
        timing_rule,
        anchor_at,
        expires_at,
        evaluated_through_at,
        body,
        next_due_at,
        prior_state,
        owner_created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(checked_subject_attempt!(
        "src/queries/upsert_schedule_attempt.sql",
        subject,
        record,
        prior,
        automation_id = automation_id,
        target = target,
        timing_rule = timing_rule,
        anchor_at = anchor_at,
        expires_at = expires_at,
        evaluated_through_at = evaluated_through_at,
        body = body,
        next_due_at = next_due_at,
        prior_state = prior_state,
        owner_created_at = owner_created_at,
        prior_updated_at = prior_updated_at,
    ))
}
