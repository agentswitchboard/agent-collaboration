use crate::{
    stored_rows::{WAKE_FAILURE, decode_wakes},
    wake_timer as wake,
};
use central_engine_model::{InstanceKey, StateTag};
use central_storage::{GuardedStatement, Statement, StoreFailure};
use fleet_engine::{
    EngineHost, MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState,
    classify_subject_decode_failure, classify_subject_read_failure, read_observed_subject_version,
};
use fleet_runtime_ports::stored_values::format_utc_millis;
use fleet_runtime_ports::{
    CentralStore as _,
    guarded_statements::{exact, text},
    workspace_decode_error::corrupt,
};
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
        let rows = self.runtime.central_store().read(Statement {
            sql: "SELECT wake_id,target,due_at,text,state,created_at,updated_at FROM automation_wakes WHERE wake_id=?", args: vec![text(&key.0)],
        }).await.map_err(classify_subject_read_failure)?;
        let Some(version) = read_observed_subject_version(&rows)? else {
            return Ok(None);
        };
        let row = decode_wakes(&rows)
            .map_err(|failure| classify_subject_decode_failure(failure, version.clone()))?
            .into_iter()
            .next()
            .ok_or_else(|| classify_subject_read_failure(corrupt("automation_wakes.wake_id")))?;
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
        let mut args = vec![
            text(next.state_tag()),
            text(format_utc_millis(self.runtime.current_engine_time()?)),
        ];
        args.extend(prior.observed_subject_version.0.clone());
        Ok(exact(
            "UPDATE automation_wakes SET state=?1,updated_at=?2 WHERE wake_id IS ?3 AND target IS ?4 AND due_at IS ?5 AND text IS ?6 AND state IS ?7 AND created_at IS ?8 AND updated_at IS ?9",
            args,
        ))
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
    fn write_attempt_failure_sql(&self) -> &'static str {
        WAKE_FAILURE
    }
}
