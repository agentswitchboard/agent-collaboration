//! Persistence and guarded settlement for system-message expiry.
use crate::{
    message_deadline::{EXPIRED_24_HOURS, system_message_deadline},
    system_message_expiry::{State, SystemMessageExpiry},
};
use central_engine_model::{EngineTime, FailureRecord, InstanceKey, StateTag};
use central_storage::{GuardedStatement, RowExpectation, SqlValue, Statement, StoreFailure};
use fleet_contracts::{AttemptGuard, EngineSubject, SubjectVersion};
use fleet_engine::{
    EngineHost, MachineStoreAdapter, SubjectAttemptWriter, SubjectLoadFailure,
    VersionedSubjectState, checked_engine_statement, checked_subject_attempt,
    checked_subject_projection, classify_subject_decode_failure, classify_subject_read_failure,
    owner_sql::observed_text_binds,
};
use fleet_runtime_ports::CentralStore as _;
use fleet_runtime_ports::stored_values::format_utc_millis;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use std::convert::Infallible;

checked_subject_projection!(StoredSystemExpiry {
    message_id: Option<String>,
    message_kind: String,
    state: String,
    failure_reason: Option<String>,
    delivery_window_ends_at: Option<String>,
    created_at: String,
    updated_at: String,
}; observed);

#[cfg(test)]
type CheckedSystemExpiry = StoredSystemExpiry<fleet_engine::owner_sql::CheckedStorage>;

fn plan_system_expiry_read(key: &InstanceKey) -> Statement {
    checked_engine_statement!(
        query_file_as,
        [CheckedSystemExpiry],
        "src/queries/read_system_expiry.sql",
        message_id = key.0.clone(),
    )
}

impl StoredSystemExpiry {
    fn decode_queued(&self) -> Result<State, StoreFailure> {
        self.message_id
            .stored_text()?
            .parse::<workspace_ids::MessageId>()
            .map_err(|_| corrupt("mail_messages.message_id"))?;
        if !matches!(
            self.message_kind.stored_text()?,
            "notice" | "deliveryOutcome"
        ) {
            return Err(corrupt("mail_messages.message_kind"));
        }
        if self.state.stored_text()? != "queued" {
            return Err(corrupt("mail_messages.state"));
        }
        if !matches!(self.failure_reason.raw(), SqlValue::Null) {
            return Err(corrupt("mail_messages.failure_reason"));
        }
        let deadline = self
            .delivery_window_ends_at
            .stored_text()?
            .parse::<EngineTime>()
            .map_err(|_| corrupt("delivery_window_ends_at"))?;
        let created = self
            .created_at
            .stored_text()?
            .parse::<EngineTime>()
            .map_err(|_| corrupt("created_at"))?;
        if system_message_deadline(created) != Some(deadline) {
            return Err(corrupt("mail_messages.delivery_window_ends_at"));
        }
        Ok(State::Queued { deadline })
    }
}

pub struct SystemMessageExpiryStore<'a, R: EngineHost>(&'a R);

impl<'a, R: EngineHost> SystemMessageExpiryStore<'a, R> {
    pub fn new(runtime: &'a R) -> Self {
        Self(runtime)
    }
}

impl<R: EngineHost> MachineStoreAdapter<SystemMessageExpiry> for SystemMessageExpiryStore<'_, R> {
    async fn load_subject_state(
        &self,
        key: &InstanceKey,
    ) -> Result<Option<VersionedSubjectState<State>>, SubjectLoadFailure> {
        let rows = self
            .0
            .central_store()
            .read(plan_system_expiry_read(key))
            .await
            .map_err(classify_subject_read_failure)?;
        let Some((stored, version)) = StoredSystemExpiry::observe(&rows)? else {
            return Ok(None);
        };
        let state = stored
            .decode_queued()
            .map_err(|failure| classify_subject_decode_failure(failure, version.clone()))?;
        Ok(Some(VersionedSubjectState {
            decoded_state: state,
            observed_subject_version: version,
        }))
    }

    fn plan_subject_state_write(
        &self,
        _key: &InstanceKey,
        prior: &VersionedSubjectState<State>,
        next: &State,
    ) -> Result<GuardedStatement, StoreFailure> {
        plan_system_expiry_transition(
            &prior.observed_subject_version,
            next.state_tag(),
            self.0.current_engine_time()?,
        )
    }

    fn plan_machine_effect_writes(
        &self,
        _key: &InstanceKey,
        effect: &Infallible,
    ) -> Result<Vec<GuardedStatement>, StoreFailure> {
        match *effect {}
    }

    fn attempt_failure_writer(&self) -> SubjectAttemptWriter {
        plan_system_expiry_attempt
    }
}

fn plan_system_expiry_transition(
    version: &SubjectVersion,
    state: &str,
    now: EngineTime,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        message_id,
        message_kind,
        prior_state,
        failure_reason,
        deadline,
        owner_created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(GuardedStatement {
        statement: checked_engine_statement!(
            query_file,
            [],
            "src/queries/advance_system_expiry.sql",
            next_state = state.to_owned(),
            expiry_reason = EXPIRED_24_HOURS.to_owned(),
            updated_at = format_utc_millis(now),
            message_id = message_id,
            message_kind = message_kind,
            prior_state = prior_state,
            failure_reason = failure_reason,
            deadline = deadline,
            owner_created_at = owner_created_at,
            prior_updated_at = prior_updated_at,
        ),
        expect: RowExpectation::Exactly(1),
    })
}

fn plan_system_expiry_attempt(
    subject: &EngineSubject,
    record: &FailureRecord,
    prior: Option<&AttemptGuard>,
    version: &SubjectVersion,
) -> Result<GuardedStatement, StoreFailure> {
    let [
        message_id,
        message_kind,
        prior_state,
        failure_reason,
        deadline,
        owner_created_at,
        prior_updated_at,
    ] = observed_text_binds(version)?;
    Ok(checked_subject_attempt!(
        "src/queries/upsert_system_expiry_attempt.sql",
        subject,
        record,
        prior,
        message_id = message_id,
        message_kind = message_kind,
        prior_state = prior_state,
        failure_reason = failure_reason,
        deadline = deadline,
        owner_created_at = owner_created_at,
        prior_updated_at = prior_updated_at,
    ))
}
