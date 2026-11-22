#[cfg(test)]
use super::attempt_records::CheckedFullAttemptProjection;
use super::{
    attempt_bindings::{AttemptBind, ObservedBind, attempt_statement},
    attempt_records::encode_attempt_failure_detail,
};
use central_engine_model::{AttemptState, FailureRecord};
use central_storage::{
    GuardedStatement, RowExpectation, SafeInteger, SqlValue, Statement, StoreFailure,
};
use fleet_contracts::{AttemptGuard, EngineSubject};
use fleet_runtime_ports::stored_values::format_utc_millis;
use fleet_runtime_ports::{guarded_statements::exact, workspace_decode_error::corrupt};

/// The literal predicate belongs to the adapter that owns the subject. No UPDATE
/// of the subject is used as a guard: that would retrigger its failed statement.
#[macro_export]
macro_rules! guarded_attempt_upsert_sql {
    ($subject_predicate:literal) => {
        concat!(
            "INSERT INTO engine_work_attempts(attempt_id,machine_kind,subject_key,state,failure_count,next_try_at,last_failure,last_failure_detail,first_failed_at,last_failed_at,retired_at,created_at,updated_at) ",
            "SELECT ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13 WHERE ",
            "(?14=0 OR EXISTS(SELECT 1 FROM engine_work_attempts WHERE machine_kind=?2 AND subject_key=?3 AND attempt_id IS ?15 AND state IS ?16 AND failure_count IS ?17 AND last_failure IS ?18 AND updated_at IS ?19)) AND (",
            $subject_predicate,
            ") ON CONFLICT(machine_kind,subject_key) DO UPDATE SET attempt_id=excluded.attempt_id,state=excluded.state,failure_count=excluded.failure_count,next_try_at=excluded.next_try_at,last_failure=excluded.last_failure,last_failure_detail=excluded.last_failure_detail,first_failed_at=excluded.first_failed_at,last_failed_at=excluded.last_failed_at,retired_at=excluded.retired_at,created_at=excluded.created_at,updated_at=excluded.updated_at ",
            "WHERE ?14=1 AND engine_work_attempts.attempt_id IS ?15 AND engine_work_attempts.state IS ?16 AND engine_work_attempts.failure_count IS ?17 AND engine_work_attempts.last_failure IS ?18 AND engine_work_attempts.updated_at IS ?19"
        )
    };
}

pub(crate) fn build_attempt_read_statement(subject: &EngineSubject) -> Statement {
    attempt_statement!(
        query_file_as,
        [CheckedFullAttemptProjection],
        "src/queries/read_full_attempt_by_subject.sql",
        machine_kind = subject.kind.0.to_owned(),
        subject_key = subject.key.0.clone(),
    )
}

pub fn plan_attempt_delete(subject: &EngineSubject) -> GuardedStatement {
    GuardedStatement {
        statement: attempt_statement!(
            query_file,
            [],
            "src/queries/delete_attempt_by_subject.sql",
            machine_kind = subject.kind.0.to_owned(),
            subject_key = subject.key.0.clone(),
        ),
        expect: RowExpectation::Any,
    }
}

/// Deletes the control record only while it is still the one a run observed
/// (the write guard's columns, compared with IS).
pub fn plan_observed_attempt_delete(
    subject: &EngineSubject,
    observed: &AttemptGuard,
) -> GuardedStatement {
    GuardedStatement {
        statement: attempt_statement!(
            query_file,
            [],
            "src/queries/delete_observed_attempt.sql",
            machine_kind = subject.kind.0.to_owned(),
            subject_key = subject.key.0.clone(),
            observed_attempt_id = ObservedBind::<String>::new(observed.attempt_id.clone()),
            observed_state = ObservedBind::<String>::new(observed.state.clone()),
            observed_count = ObservedBind::<i64>::new(observed.failure_count.clone()),
            observed_failure = ObservedBind::<String>::new(observed.last_failure.clone()),
            observed_update = ObservedBind::<String>::new(observed.updated_at.clone()),
        ),
        expect: RowExpectation::Any,
    }
}

fn encode_attempt_integer_bind(value: i64) -> Result<SafeInteger, StoreFailure> {
    value
        .try_into()
        .map_err(|_| corrupt("engine_work_attempts.failure_count"))
}

/// Bind slots 1..19; an adapter's subject-version values start at slot 20.
macro_rules! attempt_upsert_bindings {
    ($emit:ident, $subject:ident, $record:ident, $prior:ident) => {{
        let (state, next_try, retired) = match $record.state {
            AttemptState::Failing { next_try_at } => {
                ("failing", Some(format_utc_millis(next_try_at)), None)
            }
            AttemptState::Retired { retired_at } => {
                ("retired", None, Some(format_utc_millis(retired_at)))
            }
        };
        let absent_guard = AttemptGuard {
            attempt_id: SqlValue::Null,
            state: SqlValue::Null,
            failure_count: SqlValue::Null,
            last_failure: SqlValue::Null,
            updated_at: SqlValue::Null,
        };
        let guard = $prior.unwrap_or(&absent_guard);
        $emit!(
            attempt_id = $record.attempt_id.to_string(),
            machine_kind = $subject.kind.0.to_owned(),
            subject_key = $subject.key.0.clone(),
            attempt_state = state.to_owned(),
            failure_count = encode_attempt_integer_bind(i64::from($record.failure_count.get()))?,
            next_try_at = next_try,
            last_failure = $record.last_failure.storage_tag().to_owned(),
            last_failure_detail = encode_attempt_failure_detail(&$record.last_failure),
            first_failed_at = format_utc_millis($record.first_failed_at),
            last_failed_at = format_utc_millis($record.last_failed_at),
            retired_at = retired,
            created_at = format_utc_millis($record.created_at),
            updated_at = format_utc_millis($record.updated_at),
            has_prior = encode_attempt_integer_bind(i64::from($prior.is_some()))?,
            observed_attempt_id = ObservedBind::<String>::new(guard.attempt_id.clone()),
            observed_state = ObservedBind::<String>::new(guard.state.clone()),
            observed_count = ObservedBind::<i64>::new(guard.failure_count.clone()),
            observed_failure = ObservedBind::<String>::new(guard.last_failure.clone()),
            observed_update = ObservedBind::<String>::new(guard.updated_at.clone()),
        )
    }};
}

pub(super) fn plan_control_attempt_upsert(
    subject: &EngineSubject,
    record: &FailureRecord,
    prior: Option<&AttemptGuard>,
) -> Result<GuardedStatement, StoreFailure> {
    macro_rules! checked_control_statement {
        ($($bind:tt)*) => {
            attempt_statement!(query_file, [], "src/queries/upsert_control_attempt.sql", $($bind)*)
        };
    }

    Ok(GuardedStatement {
        statement: attempt_upsert_bindings!(checked_control_statement, subject, record, prior),
        expect: RowExpectation::Exactly(1),
    })
}

/// The owning adapter supplies its subject predicate and any extra version bindings.
pub(crate) fn plan_guarded_attempt_upsert(
    sql: &'static str,
    subject: &EngineSubject,
    record: &FailureRecord,
    prior: Option<&AttemptGuard>,
    subject_version: Vec<SqlValue>,
) -> Result<GuardedStatement, StoreFailure> {
    macro_rules! guarded_subject_arguments {
        ($($bind:ident = $value:expr),+ $(,)?) => {
            vec![$(AttemptBind::into_storage($value)),+]
        };
    }
    let mut args = attempt_upsert_bindings!(guarded_subject_arguments, subject, record, prior);
    args.extend(subject_version);
    Ok(exact(sql, args))
}

#[cfg(test)]
#[path = "attempt_statements_test.rs"]
mod tests;
