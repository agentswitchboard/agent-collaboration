use super::attempt_records::encode_attempt_failure_detail;
use central_engine_model::{AttemptState, FailureRecord};
use central_storage::{GuardedStatement, SqlValue, Statement, StoreFailure};
use fleet_contracts::{AttemptGuard, EngineSubject};
use fleet_runtime_ports::stored_values::format_utc_millis;
use fleet_runtime_ports::{
    guarded_statements::{any, exact, text},
    workspace_decode_error::corrupt,
};

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

pub const UPSERT_CONTROL_ATTEMPT_SQL: &str = include_str!("../queries/upsert_control_attempt.sql");

pub(crate) fn build_attempt_read_statement(subject: &EngineSubject) -> Statement {
    let kind = subject.kind.0;
    let key = subject.key.0.as_str();
    #[cfg(test)]
    let _ = sqlx::query_file_as!(
        super::attempt_records::FullAttemptProjection,
        "src/queries/read_full_attempt_by_subject.sql",
        kind,
        key
    );
    Statement {
        sql: include_str!("../queries/read_full_attempt_by_subject.sql"),
        args: vec![text(kind), text(key)],
    }
}

pub fn plan_attempt_delete(subject: &EngineSubject) -> GuardedStatement {
    let kind = subject.kind.0;
    let key = subject.key.0.as_str();
    #[cfg(test)]
    let _ = sqlx::query_file!("src/queries/delete_attempt_by_subject.sql", kind, key);
    any(
        include_str!("../queries/delete_attempt_by_subject.sql"),
        vec![text(kind), text(key)],
    )
}

/// Deletes the control record only while it is still the one a run observed
/// (the write guard's columns, compared with IS). A record written or replaced
/// after the observation matches nothing and survives.
pub fn plan_observed_attempt_delete(
    subject: &EngineSubject,
    observed: &AttemptGuard,
) -> GuardedStatement {
    let kind = subject.kind.0;
    let key = subject.key.0.as_str();
    // Guard cells retain their storage class even when the observed row is corrupt.
    #[cfg(test)]
    let _ = sqlx::query_file!(
        "src/queries/delete_observed_attempt.sql",
        kind,
        key,
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<String>,
        None::<String>
    );
    any(
        include_str!("../queries/delete_observed_attempt.sql"),
        vec![
            text(kind),
            text(key),
            observed.attempt_id.clone(),
            observed.state.clone(),
            observed.failure_count.clone(),
            observed.last_failure.clone(),
            observed.updated_at.clone(),
        ],
    )
}

fn encode_attempt_integer_bind(value: i64) -> Result<SqlValue, StoreFailure> {
    value
        .try_into()
        .map(SqlValue::Integer)
        .map_err(|_| corrupt("engine_work_attempts.failure_count"))
}

/// Bind slots 1..19; an adapter's subject-version values start at slot 20.
pub(crate) fn plan_guarded_attempt_upsert(
    sql: &'static str,
    subject: &EngineSubject,
    record: &FailureRecord,
    prior: Option<&AttemptGuard>,
    subject_version: Vec<SqlValue>,
) -> Result<GuardedStatement, StoreFailure> {
    let (state, next_try, retired_at) = match record.state {
        AttemptState::Failing { next_try_at } => (
            "failing",
            text(format_utc_millis(next_try_at)),
            SqlValue::Null,
        ),
        AttemptState::Retired { retired_at } => (
            "retired",
            SqlValue::Null,
            text(format_utc_millis(retired_at)),
        ),
    };
    let mut args = vec![
        text(record.attempt_id),
        text(subject.kind.0),
        text(&subject.key.0),
        text(state),
        encode_attempt_integer_bind(i64::from(record.failure_count.get()))?,
        next_try,
        text(record.last_failure.storage_tag()),
        encode_attempt_failure_detail(&record.last_failure).map_or(SqlValue::Null, text),
        text(format_utc_millis(record.first_failed_at)),
        text(format_utc_millis(record.last_failed_at)),
        retired_at,
        text(format_utc_millis(record.created_at)),
        text(format_utc_millis(record.updated_at)),
        encode_attempt_integer_bind(i64::from(prior.is_some()))?,
    ];
    match prior {
        Some(prior) => args.extend([
            prior.attempt_id.clone(),
            prior.state.clone(),
            prior.failure_count.clone(),
            prior.last_failure.clone(),
            prior.updated_at.clone(),
        ]),
        None => args.extend([
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ]),
    }
    #[cfg(test)]
    let _ = sqlx::query_file!(
        "src/queries/upsert_control_attempt.sql",
        record.attempt_id.to_string(),
        subject.kind.0,
        &subject.key.0,
        state,
        i64::from(record.failure_count.get()),
        None::<String>,
        record.last_failure.storage_tag(),
        encode_attempt_failure_detail(&record.last_failure),
        format_utc_millis(record.first_failed_at),
        format_utc_millis(record.last_failed_at),
        None::<String>,
        format_utc_millis(record.created_at),
        format_utc_millis(record.updated_at),
        i64::from(prior.is_some()),
        None::<String>,
        None::<String>,
        None::<i64>,
        None::<String>,
        None::<String>
    );
    args.extend(subject_version);
    Ok(exact(sql, args))
}

#[cfg(test)]
#[path = "attempt_statements_test.rs"]
mod tests;
