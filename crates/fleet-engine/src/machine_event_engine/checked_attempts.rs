//! Attempt policy on a job's already-lent applying transaction.
use crate::CentralTransaction;
use central_engine_model::{
    AttemptState, EngineTime, FailureRecord, FailureTransition, ItemFailureClass,
    RetirementThreshold, after_failure,
};
use central_storage::{ConstraintKind, CorruptReason, ReadOutcome, SqlValue, StoreFailure};
use fleet_contracts::{EngineSubject, MailDeliveryReady};
use fleet_runtime_ports::{
    IdSource, stored_values::format_utc_millis, workspace_decode_error::corrupt,
};
use sqlx::{ColumnIndex, Database, Decode, Encode, IntoArguments, Type};

use super::attempt_records::{decode_attempt_with_guard, encode_attempt_failure_detail};

#[derive(sqlx::FromRow)]
struct StoredCheckedAttempt {
    attempt_id: Option<String>,
    state: String,
    failure_count: i64,
    next_try_at: Option<String>,
    last_failure: String,
    last_failure_detail: Option<String>,
    first_failed_at: String,
    last_failed_at: String,
    retired_at: Option<String>,
    created_at: String,
    updated_at: String,
}
impl StoredCheckedAttempt {
    fn decode(self) -> Result<Result<FailureRecord, ItemFailureClass>, StoreFailure> {
        let text = |value: Option<String>| value.map_or(SqlValue::Null, SqlValue::Text);
        let fields = [
            ("attempt_id", text(self.attempt_id)),
            ("state", SqlValue::Text(self.state)),
            (
                "failure_count",
                SqlValue::Integer(
                    self.failure_count
                        .try_into()
                        .map_err(|_| corrupt("engine_work_attempts.failure_count"))?,
                ),
            ),
            ("next_try_at", text(self.next_try_at)),
            ("last_failure", SqlValue::Text(self.last_failure)),
            ("last_failure_detail", text(self.last_failure_detail)),
            ("first_failed_at", SqlValue::Text(self.first_failed_at)),
            ("last_failed_at", SqlValue::Text(self.last_failed_at)),
            ("retired_at", text(self.retired_at)),
            ("created_at", SqlValue::Text(self.created_at)),
            ("updated_at", SqlValue::Text(self.updated_at)),
        ];
        let (columns, values) = fields
            .into_iter()
            .map(|(field, value)| (field.to_owned(), value))
            .unzip();
        let observed = ReadOutcome {
            columns,
            rows: vec![values],
        };
        Ok(decode_attempt_with_guard(&observed)?
            .ok_or_else(|| corrupt("engine_work_attempts.subject"))?
            .decoded_failure_record
            .map_err(|corruption| corruption.to_corrupt_row_failure()))
    }
}

macro_rules! attempt_read {
    ($emit:ident, $subject:expr) => {
        $emit!(
            "src/queries/read_checked_attempt.sql",
            $subject.kind.0.to_owned(),
            $subject.key.0.clone()
        )
    };
}
macro_rules! attempt_write {
    ($emit:ident, $subject:expr, $record:expr, $state:expr, $next:expr, $retired:expr) => {
        $emit!(
            "src/queries/record_checked_attempt.sql",
            $record.attempt_id.to_string(),
            $subject.kind.0.to_owned(),
            $subject.key.0.clone(),
            $state.to_owned(),
            i64::from($record.failure_count.get()),
            $next,
            $record.last_failure.storage_tag().to_owned(),
            encode_attempt_failure_detail(&$record.last_failure),
            format_utc_millis($record.first_failed_at),
            format_utc_millis($record.last_failed_at),
            $retired,
            format_utc_millis($record.created_at),
            format_utc_millis($record.updated_at)
        )
    };
}

pub enum CheckedAttempt {
    Ready(Option<FailureRecord>),
    Deferred,
    Corrupt(ItemFailureClass),
}

/// Result of a due write on a checked owning transaction.
pub enum CheckedDueUnit<Rejected> {
    Applied { ready: Option<MailDeliveryReady> },
    Stale,
    Failed(FailureRecord),
    Rejected(Rejected),
}

/// No due unit publishes an alarm. Its owning batch publishes once at the end.
///
/// ```compile_fail,E0599
/// use fleet_engine::CheckedDueFailure;
/// let _ = CheckedDueFailure::AlarmUnpublishable;
/// ```
#[derive(Debug, thiserror::Error)]
pub enum CheckedDueFailure {
    #[error("the due store is unavailable")]
    Unavailable,
    #[error("corrupt due data: {reason:?}")]
    Corrupt { reason: CorruptReason },
    #[error("the due transaction's answer was lost")]
    AnswerLost,
}

impl CheckedDueFailure {
    pub fn from_store(failure: StoreFailure) -> Self {
        match failure {
            StoreFailure::Corrupt { reason } => Self::Corrupt { reason },
            StoreFailure::AnswerLost => Self::AnswerLost,
            StoreFailure::Unavailable | StoreFailure::AlarmUnpublishable { .. } => {
                Self::Unavailable
            }
        }
    }
}

/// Use the same two drivers' SQL error classes as the guarded Engine path.
pub fn checked_statement_rejection(error: &sqlx::Error) -> Option<ItemFailureClass> {
    let sqlx::Error::Database(database) = error else {
        return None;
    };
    use sqlx::error::ErrorKind;
    let constraint = match database.kind() {
        ErrorKind::UniqueViolation => Some(ConstraintKind::Unique),
        ErrorKind::ForeignKeyViolation => Some(ConstraintKind::ForeignKey),
        ErrorKind::NotNullViolation => Some(ConstraintKind::NotNull),
        ErrorKind::CheckViolation => Some(ConstraintKind::CheckBoolean),
        _ => None,
    };
    if let Some(constraint) = constraint {
        return Some(ItemFailureClass::ConstraintViolated { constraint });
    }
    matches!(
        database.code().as_deref(),
        Some(
            "19" | "20"
                | "531"
                | "1043"
                | "1811"
                | "2323"
                | "2579"
                | "2835"
                | "3091"
                | "SQLITE_CONSTRAINT"
                | "SQLITE_MISMATCH"
                | "SQLITE_CONSTRAINT_COMMITHOOK"
                | "SQLITE_CONSTRAINT_FUNCTION"
                | "SQLITE_CONSTRAINT_TRIGGER"
                | "SQLITE_CONSTRAINT_VTAB"
                | "SQLITE_CONSTRAINT_ROWID"
                | "SQLITE_CONSTRAINT_PINNED"
                | "SQLITE_CONSTRAINT_DATATYPE"
        )
    )
    .then_some(ItemFailureClass::StatementRejected)
}

pub async fn observe_checked_attempt<T: CentralTransaction>(
    transaction: &mut T,
    subject: &EngineSubject,
    now: EngineTime,
) -> Result<CheckedAttempt, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    Option<String>: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    i64: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    macro_rules! typed {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            sqlx::query_as::<T::Database, StoredCheckedAttempt>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
        };
    }
    let row = attempt_read!(typed, subject)
        .fetch_optional(transaction.executor())
        .await
        .map_err(|_| StoreFailure::Unavailable)?;
    let Some(row) = row else {
        return Ok(CheckedAttempt::Ready(None));
    };
    Ok(match row.decode()? {
        Ok(record) => match record.state {
            AttemptState::Retired { .. } => CheckedAttempt::Deferred,
            AttemptState::Failing { next_try_at } if next_try_at > now => CheckedAttempt::Deferred,
            AttemptState::Failing { .. } => CheckedAttempt::Ready(Some(record)),
        },
        Err(class) => CheckedAttempt::Corrupt(class),
    })
}

/// A caller must hold the owning transaction from reading the affected subject
/// and attempt through this write. No other transaction can replace either one.
pub async fn record_checked_attempt<T: CentralTransaction>(
    transaction: &mut T,
    subject: &EngineSubject,
    prior: Option<&FailureRecord>,
    class: ItemFailureClass,
    now: EngineTime,
    ids: &dyn IdSource,
) -> Result<Option<FailureRecord>, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'value> Encode<'value, T::Database>,
    Option<String>: Type<T::Database> + for<'value> Encode<'value, T::Database>,
    i64: Type<T::Database> + for<'value> Encode<'value, T::Database>,
{
    let FailureTransition::Record(record) = after_failure(
        prior,
        class,
        now,
        RetirementThreshold::DEFAULT,
        ids.new_id(),
    )
    .map_err(|_| corrupt("engine_work_attempts.failure_update"))?
    else {
        return Ok(None);
    };
    let (state, next, retired) = match record.state {
        AttemptState::Failing { next_try_at } => {
            ("failing", Some(format_utc_millis(next_try_at)), None)
        }
        AttemptState::Retired { retired_at } => {
            ("retired", None, Some(format_utc_millis(retired_at)))
        }
    };
    macro_rules! write {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            sqlx::query::<T::Database>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
        };
    }
    let result = attempt_write!(write, subject, record, state, next, retired)
        .execute(transaction.executor())
        .await
        .map_err(|_| StoreFailure::Unavailable)?;
    if T::rows_affected(result) != 1 {
        return Err(corrupt("engine_work_attempts.failure_update"));
    }
    Ok(Some(record))
}

#[cfg(test)]
#[path = "checked_attempts_test.rs"]
mod tests;
