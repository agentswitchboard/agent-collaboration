//! Checked Central alarm and timed-list rows read on the owning executor.
use central_engine_model::{DueListQuery, DueRow, InstanceKey};
use central_storage::{CorruptReason, EngineAlarmTimes, SafeInteger, StoreFailure};
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Type};

/// An owning Central transaction lends the repository an executor.
/// The host retains responsibility for begin, commit and physical alarm writes.
pub trait CentralTransaction {
    type Database: Database;

    fn executor(&mut self) -> impl Executor<'_, Database = Self::Database>;

    fn rows_affected(result: <Self::Database as Database>::QueryResult) -> u64;
}

#[derive(sqlx::FromRow)]
struct StoredAlarmInputs {
    #[sqlx(rename = "on_grid_due?: i64")]
    on_grid_due: Option<i64>,
    #[sqlx(rename = "legacy_due?: i64")]
    legacy_due: Option<i64>,
}

pub async fn read_alarm_inputs<T: CentralTransaction>(
    transaction: &mut T,
) -> Result<EngineAlarmTimes, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    let row = sqlx::query_as::<T::Database, StoredAlarmInputs>(include_str!(
        "queries/read_alarm_inputs.sql"
    ))
    .fetch_one(transaction.executor())
    .await
    .map_err(|error| query_failure(error, "engine alarm input"))?;
    Ok(EngineAlarmTimes {
        earliest_on_grid_due_unix_millis: validate_time(row.on_grid_due, "engine alarm input")?,
        earliest_legacy_due_unix_millis: validate_time(row.legacy_due, "engine alarm input")?,
    })
}

#[derive(sqlx::FromRow)]
struct StoredDueRow {
    #[sqlx(rename = "subject_key?: String")]
    subject_key: Option<String>,
    #[sqlx(rename = "occurrence_ms!: i64")]
    occurrence_ms: i64,
    #[sqlx(rename = "effective_due_ms!: i64")]
    effective_due_ms: i64,
}

/// Reads an owner's due list and validates its storage types before Engine dispatch.
pub async fn read_timed_due_rows<T: CentralTransaction>(
    transaction: &mut T,
    query: DueListQuery,
) -> Result<Vec<DueRow>, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    String: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    sqlx::query_as::<T::Database, StoredDueRow>(query.sql)
        .bind(query.horizon.get())
        .bind(query.cap.get())
        .fetch_all(transaction.executor())
        .await
        .map_err(due_list_failure)?
        .into_iter()
        .map(|stored| {
            let subject_key = stored.subject_key.ok_or_else(|| StoreFailure::Corrupt {
                reason: CorruptReason::BadValue {
                    column: "subject_key".into(),
                },
            })?;
            Ok(DueRow {
                key: InstanceKey(subject_key),
                occurrence_ms: validate_integer(stored.occurrence_ms, "occurrence_ms")?,
                effective_due_ms: validate_integer(stored.effective_due_ms, "effective_due_ms")?,
            })
        })
        .collect()
}

fn due_list_failure(error: sqlx::Error) -> StoreFailure {
    match error {
        sqlx::Error::ColumnDecode { index, .. } => {
            let index = index.trim_matches('"');
            let field = index
                .split_once(['?', '!'])
                .map_or(index, |(field, _)| field);
            StoreFailure::Corrupt {
                reason: CorruptReason::BadValue {
                    column: field.into(),
                },
            }
        }
        error => query_failure(error, "engine.due_list"),
    }
}

fn validate_time(time: Option<i64>, column: &str) -> Result<Option<i64>, StoreFailure> {
    time.map(|value| validate_integer(value, column))
        .transpose()
}

fn validate_integer(value: i64, column: &str) -> Result<i64, StoreFailure> {
    SafeInteger::new(value)
        .map(|value| value.get())
        .map_err(|_| StoreFailure::Corrupt {
            reason: CorruptReason::BadValue {
                column: column.into(),
            },
        })
}

fn query_failure(error: sqlx::Error, column: &str) -> StoreFailure {
    match error {
        sqlx::Error::ColumnDecode { .. } | sqlx::Error::RowNotFound => StoreFailure::Corrupt {
            reason: CorruptReason::BadValue {
                column: column.into(),
            },
        },
        sqlx::Error::ColumnNotFound(column) => StoreFailure::Corrupt {
            reason: CorruptReason::MissingColumn { column },
        },
        _ => StoreFailure::Unavailable,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "central_queries_test.rs"]
mod tests;
