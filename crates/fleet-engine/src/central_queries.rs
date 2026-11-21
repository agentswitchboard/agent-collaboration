//! Checked Central alarm inputs read on the owning transaction's executor.
use central_storage::{CorruptReason, EngineAlarmTimes, SafeInteger, StoreFailure};
use sqlx::{ColumnIndex, Database, Decode, Executor, IntoArguments, Type};

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
