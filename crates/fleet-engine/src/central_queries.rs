//! Checked Central alarm and timed-list rows read on the owning executor.
use central_engine_model::{DueListKind, DueListQuery, DueRow, InstanceKey};
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

// Both the native witness and runtime decoder expand this private row declaration.
#[doc(hidden)]
#[macro_export]
macro_rules! __declare_timed_due_row {
    () => {
        #[derive(sqlx::FromRow)]
        struct StoredDueRow {
            #[sqlx(rename = "subject_key?: String")]
            subject_key: Option<String>,
            #[sqlx(rename = "occurrence_ms!: i64")]
            occurrence_ms: i64,
            #[sqlx(rename = "effective_due_ms!: i64")]
            effective_due_ms: i64,
        }
    };
}

/// Shares the owner file and positional bindings between runtime and the SQLx witness.
#[doc(hidden)]
#[macro_export]
macro_rules! __with_timed_due_sql {
    (@bind $file:literal, $emit:ident, $input:expr) => {{
        let (horizon, cap) = $input.bindings();
        $emit!($file, horizon, cap)
    }};
    (Wakes, $emit:ident, $input:expr) => {
        $crate::__with_timed_due_sql!(@bind "../fleet-wakes/src/queries/read_due_wakes.sql", $emit, $input)
    };
    (Schedules, $emit:ident, $input:expr) => {
        $crate::__with_timed_due_sql!(@bind "../fleet-schedules/src/queries/read_due_schedules.sql", $emit, $input)
    };
    (DirectMessages, $emit:ident, $input:expr) => {
        $crate::__with_timed_due_sql!(@bind "../fleet-mail/src/queries/read_due_direct_messages.sql", $emit, $input)
    };
    (SystemExpiries, $emit:ident, $input:expr) => {
        $crate::__with_timed_due_sql!(@bind "../fleet-mail/src/queries/read_due_system_expiries.sql", $emit, $input)
    };
    (Probes, $emit:ident, $input:expr) => {
        $crate::__with_timed_due_sql!(@bind "../collaboration-central-core/src/engine/queries/read_due_probes.sql", $emit, $input)
    };
}

/// Native, owner-local checks use the same typed row, SQL file and bind order as runtime.
#[macro_export]
macro_rules! check_timed_due_list {
    ($kind:ident, $input:expr) => {{
        $crate::__declare_timed_due_row!();
        macro_rules! witness {
            ($file:literal, $horizon:expr, $cap:expr) => {
                sqlx::query_file_as!(StoredDueRow, $file, $horizon, $cap)
            };
        }
        $crate::__with_timed_due_sql!($kind, witness, $input)
    }};
}

__declare_timed_due_row!();

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
    macro_rules! runtime {
        ($file:literal, $($arg:expr),+) => {
            sqlx::query_as::<T::Database, StoredDueRow>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))
                $(.bind($arg))+
        };
    }
    let select = match query.kind() {
        DueListKind::Wakes => __with_timed_due_sql!(Wakes, runtime, query),
        DueListKind::Schedules => __with_timed_due_sql!(Schedules, runtime, query),
        DueListKind::DirectMessages => __with_timed_due_sql!(DirectMessages, runtime, query),
        DueListKind::SystemExpiries => __with_timed_due_sql!(SystemExpiries, runtime, query),
        DueListKind::Probes => __with_timed_due_sql!(Probes, runtime, query),
    };
    select
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
