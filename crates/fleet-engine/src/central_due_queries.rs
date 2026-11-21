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

/// Declares the only kind-to-file mapping used by runtime and the owner witnesses.
macro_rules! declare_timed_due_lists {
    ($d:tt; $($(#[$feature:meta])* $kind:ident, $witness:ident, $file:literal);+ $(;)?) => {
        $(
            $(#[$feature])*
            #[doc(hidden)]
            #[macro_export]
            macro_rules! $witness {
                ($d emit:ident, $d input:expr) => {{
                    let (horizon, cap) = $d input.bindings();
                    $d emit!($file, horizon, cap)
                }};
            }
        )+

        /// Checks the SQL and binds for one kind on its owning crate's SQLite schema.
        #[macro_export]
        macro_rules! check_timed_due_list {
            $(
                ($kind) => {{
                    const _: () = {
                        let path = $file.as_bytes();
                        let owner = concat!("../", env!("CARGO_PKG_NAME"), "/").as_bytes();
                        assert!(path.len() > owner.len(), "timed-list witness must be in its owning crate");
                        let mut offset = 0;
                        while offset < owner.len() {
                            assert!(path[offset] == owner[offset], "timed-list witness must be in its owning crate");
                            offset += 1;
                        }
                    };
                    $crate::__declare_timed_due_row!();
                    macro_rules! witness {
                        ($d sql:literal, $d horizon:expr, $d cap:expr) => {
                            sqlx::query_file_as!(StoredDueRow, $d sql, $d horizon, $d cap)
                        };
                    }
                    let query = central_engine_model::due_list_query(
                        central_engine_model::DueListKind::$kind,
                        central_engine_model::EngineTime::UNIX_EPOCH,
                        1,
                    ).unwrap();
                    use $crate::$witness;
                    $witness!(witness, query)
                }};
            )+
        }

        macro_rules! select_timed_due_list {
            ($d emit:ident, $d input:expr) => {
                match $d input.kind() {
                    $(
                        $(#[$feature])*
                        DueListKind::$kind => $witness!($d emit, $d input),
                    )+
                }
            };
        }
    };
}

declare_timed_due_lists! {
    $;
    Wakes, wake_list, "../fleet-wakes/src/queries/read_due_wakes.sql";
    Schedules, schedule_list, "../fleet-schedules/src/queries/read_due_schedules.sql";
    DirectMessages, direct_message_list, "../fleet-mail/src/queries/read_due_direct_messages.sql";
    SystemExpiries, system_expiry_list, "../fleet-mail/src/queries/read_due_system_expiries.sql";
    #[cfg(feature = "test-support")]
    Probes, probe_list, "../collaboration-central-core/src/engine/queries/read_due_probes.sql";
}

__declare_timed_due_row!();

/// Reads an owner's due list and validates its storage types before FleetEngine dispatch.
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
    let select = select_timed_due_list!(runtime, query);
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
#[path = "central_due_queries_test.rs"]
mod tests;
