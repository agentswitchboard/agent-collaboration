//! Checked Central alarm and timed-list rows read on the owning executor.
use crate::DueKind;
use central_engine_model::{DueListKind, DueListQuery, DueRow, InstanceKey};
use central_storage::{CorruptReason, EngineAlarmTimes, SafeInteger, StoreFailure};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::EngineDueTimes;
use sqlx::{ColumnIndex, Database, Decode, Encode, IntoArguments, Type};

#[derive(sqlx::FromRow)]
struct StoredAlarmInputs {
    #[sqlx(rename = "on_grid_due?: i64")]
    on_grid_due: Option<i64>,
    #[sqlx(rename = "legacy_due?: i64")]
    legacy_due: Option<i64>,
}

#[derive(Default, sqlx::FromRow)]
struct StoredIndexedAlarmInputs {
    #[sqlx(rename = "on_grid_due?: i64")]
    on_grid_due: Option<i64>,
    #[sqlx(rename = "notice_window_due?: i64")]
    notice_window_due: Option<i64>,
    #[sqlx(rename = "workspace_drain_due?: i64")]
    workspace_drain_due: Option<i64>,
    #[sqlx(rename = "look_ahead_due?: i64")]
    look_ahead_due: Option<i64>,
    #[sqlx(rename = "exact_due?: i64")]
    exact_due: Option<i64>,
    #[sqlx(rename = "approval_due?: i64")]
    approval_due: Option<i64>,
}

struct IndexedAlarmSnapshot {
    times: EngineAlarmTimes,
    due: EngineDueTimes,
    legacy_by_kind: [(DueKind, Option<i64>); 2],
}

macro_rules! indexed_alarm_query {
    ($emit:ident, $tags:expr) => {
        $emit!("src/queries/read_indexed_alarm_inputs.sql", $tags)
    };
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
#[macro_export]
macro_rules! check_fixture_alarm_query {
    ($emit:ident) => {
        $emit!("../collaboration-central-core/src/engine/queries/read_fixture_alarm_inputs.sql")
    };
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
#[macro_export]
macro_rules! check_fixture_alarm_presence {
    ($emit:ident) => {
        $emit!("../collaboration-central-core/src/engine/queries/read_fixture_alarm_presence.sql")
    };
}

#[cfg(feature = "test-support")]
#[derive(sqlx::FromRow)]
struct StoredFixturePresence {
    #[sqlx(rename = "installed!: i64")]
    installed: i64,
}

fn grid_tags() -> Result<String, StoreFailure> {
    serde_json::to_string(&DueKind::GRID.map(DueKind::tag)).map_err(|_| StoreFailure::Unavailable)
}

pub async fn read_alarm_inputs<CentralTransactionLender: CentralTransaction>(
    transaction: &mut CentralTransactionLender,
) -> Result<EngineAlarmTimes, StoreFailure>
where
    <CentralTransactionLender::Database as Database>::Arguments:
        IntoArguments<CentralTransactionLender::Database>,
    String: Type<CentralTransactionLender::Database>
        + for<'q> Encode<'q, CentralTransactionLender::Database>,
    i64: Type<CentralTransactionLender::Database>
        + for<'row> Decode<'row, CentralTransactionLender::Database>,
    for<'row> &'row str: ColumnIndex<<CentralTransactionLender::Database as Database>::Row>,
{
    Ok(read_alarm_snapshot(transaction).await?.times)
}

pub async fn read_engine_due_times<CentralTransactionLender: CentralTransaction>(
    transaction: &mut CentralTransactionLender,
) -> Result<EngineDueTimes, StoreFailure>
where
    <CentralTransactionLender::Database as Database>::Arguments:
        IntoArguments<CentralTransactionLender::Database>,
    String: Type<CentralTransactionLender::Database>
        + for<'q> Encode<'q, CentralTransactionLender::Database>,
    i64: Type<CentralTransactionLender::Database>
        + for<'row> Decode<'row, CentralTransactionLender::Database>,
    for<'row> &'row str: ColumnIndex<<CentralTransactionLender::Database as Database>::Row>,
{
    Ok(read_alarm_snapshot(transaction).await?.due)
}

async fn read_alarm_snapshot<CentralTransactionLender: CentralTransaction>(
    transaction: &mut CentralTransactionLender,
) -> Result<IndexedAlarmSnapshot, StoreFailure>
where
    <CentralTransactionLender::Database as Database>::Arguments:
        IntoArguments<CentralTransactionLender::Database>,
    String: Type<CentralTransactionLender::Database>
        + for<'q> Encode<'q, CentralTransactionLender::Database>,
    i64: Type<CentralTransactionLender::Database>
        + for<'row> Decode<'row, CentralTransactionLender::Database>,
    for<'row> &'row str: ColumnIndex<<CentralTransactionLender::Database as Database>::Row>,
{
    macro_rules! bind_indexed_alarm {
        ($file:literal, $tags:expr) => {
            sqlx::query_as::<CentralTransactionLender::Database, StoredIndexedAlarmInputs>(
                include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
            )
            .bind($tags)
        };
    }
    let row = indexed_alarm_query!(bind_indexed_alarm, grid_tags()?)
        .fetch_optional(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "engine alarm input"))?
        .unwrap_or_default();
    let on_grid_due = row.on_grid_due;
    let look_ahead_due = row.look_ahead_due;
    #[cfg(feature = "test-support")]
    let (on_grid_due, look_ahead_due) = {
        macro_rules! bind_fixture_alarm {
            ($file:literal) => {
                sqlx::query_as::<CentralTransactionLender::Database, StoredAlarmInputs>(
                    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
                )
            };
        }
        macro_rules! bind_presence {
            ($file:literal) => {
                sqlx::query_as::<CentralTransactionLender::Database, StoredFixturePresence>(
                    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
                )
            };
        }
        let installed = crate::check_fixture_alarm_presence!(bind_presence)
            .fetch_one(transaction.executor())
            .await
            .map_err(|error| map_central_query_failure(error, "engine fixture alarm schema"))?;
        if installed.installed == 1 {
            let fixture = crate::check_fixture_alarm_query!(bind_fixture_alarm)
                .fetch_one(transaction.executor())
                .await
                .map_err(|error| map_central_query_failure(error, "engine fixture alarm input"))?;
            (
                [on_grid_due, fixture.on_grid_due]
                    .into_iter()
                    .flatten()
                    .min(),
                [look_ahead_due, fixture.on_grid_due]
                    .into_iter()
                    .flatten()
                    .min(),
            )
        } else {
            (on_grid_due, look_ahead_due)
        }
    };
    let legacy_by_kind = [
        (
            DueKind::LegacyNoticeWindows,
            validate_optional_alarm_millis(row.notice_window_due, "notice window alarm input")?,
        ),
        (
            DueKind::LegacyWorkspaceDrains,
            validate_optional_alarm_millis(row.workspace_drain_due, "workspace drain alarm input")?,
        ),
    ];
    let times = EngineAlarmTimes {
        earliest_on_grid_due_unix_millis: validate_optional_alarm_millis(
            on_grid_due,
            "engine alarm input",
        )?,
        earliest_legacy_due_unix_millis: legacy_by_kind.iter().filter_map(|(_, due)| *due).min(),
    };
    Ok(IndexedAlarmSnapshot {
        times,
        due: EngineDueTimes {
            look_ahead_due_ms: validate_optional_alarm_millis(
                look_ahead_due,
                "look-ahead due input",
            )?,
            exact_due_ms: [row.exact_due, times.earliest_legacy_due_unix_millis]
                .into_iter()
                .flatten()
                .min()
                .map(|due| validate_optional_alarm_millis(Some(due), "exact due input"))
                .transpose()?
                .flatten(),
            approval_due_ms: validate_optional_alarm_millis(
                row.approval_due,
                "approval due input",
            )?,
        },
        legacy_by_kind,
    })
}

// The native query witness and the runtime decoder expand the same row declaration.
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

/// Declares the reference statements and their owning query witnesses.
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
                    macro_rules! check_timed_due_query {
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
                    $witness!(check_timed_due_query, query)
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
    ApprovalExpiries, approval_expiry_list, "../fleet-approvals/src/queries/read_approval_deadline_reference.sql";
    #[cfg(feature = "test-support")]
    Probes, probe_list, "../collaboration-central-core/src/engine/queries/read_due_probes.sql";
}

__declare_timed_due_row!();

macro_rules! projected_due_query {
    ($emit:ident, $kind:expr, $horizon:expr, $cap:expr, $retry_cutoff:expr) => {
        $emit!(
            "src/queries/read_projected_due_items.sql",
            $kind,
            $horizon,
            $cap,
            $retry_cutoff
        )
    };
}

/// Reads due rows in dispatch order from the Engine index.
pub async fn read_timed_due_rows<CentralTransactionLender: CentralTransaction>(
    transaction: &mut CentralTransactionLender,
    query: DueListQuery,
) -> Result<Vec<DueRow>, StoreFailure>
where
    <CentralTransactionLender::Database as Database>::Arguments:
        IntoArguments<CentralTransactionLender::Database>,
    i64: Type<CentralTransactionLender::Database>
        + for<'value> Encode<'value, CentralTransactionLender::Database>
        + for<'row> Decode<'row, CentralTransactionLender::Database>,
    String: Type<CentralTransactionLender::Database>
        + for<'q> Encode<'q, CentralTransactionLender::Database>
        + for<'row> Decode<'row, CentralTransactionLender::Database>,
    for<'row> &'row str: ColumnIndex<<CentralTransactionLender::Database as Database>::Row>,
{
    let kind = match query.kind() {
        DueListKind::Wakes => DueKind::WakeTimer,
        DueListKind::Schedules => DueKind::AutomationTimer,
        DueListKind::DirectMessages => DueKind::DirectMessage,
        DueListKind::SystemExpiries => DueKind::SystemMessageExpiry,
        DueListKind::ApprovalExpiries => DueKind::ApprovalRequestExpiry,
        #[cfg(feature = "test-support")]
        DueListKind::Probes => return read_due_list(transaction, query, None).await,
    };
    read_due_list(transaction, query, Some(kind)).await
}

async fn read_due_list<T: CentralTransaction>(
    transaction: &mut T,
    query: DueListQuery,
    projection: Option<DueKind>,
) -> Result<Vec<DueRow>, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    macro_rules! build_timed_due_query {
        ($file:literal, $($arg:expr),+) => {
            sqlx::query_as::<T::Database, StoredDueRow>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))
                $(.bind($arg))+
        };
    }
    let timed_due_query = match projection {
        Some(kind) => {
            let (horizon, cap) = query.bindings();
            projected_due_query!(
                build_timed_due_query,
                kind.tag().to_owned(),
                horizon,
                cap,
                query.retry_cutoff()
            )
        }
        None => select_timed_due_list!(build_timed_due_query, query),
    };
    timed_due_query
        .fetch_all(transaction.executor())
        .await
        .map_err(map_due_list_read_failure)?
        .into_iter()
        .map(|stored| {
            let subject_key = stored.subject_key.ok_or_else(|| StoreFailure::Corrupt {
                reason: CorruptReason::BadValue {
                    column: "subject_key".into(),
                },
            })?;
            Ok(DueRow {
                key: InstanceKey(subject_key),
                occurrence_ms: validate_sql_safe_integer(stored.occurrence_ms, "occurrence_ms")?,
                effective_due_ms: validate_sql_safe_integer(
                    stored.effective_due_ms,
                    "effective_due_ms",
                )?,
            })
        })
        .collect()
}

fn map_due_list_read_failure(error: sqlx::Error) -> StoreFailure {
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
        error => map_central_query_failure(error, "engine.due_list"),
    }
}

fn validate_optional_alarm_millis(
    time: Option<i64>,
    column: &str,
) -> Result<Option<i64>, StoreFailure> {
    time.map(|value| validate_sql_safe_integer(value, column))
        .transpose()
}

fn validate_sql_safe_integer(value: i64, column: &str) -> Result<i64, StoreFailure> {
    SafeInteger::new(value)
        .map(|value| value.get())
        .map_err(|_| StoreFailure::Corrupt {
            reason: CorruptReason::BadValue {
                column: column.into(),
            },
        })
}

fn map_central_query_failure(error: sqlx::Error, column: &str) -> StoreFailure {
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

#[path = "due_table_comparison.rs"]
mod comparison;
pub use comparison::{DueItemTimes, DueTableDifference, InvalidDueEntry, compare_due_table};
