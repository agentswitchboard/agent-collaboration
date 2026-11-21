//! Runs core's statement batches and reads on a Durable Object's SQL storage.
//!
//! A batch runs inside `Storage::transaction`: every statement, its guard, and for
//! central's own object the `SetFromQuery` alarm statement and `setAlarm`, commit
//! together or roll back together. The store knows no business table.

use crate::store_wire::{OwnedAlarmUpdate, OwnedBatch, OwnedStatement};
use central_storage::{
    AlarmPublication, AlarmPublicationFailure, BatchOutcome, ConstraintKind, CorruptReason,
    EngineAlarmDecider, EngineAlarmTimes, ReadOutcome, SafeInteger, SqlValue, StoreFailure,
};
use fleet_runtime_ports::Clock;
use sqlx_core::column::Column;
use sqlx_core::error::ErrorKind;
use sqlx_core::migrate::Migrator;
use sqlx_core::query::{Query, query};
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_durable_object::{Do, DoArguments, DoConnection, DoRow, DoTransaction};
use sqlx_durable_object_core::{AsValue, Value};
use std::sync::Arc;

/// Whether a batch's `AlarmUpdate` reaches the object's alarm.
pub(crate) enum AlarmPolicy {
    /// The storage cases' object: `SetFromQuery` sets or clears its one alarm,
    /// through this handle on the same object's storage.
    #[cfg(feature = "test-support")]
    Apply(worker::Storage),
    Engine(
        worker::Storage,
        crate::worker_clock::WorkspaceClock,
        EngineAlarmDecider,
    ),
    /// A workspace store owns no alarm; the update is ignored.
    Ignore,
}

/// Why a batch's transaction rolled back.
enum BatchStop {
    GuardFailed { index: u32, rows_affected: u64 },
    ConstraintViolated { index: u32, kind: ConstraintKind },
    StatementRejected { index: u32 },
    Failed(StoreFailure),
}

impl From<sqlx_core::Error> for BatchStop {
    fn from(error: sqlx_core::Error) -> Self {
        Self::Failed(store_failure(&error))
    }
}

/// A database error that is not a constraint: the store could not do what it was asked.
fn store_failure(error: &sqlx_core::Error) -> StoreFailure {
    worker::console_warn!("statement store failure: {error}");
    StoreFailure::Unavailable
}

fn constraint_kind(error: &sqlx_core::Error) -> Option<ConstraintKind> {
    let sqlx_core::Error::Database(database) = error else {
        return None;
    };
    match database.kind() {
        ErrorKind::UniqueViolation => Some(ConstraintKind::Unique),
        ErrorKind::ForeignKeyViolation => Some(ConstraintKind::ForeignKey),
        ErrorKind::NotNullViolation => Some(ConstraintKind::NotNull),
        // Decision 31: CHECK constraints exist only on boolean columns.
        ErrorKind::CheckViolation => Some(ConstraintKind::CheckBoolean),
        _ => None,
    }
}

fn bound(statement: &OwnedStatement) -> Query<'static, Do, DoArguments> {
    statement.args.iter().fold(
        query(AssertSqlSafe(statement.sql.clone())),
        |query, argument| match argument {
            SqlValue::Null => query.bind(None::<i64>),
            SqlValue::Integer(value) => query.bind(value.get()),
            SqlValue::Text(value) => query.bind(value.clone()),
            SqlValue::Blob(value) => query.bind(value.clone()),
        },
    )
}

fn sql_value(value: &Value, column: &str) -> Result<SqlValue, StoreFailure> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        // The driver already refuses wider integers; a value outside ±(2^53−1) here
        // is still refused rather than trusted.
        Value::Integer(value) => SafeInteger::new(*value)
            .map(SqlValue::Integer)
            .map_err(|_| StoreFailure::Corrupt {
                reason: CorruptReason::BadValue {
                    column: column.to_owned(),
                },
            }),
        // REAL is outside the storage contract (no schema stores one).
        Value::Text(value) => Ok(SqlValue::Text(value.clone())),
        Value::Blob(value) => Ok(SqlValue::Blob(value.clone())),
        _ => Err(StoreFailure::Corrupt {
            reason: CorruptReason::UnknownKind {
                column: column.to_owned(),
            },
        }),
    }
}

fn read_outcome(rows: &[DoRow]) -> Result<ReadOutcome, StoreFailure> {
    let columns: Vec<String> = rows
        .first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect()
        })
        .unwrap_or_default();
    let rows = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    let raw = row.try_get_raw(index).map_err(|_| StoreFailure::Corrupt {
                        reason: CorruptReason::MissingColumn {
                            column: column.clone(),
                        },
                    })?;
                    sql_value(raw.as_value(), column)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ReadOutcome { columns, rows })
}

/// The alarm `SetFromQuery` asks for: one INTEGER Unix-millisecond value, or NULL.
fn alarm_time(outcome: &ReadOutcome) -> Result<Option<i64>, StoreFailure> {
    let column = outcome.columns.first().cloned().unwrap_or_default();
    let bad_value = || StoreFailure::Corrupt {
        reason: CorruptReason::BadValue {
            column: column.clone(),
        },
    };
    match outcome.rows.as_slice() {
        [row] => match row.as_slice() {
            [SqlValue::Integer(at)] => Ok(Some(at.get())),
            [SqlValue::Null] => Ok(None),
            _ => Err(bad_value()),
        },
        _ => Err(bad_value()),
    }
}

fn alarm_out_of_range(rows: &[DoRow]) -> StoreFailure {
    let column = rows
        .first()
        .and_then(|row| row.columns().first())
        .map(|column| column.name().to_owned())
        .unwrap_or_default();
    StoreFailure::Corrupt {
        reason: CorruptReason::BadValue { column },
    }
}

#[cfg(feature = "test-support")]
#[derive(Debug)]
struct AlarmInputExecutor<'a, E> {
    inner: E,
    dispatches: &'a std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "test-support")]
impl<'c, E: sqlx_core::executor::Executor<'c, Database = Do>> sqlx_core::executor::Executor<'c>
    for AlarmInputExecutor<'c, E>
{
    type Database = Do;

    fn execute<'e, 'q: 'e, Q>(
        self,
        query: Q,
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<sqlx_durable_object::DoQueryResult, sqlx_core::Error>,
    >
    where
        'c: 'e,
        Q: 'q + sqlx_core::executor::Execute<'q, Do>,
    {
        self.inner.execute(query)
    }

    fn fetch_many<'e, 'q: 'e, Q>(
        self,
        query: Q,
    ) -> futures_util::stream::BoxStream<
        'e,
        Result<sqlx_core::Either<sqlx_durable_object::DoQueryResult, DoRow>, sqlx_core::Error>,
    >
    where
        'c: 'e,
        Q: 'q + sqlx_core::executor::Execute<'q, Do>,
    {
        self.dispatches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, Q>(
        self,
        query: Q,
    ) -> futures_util::future::BoxFuture<'e, Result<Option<DoRow>, sqlx_core::Error>>
    where
        'c: 'e,
        Q: 'q + sqlx_core::executor::Execute<'q, Do>,
    {
        self.dispatches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.fetch_optional(query)
    }

    fn prepare_with<'e>(
        self,
        sql: sqlx_core::sql_str::SqlStr,
        parameters: &'e [sqlx_durable_object::DoTypeInfo],
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<sqlx_durable_object::DoStatement, sqlx_core::Error>,
    >
    where
        'c: 'e,
    {
        self.inner.prepare_with(sql, parameters)
    }

    fn describe<'e>(
        self,
        sql: sqlx_core::sql_str::SqlStr,
    ) -> futures_util::future::BoxFuture<
        'e,
        Result<sqlx_core::describe::Describe<Do>, sqlx_core::Error>,
    >
    where
        'c: 'e,
    {
        self.inner.describe(sql)
    }
}

pub(super) struct WorkerTransaction<'a, T> {
    inner: &'a T,
    #[cfg(feature = "test-support")]
    dispatches: std::sync::atomic::AtomicUsize,
}

impl<'a, T> WorkerTransaction<'a, T> {
    pub(super) fn new(inner: &'a T) -> Self {
        Self {
            inner,
            #[cfg(feature = "test-support")]
            dispatches: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl<T> fleet_engine::CentralTransaction for WorkerTransaction<'_, T>
where
    for<'executor> &'executor T: sqlx_core::executor::Executor<'executor, Database = Do>,
{
    type Database = Do;

    fn executor(&mut self) -> impl sqlx_core::executor::Executor<'_, Database = Do> {
        #[cfg(feature = "test-support")]
        return AlarmInputExecutor {
            inner: self.inner,
            dispatches: &self.dispatches,
        };
        #[cfg(not(feature = "test-support"))]
        self.inner
    }

    fn rows_affected(result: <Do as sqlx_core::database::Database>::QueryResult) -> u64 {
        result.rows_affected()
    }
}

pub(crate) async fn read_timed_due_rows(
    connection: &DoConnection,
    query: fleet_runtime_ports::DueListQuery,
) -> Result<Vec<fleet_runtime_ports::DueRow>, StoreFailure> {
    fleet_engine::read_timed_due_rows(&mut WorkerTransaction::new(connection), query).await
}

pub(crate) async fn engine_alarm_inputs(
    connection: &DoConnection,
) -> Result<EngineAlarmTimes, StoreFailure> {
    fleet_engine::read_alarm_inputs(&mut WorkerTransaction::new(connection)).await
}

async fn run_in_transaction(
    transaction: &DoTransaction,
    batch: OwnedBatch,
    policy: AlarmPolicy,
) -> Result<(Vec<u64>, AlarmPublication), BatchStop> {
    let mut rows_affected = Vec::with_capacity(batch.statements.len());
    for (index, guarded) in batch.statements.iter().enumerate() {
        let index =
            u32::try_from(index).map_err(|_| BatchStop::Failed(StoreFailure::Unavailable))?;
        let affected = match bound(&guarded.statement).execute(transaction).await {
            Ok(result) => result.rows_affected(),
            Err(error) => {
                return Err(match constraint_kind(&error) {
                    Some(kind) => BatchStop::ConstraintViolated { index, kind },
                    None if statement_is_rejected(&error) => BatchStop::StatementRejected { index },
                    None => BatchStop::Failed(store_failure(&error)),
                });
            }
        };
        if !guarded.expect.admits(affected) {
            return Err(BatchStop::GuardFailed {
                index,
                rows_affected: affected,
            });
        }
        rows_affected.push(affected);
    }
    Ok((
        rows_affected,
        apply_alarm(transaction, &batch.alarm, policy).await?,
    ))
}

async fn apply_alarm(
    transaction: &DoTransaction,
    alarm: &OwnedAlarmUpdate,
    policy: AlarmPolicy,
) -> Result<AlarmPublication, BatchStop> {
    let (storage, clock) = match policy {
        #[cfg(feature = "test-support")]
        AlarmPolicy::Apply(storage) => (Some(storage), None),
        AlarmPolicy::Engine(storage, clock, decide) => (Some(storage), Some((clock, decide))),
        AlarmPolicy::Ignore => (None, None),
    };
    let mut publication = AlarmPublication::Unchanged;
    if let Some(storage) = storage {
        let at = match alarm {
            OwnedAlarmUpdate::Unchanged => return Ok(publication),
            OwnedAlarmUpdate::SetFromQuery(statement) => {
                let rows = bound(statement).fetch_all(transaction).await?;
                let at = alarm_time(&read_outcome(&rows).map_err(BatchStop::Failed)?)
                    .map_err(BatchStop::Failed)?;
                if at.is_some_and(|at| chrono::DateTime::from_timestamp_millis(at).is_none()) {
                    return Err(BatchStop::Failed(alarm_out_of_range(&rows)));
                }
                at
            }
            OwnedAlarmUpdate::Engine(plan) => {
                let (clock, decide) =
                    clock.ok_or(BatchStop::Failed(StoreFailure::AlarmUnpublishable {
                        reason: AlarmPublicationFailure::ClockUnavailable,
                    }))?;
                let mut alarm_transaction = WorkerTransaction::new(transaction);
                let times = match plan {
                    central_storage::EngineAlarmPlan::Clear => EngineAlarmTimes {
                        earliest_on_grid_due_unix_millis: None,
                        earliest_legacy_due_unix_millis: None,
                    },
                    central_storage::EngineAlarmPlan::Recompute => {
                        fleet_engine::read_alarm_inputs(&mut alarm_transaction)
                            .await
                            .map_err(BatchStop::Failed)?
                    }
                };
                #[cfg(feature = "test-support")]
                worker::console_log!(
                    "Central alarm input SELECTs: {}",
                    alarm_transaction
                        .dispatches
                        .load(std::sync::atomic::Ordering::Relaxed)
                );
                decide(times, clock.now().unix_millis(), &mut |_| {}).map_err(|reason| {
                    BatchStop::Failed(StoreFailure::AlarmUnpublishable { reason })
                })?
            }
        };
        // A storage write in the same transaction: it commits with the batch or not at all.
        let written = match at {
            Some(at) => match chrono::DateTime::from_timestamp_millis(at) {
                Some(at) => storage.set_alarm(at).await,
                None => {
                    return Err(BatchStop::Failed(StoreFailure::AlarmUnpublishable {
                        reason: AlarmPublicationFailure::EarliestDueOutOfRange,
                    }));
                }
            },
            None => storage.delete_alarm().await,
        };
        written.map_err(|error| {
            worker::console_warn!("statement store alarm write failed: {error}");
            BatchStop::Failed(StoreFailure::Unavailable)
        })?;
        publication = at.map_or(AlarmPublication::Cleared, |unix_millis| {
            AlarmPublication::Set { unix_millis }
        });
    }
    Ok(publication)
}

/// Runs `batch` atomically on `connection`'s storage.
pub(crate) async fn run_batch(
    connection: &DoConnection,
    batch: OwnedBatch,
    policy: AlarmPolicy,
) -> Result<BatchOutcome, StoreFailure> {
    let outcome = connection
        .transaction(move |transaction| async move {
            run_in_transaction(&transaction, batch, policy).await
        })
        .await;
    match outcome {
        Ok((rows_affected, publication)) => Ok(BatchOutcome::Applied {
            rows_affected,
            publication,
        }),
        Err(BatchStop::GuardFailed {
            index,
            rows_affected,
        }) => Ok(BatchOutcome::GuardFailed {
            index,
            rows_affected,
        }),
        Err(BatchStop::ConstraintViolated { index, kind }) => {
            Ok(BatchOutcome::ConstraintViolated { index, kind })
        }
        Err(BatchStop::Failed(failure)) => Err(failure),
        Err(BatchStop::StatementRejected { index }) => {
            Ok(BatchOutcome::StatementRejected { index })
        }
    }
}

/// A due unit and its later attempt charge each use an independent transaction.
pub(super) async fn write_checked_due<Answer: 'static, Rejected: 'static>(
    connection: &DoConnection,
    apply: impl AsyncFnOnce(&DoTransaction) -> Result<Answer, Rejected> + 'static,
) -> Result<std::ops::ControlFlow<Rejected, Answer>, fleet_engine::CheckedDueFailure> {
    use std::ops::ControlFlow;

    enum CheckedDueStop<Rejected> {
        Callback(Rejected),
        Commit,
    }
    impl<Rejected> From<sqlx_core::Error> for CheckedDueStop<Rejected> {
        fn from(_: sqlx_core::Error) -> Self {
            Self::Commit
        }
    }
    match connection
        .transaction(move |transaction| async move {
            apply(&transaction).await.map_err(CheckedDueStop::Callback)
        })
        .await
    {
        Ok(answer) => Ok(ControlFlow::Continue(answer)),
        Err(CheckedDueStop::Callback(rejection)) => Ok(ControlFlow::Break(rejection)),
        Err(CheckedDueStop::Commit) => Err(fleet_engine::CheckedDueFailure::AnswerLost),
    }
}

/// The same owning alarm step as a guarded batch, after the portable mail operation.
pub(super) async fn write_direct_with_engine_alarm<Answer: 'static>(
    connection: &DoConnection,
    policy: AlarmPolicy,
    apply: impl AsyncFnOnce(
        &DoTransaction,
    ) -> Result<
        fleet_mail::direct_store_writes::DirectApplied<Answer>,
        fleet_mail::direct_store_writes::ApplyingFailure,
    > + 'static,
) -> Result<Answer, fleet_mail::DirectWriteFailure> {
    use fleet_mail::direct_store_writes::ApplyingFailure;

    enum MailTransactionAbort {
        Applying(ApplyingFailure),
        Commit,
    }
    impl From<sqlx_core::Error> for MailTransactionAbort {
        fn from(_: sqlx_core::Error) -> Self {
            Self::Commit
        }
    }
    connection
        .transaction(move |transaction| async move {
            let applied = apply(&transaction)
                .await
                .map_err(MailTransactionAbort::Applying)?;
            if let Some(plan) = applied.alarm {
                apply_alarm(&transaction, &OwnedAlarmUpdate::Engine(plan), policy)
                    .await
                    .map_err(|stop| match stop {
                        BatchStop::Failed(failure) => ApplyingFailure::Store(failure),
                        BatchStop::GuardFailed { .. }
                        | BatchStop::ConstraintViolated { .. }
                        | BatchStop::StatementRejected { .. } => ApplyingFailure::Conflict,
                    })
                    .map_err(MailTransactionAbort::Applying)?;
            }
            Ok(applied.outcome)
        })
        .await
        .map_err(|failure| match failure {
            MailTransactionAbort::Applying(failure) => failure.into_write_failure(),
            MailTransactionAbort::Commit => fleet_mail::DirectWriteFailure::AnswerLost,
        })
}

/// Runs one read outside any write.
pub(crate) async fn read(
    connection: &DoConnection,
    statement: OwnedStatement,
) -> Result<ReadOutcome, StoreFailure> {
    let rows = bound(&statement)
        .fetch_all(connection)
        .await
        .map_err(|error| store_failure(&error))?;
    read_outcome(&rows)
}

/// Why opening an object's store failed; its transaction rolled back.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct MigrationFailure(String);

/// Runs `migrator` inside one storage transaction: all pending migrations and their
/// receipts commit together, or none do.
pub(crate) async fn migrate(
    connection: &DoConnection,
    migrator: Arc<Migrator>,
) -> Result<(), MigrationFailure> {
    connection
        .transaction(move |transaction| async move {
            sqlx_durable_object::migrate::run_migrations(&transaction, &migrator)
                .await
                .map(|_| ())
                .map_err(|error| MigrationFailure(error.to_string()))
        })
        .await
}

impl From<sqlx_core::Error> for MigrationFailure {
    fn from(error: sqlx_core::Error) -> Self {
        Self(error.to_string())
    }
}

#[cfg(test)]
#[path = "statement_store_test.rs"]
mod tests;

fn statement_is_rejected(error: &sqlx_core::Error) -> bool {
    let sqlx_core::Error::Database(database) = error else {
        return false;
    };
    matches!(
        database.code().as_deref(),
        Some(
            "SQLITE_CONSTRAINT"
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
}
