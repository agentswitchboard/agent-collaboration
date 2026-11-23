//! Apply the engine's alarm choice in the Workerd storage transaction.
use super::{AlarmPolicy, BatchStop, WorkerExecutorLender, bound, read_outcome};
use crate::store_wire::OwnedAlarmUpdate;
use central_storage::{AlarmPublication, AlarmPublicationFailure, EngineAlarmPlan, StoreFailure};
use fleet_runtime_ports::Clock;
use sqlx_durable_object::DoTransaction;

use central_storage::{CorruptReason, ReadOutcome, SqlValue};
use sqlx_core::{column::Column, row::Row};
use sqlx_durable_object::DoRow;

pub(super) async fn apply_alarm(
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
            OwnedAlarmUpdate::Engine(EngineAlarmPlan::Clear) => None,
            OwnedAlarmUpdate::Engine(EngineAlarmPlan::Recompute) => {
                let (clock, decide) =
                    clock.ok_or(BatchStop::Failed(StoreFailure::AlarmUnpublishable {
                        reason: AlarmPublicationFailure::ClockUnavailable,
                    }))?;
                let mut alarm_executor_lender =
                    WorkerExecutorLender::from_storage_executor(transaction);
                let alarm_inputs = fleet_engine::read_alarm_inputs(&mut alarm_executor_lender)
                    .await
                    .map_err(BatchStop::Failed)?;
                let retained = storage
                    .get_alarm()
                    .await
                    .map_err(|_| BatchStop::Failed(StoreFailure::Unavailable))?;
                #[cfg(feature = "test-support")]
                worker::console_log!(
                    "Central alarm input SELECTs: {}",
                    alarm_executor_lender
                        .read_dispatches
                        .load(std::sync::atomic::Ordering::Relaxed)
                );
                let decided = decide(
                    alarm_inputs,
                    clock.now().unix_millis(),
                    &|| retained,
                    &mut |_| {},
                )
                .map_err(|reason| BatchStop::Failed(StoreFailure::AlarmUnpublishable { reason }))?;
                if let Some(unix_millis) = decided.filter(|mark| Some(*mark) == retained) {
                    return Ok(AlarmPublication::Set { unix_millis });
                }
                decided
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

/// The alarm `SetFromQuery` asks for: one INTEGER Unix-millisecond value, or NULL.
pub(super) fn alarm_time(outcome: &ReadOutcome) -> Result<Option<i64>, StoreFailure> {
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
