//! Diagnostic comparisons run on a caller's consistent view of one live store.
use super::{StoredAlarmInputs, map_central_query_failure, read_due_list};
use crate::{DueKind, DueLane, read_alarm_inputs, read_timed_due_rows};
use central_engine_model::{
    AlarmDecision, AlarmInputs, DueBatchPolicy, DueListKind, DueRow, EngineTime,
    MAX_ALARM_UNIX_MILLIS, due_list_query, next_alarm, supported_time,
};
use central_storage::{EngineAlarmTimes, StoreFailure};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::stored_values::format_utc_millis;
use sqlx::{ColumnIndex, Database, Decode, Encode, IntoArguments, Type};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DueTableDifference {
    MissingItem {
        kind: DueKind,
        reference: DueRow,
    },
    ExtraItem {
        kind: DueKind,
        projected: DueRow,
    },
    WrongItemTime {
        kind: DueKind,
        reference: DueRow,
        projected: DueRow,
    },
    WrongDispatch {
        kind: DueKind,
        reference: Vec<DueRow>,
        projected: Vec<DueRow>,
    },
    MissingSource {
        kind: DueKind,
        reference: Option<i64>,
    },
    ExtraSource {
        kind: DueKind,
        projected: Option<i64>,
    },
    WrongSourceTime {
        kind: DueKind,
        reference: Option<i64>,
        projected: Option<i64>,
    },
    InvalidSource {
        kind: String,
        key: String,
    },
    NonCanonicalLegacyTime {
        kind: DueKind,
        key: String,
        stored_time: String,
    },
    WrongAlarmInputs {
        reference: EngineAlarmTimes,
        projected: EngineAlarmTimes,
    },
    WrongAlarmDecision {
        reference: AlarmDecision,
        projected: AlarmDecision,
    },
}

#[derive(sqlx::FromRow)]
struct StoredSource {
    #[sqlx(rename = "kind!: String")]
    kind: String,
    #[sqlx(rename = "key!: String")]
    key: String,
    #[sqlx(rename = "effective_due_ms?: i64")]
    effective_due_ms: Option<i64>,
    #[sqlx(rename = "is_reference!: i64")]
    is_reference: i64,
}

#[derive(sqlx::FromRow)]
struct LegacyTime {
    #[sqlx(rename = "kind!: String")]
    kind: String,
    #[sqlx(rename = "key!: String")]
    key: String,
    #[sqlx(rename = "stored_time!: String")]
    stored_time: String,
}

macro_rules! comparison_queries {
    ($emit:ident) => {
        (
            $emit!(StoredAlarmInputs, "src/queries/read_alarm_inputs.sql"),
            $emit!(StoredSource, "src/queries/compare_due_sources.sql"),
            $emit!(LegacyTime, "src/queries/compare_legacy_times.sql"),
        )
    };
}

/// Compare every due item and source control, the capped dispatch order, and
/// the resulting alarm decision. Run on a read snapshot, outside event dispatch.
pub async fn compare_due_table<T: CentralTransaction>(
    transaction: &mut T,
    horizon: EngineTime,
    cap: usize,
    alarm_inputs: AlarmInputs,
    policy: &DueBatchPolicy,
) -> Result<Vec<DueTableDifference>, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let mut differences = Vec::new();
    let max_time = EngineTime::from_timestamp_millis(MAX_ALARM_UNIX_MILLIS)
        .ok_or(StoreFailure::Unavailable)?;
    for (kind, list_kind) in [
        (DueKind::WakeTimer, DueListKind::Wakes),
        (DueKind::AutomationTimer, DueListKind::Schedules),
        (DueKind::DirectMessage, DueListKind::DirectMessages),
        (DueKind::SystemMessageExpiry, DueListKind::SystemExpiries),
    ] {
        let all = due_list_query(list_kind, max_time, i32::MAX as usize)
            .map_err(|_| StoreFailure::Unavailable)?;
        let reference = read_due_list(transaction, all, None).await?;
        let projected = read_timed_due_rows(transaction, all).await?;
        compare_items(kind, &reference, &projected, &mut differences);

        let query =
            due_list_query(list_kind, horizon, cap).map_err(|_| StoreFailure::Unavailable)?;
        let reference = read_due_list(transaction, query, None).await?;
        let projected = read_timed_due_rows(transaction, query).await?;
        if reference != projected {
            differences.push(DueTableDifference::WrongDispatch {
                kind,
                reference,
                projected,
            });
        }
    }

    macro_rules! query_as {
        ($row:ty, $file:literal) => {
            sqlx::query_as::<T::Database, $row>(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/",
                $file
            )))
        };
    }
    let (alarm_query, sources_query, legacy_query) = comparison_queries!(query_as);
    let alarm = alarm_query
        .fetch_one(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "reference alarm inputs"))?;
    let reference = EngineAlarmTimes {
        earliest_on_grid_due_unix_millis: alarm.on_grid_due,
        earliest_legacy_due_unix_millis: alarm.legacy_due,
    };
    let projected = read_alarm_inputs(transaction).await?;
    if projected != reference {
        differences.push(DueTableDifference::WrongAlarmInputs {
            reference,
            projected,
        });
    }
    let reference = alarm_decision(alarm_inputs, reference, policy);
    let projected = alarm_decision(alarm_inputs, projected, policy);
    if projected != reference {
        differences.push(DueTableDifference::WrongAlarmDecision {
            reference,
            projected,
        });
    }

    let sources = sources_query
        .fetch_all(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "due source comparison"))?;
    compare_sources(sources, &mut differences);

    let legacy_times = legacy_query
        .fetch_all(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "legacy time comparison"))?;
    for LegacyTime {
        kind,
        key,
        stored_time,
    } in legacy_times
    {
        let kind = DueKind::from_tag(&kind).ok_or(StoreFailure::Unavailable)?;
        let canonical = stored_time
            .parse::<EngineTime>()
            .is_ok_and(|parsed| supported_time(parsed) && format_utc_millis(parsed) == stored_time);
        if !canonical {
            differences.push(DueTableDifference::NonCanonicalLegacyTime {
                kind,
                key,
                stored_time,
            });
        }
    }
    Ok(differences)
}

fn compare_items(
    kind: DueKind,
    reference: &[DueRow],
    projected: &[DueRow],
    differences: &mut Vec<DueTableDifference>,
) {
    let mut projected: BTreeMap<_, _> = projected.iter().map(|row| (&row.key.0, row)).collect();
    for reference in reference {
        match projected.remove(&reference.key.0) {
            None => differences.push(DueTableDifference::MissingItem {
                kind,
                reference: reference.clone(),
            }),
            Some(projected) if projected != reference => {
                differences.push(DueTableDifference::WrongItemTime {
                    kind,
                    reference: reference.clone(),
                    projected: projected.clone(),
                })
            }
            Some(_) => {}
        }
    }
    differences.extend(
        projected
            .into_values()
            .map(|row| DueTableDifference::ExtraItem {
                kind,
                projected: row.clone(),
            }),
    );
}

fn compare_sources(rows: Vec<StoredSource>, differences: &mut Vec<DueTableDifference>) {
    let mut sources = [
        DueKind::WakeTimer,
        DueKind::AutomationTimer,
        DueKind::DirectMessage,
        DueKind::SystemMessageExpiry,
        DueKind::LegacyNoticeWindows,
        DueKind::LegacyWorkspaceDrains,
    ]
    .map(|kind| (kind, None, None));
    for StoredSource {
        kind,
        key,
        effective_due_ms,
        is_reference,
    } in rows
    {
        let resolved = match is_reference {
            1 if kind == "engine_source" => {
                DueKind::from_tag(&key).filter(|kind| kind.lane() == DueLane::OnGrid)
            }
            1 if key == "all" => {
                DueKind::from_tag(&kind).filter(|kind| kind.lane() == DueLane::Legacy)
            }
            0 if kind == key => DueKind::from_tag(&kind),
            _ => None,
        };
        if let Some((_, reference, projected)) = sources
            .iter_mut()
            .find(|(kind, _, _)| Some(*kind) == resolved)
        {
            if is_reference == 1 {
                *reference = Some(effective_due_ms);
            } else {
                *projected = Some(effective_due_ms);
            }
        } else if is_reference == 0 {
            differences.push(DueTableDifference::InvalidSource { kind, key });
        }
    }
    for (kind, reference, projected) in sources {
        match (reference, projected) {
            (Some(reference), None) => {
                differences.push(DueTableDifference::MissingSource { kind, reference })
            }
            (None, Some(projected)) => {
                differences.push(DueTableDifference::ExtraSource { kind, projected })
            }
            (Some(reference), Some(projected)) if reference != projected => {
                differences.push(DueTableDifference::WrongSourceTime {
                    kind,
                    reference,
                    projected,
                });
            }
            _ => {}
        }
    }
}

fn alarm_decision(
    mut inputs: AlarmInputs,
    times: EngineAlarmTimes,
    policy: &DueBatchPolicy,
) -> AlarmDecision {
    inputs.earliest_on_grid_due = times
        .earliest_on_grid_due_unix_millis
        .and_then(EngineTime::from_timestamp_millis);
    inputs.earliest_legacy_due = times
        .earliest_legacy_due_unix_millis
        .and_then(EngineTime::from_timestamp_millis);
    next_alarm(&inputs, policy)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "due_table_comparison_test.rs"]
mod tests;
