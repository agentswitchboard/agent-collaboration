//! Diagnostic comparisons run on a caller's consistent view of one live store.
use super::{StoredAlarmInputs, map_central_query_failure, read_alarm_snapshot, read_due_list};
use crate::{DueKind, DueLane, DueMillis, read_timed_due_rows};
use central_engine_model::{
    AlarmDecision, AlarmInputs, DueBatchPolicy, DueListKind, DueRow, EngineTime, InstanceKey,
    due_list_query, next_alarm, supported_time,
};
use central_storage::{EngineAlarmTimes, StoreFailure};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::stored_values::format_utc_millis;
use sqlx::{ColumnIndex, Database, Decode, Encode, IntoArguments, Type};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DueItemTimes {
    pub key: InstanceKey,
    pub occurrence_ms: i64,
    pub base_due_ms: i64,
    pub effective_due_ms: Option<i64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidDueEntry {
    StorageType,
    EntryId,
    Kind,
    Role,
    LegacyItem,
    SubjectKey,
    TimingShape,
    TimingRange,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DueTableDifference {
    MissingItem {
        kind: DueKind,
        reference: DueItemTimes,
    },
    ExtraItem {
        kind: DueKind,
        projected: DueItemTimes,
    },
    WrongItemTime {
        kind: DueKind,
        reference: DueItemTimes,
        projected: DueItemTimes,
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
    InvalidEntry {
        id: String,
        kind: String,
        role: String,
        key: String,
        reason: InvalidDueEntry,
    },
    InvalidOwnerKey {
        kind: DueKind,
        key: Option<String>,
    },
    NonCanonicalLegacyTime {
        kind: DueKind,
        key: String,
        stored_time: String,
    },
    WrongLegacyContribution {
        kind: DueKind,
        reference: Option<i64>,
        projected: Option<i64>,
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

#[derive(Default, sqlx::FromRow)]
struct ReferenceLegacyInputs {
    #[sqlx(rename = "notice_window_due?: i64")]
    notice_window_due: Option<i64>,
    #[sqlx(rename = "workspace_drain_due?: i64")]
    workspace_drain_due: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ReferenceApprovalAlarmInput {
    #[sqlx(rename = "approval_due_ms?: i64")]
    approval_due_ms: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct StoredSource {
    #[sqlx(rename = "kind!: String")]
    kind: String,
    #[sqlx(rename = "key!: String")]
    key: String,
    #[sqlx(rename = "effective_due_ms?: i64")]
    effective_due_ms: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct StoredProjection {
    #[sqlx(rename = "id!: String")]
    id: String,
    #[sqlx(rename = "kind!: String")]
    kind: String,
    #[sqlx(rename = "role!: String")]
    role: String,
    #[sqlx(rename = "key!: String")]
    key: String,
    #[sqlx(rename = "occurrence_ms?: i64")]
    occurrence_ms: Option<i64>,
    #[sqlx(rename = "base_due_ms?: i64")]
    base_due_ms: Option<i64>,
    #[sqlx(rename = "effective_due_ms?: i64")]
    effective_due_ms: Option<i64>,
    #[sqlx(rename = "valid_storage!: i64")]
    valid_storage: i64,
}

#[derive(sqlx::FromRow)]
struct OwnerItem {
    #[sqlx(rename = "kind!: String")]
    kind: String,
    #[sqlx(rename = "key?: String")]
    key: Option<String>,
    #[sqlx(rename = "occurrence_ms!: i64")]
    occurrence_ms: i64,
    #[sqlx(rename = "base_due_ms!: i64")]
    base_due_ms: i64,
    #[sqlx(rename = "effective_due_ms?: i64")]
    effective_due_ms: Option<i64>,
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
            $emit!(
                ReferenceApprovalAlarmInput,
                "src/queries/read_approval_expiry_alarm_input.sql"
            ),
            $emit!(
                ReferenceLegacyInputs,
                "src/queries/compare_legacy_alarm_inputs.sql"
            ),
            $emit!(StoredSource, "src/queries/compare_due_sources.sql"),
            $emit!(LegacyTime, "src/queries/compare_legacy_times.sql"),
            $emit!(StoredProjection, "src/queries/read_due_projection.sql"),
            $emit!(OwnerItem, "src/queries/compare_owner_due_items.sql"),
        )
    };
}

#[derive(Default)]
struct Projection {
    items: BTreeMap<(DueKind, String), DueItemTimes>,
    sources: BTreeMap<DueKind, Option<i64>>,
}

enum ProjectionEntry {
    Item(DueKind, DueItemTimes),
    SourceControl(DueKind, Option<i64>),
}

impl StoredProjection {
    fn entry(&self) -> Result<ProjectionEntry, InvalidDueEntry> {
        use InvalidDueEntry as Invalid;
        if self.valid_storage != 1 {
            return Err(Invalid::StorageType);
        }
        if !uuid_v7(&self.id) {
            return Err(Invalid::EntryId);
        }
        let kind = DueKind::from_tag(&self.kind).ok_or(Invalid::Kind)?;
        if [self.occurrence_ms, self.base_due_ms, self.effective_due_ms]
            .into_iter()
            .flatten()
            .any(|time| DueMillis::try_from(time).is_err())
        {
            return Err(Invalid::TimingRange);
        }
        match self.role.as_str() {
            "item" => {
                if kind.lane() != DueLane::OnGrid {
                    return Err(Invalid::LegacyItem);
                }
                if !uuid_v7(&self.key) {
                    return Err(Invalid::SubjectKey);
                }
                let (Some(occurrence_ms), Some(base_due_ms)) =
                    (self.occurrence_ms, self.base_due_ms)
                else {
                    return Err(Invalid::TimingShape);
                };
                Ok(ProjectionEntry::Item(
                    kind,
                    DueItemTimes {
                        key: InstanceKey(self.key.clone()),
                        occurrence_ms,
                        base_due_ms,
                        effective_due_ms: self.effective_due_ms,
                    },
                ))
            }
            "source_control" => {
                if self.key != kind.tag() {
                    return Err(Invalid::SubjectKey);
                }
                if self.occurrence_ms.is_some() || self.base_due_ms.is_some() {
                    return Err(Invalid::TimingShape);
                }
                Ok(ProjectionEntry::SourceControl(kind, self.effective_due_ms))
            }
            _ => Err(Invalid::Role),
        }
    }
}

fn uuid_v7(value: &str) -> bool {
    uuid::Uuid::parse_str(value)
        .is_ok_and(|id| id.get_version_num() == 7 && id.to_string() == value)
}

impl Projection {
    fn decode(rows: Vec<StoredProjection>, differences: &mut Vec<DueTableDifference>) -> Self {
        let mut decoded = Self::default();
        for row in rows {
            match row.entry() {
                Ok(ProjectionEntry::Item(kind, item)) => {
                    decoded.items.insert((kind, row.key), item);
                }
                Ok(ProjectionEntry::SourceControl(kind, effective)) => {
                    decoded.sources.insert(kind, effective);
                }
                Err(reason) => differences.push(DueTableDifference::InvalidEntry {
                    id: row.id,
                    kind: row.kind,
                    role: row.role,
                    key: row.key,
                    reason,
                }),
            }
        }
        decoded
    }

    fn compare_items(
        mut self,
        owners: Vec<OwnerItem>,
        differences: &mut Vec<DueTableDifference>,
    ) -> BTreeMap<DueKind, Option<i64>> {
        for owner in owners {
            let Some(kind) = DueKind::from_tag(&owner.kind) else {
                continue;
            };
            let Some(key) = owner.key.as_ref().filter(|key| uuid_v7(key)).cloned() else {
                differences.push(DueTableDifference::InvalidOwnerKey {
                    kind,
                    key: owner.key,
                });
                continue;
            };
            let reference = DueItemTimes {
                key: InstanceKey(key.clone()),
                occurrence_ms: owner.occurrence_ms,
                base_due_ms: owner.base_due_ms,
                effective_due_ms: owner.effective_due_ms,
            };
            match self.items.remove(&(kind, key)) {
                None => differences.push(DueTableDifference::MissingItem { kind, reference }),
                Some(projected) if projected != reference => {
                    differences.push(DueTableDifference::WrongItemTime {
                        kind,
                        reference,
                        projected,
                    });
                }
                Some(_) => {}
            }
        }
        differences.extend(
            self.items
                .into_iter()
                .map(|((kind, _), projected)| DueTableDifference::ExtraItem { kind, projected }),
        );
        self.sources
    }
}

fn compare_sources(
    mut projected: BTreeMap<DueKind, Option<i64>>,
    rows: Vec<StoredSource>,
    differences: &mut Vec<DueTableDifference>,
) {
    for row in rows {
        let kind = match (row.kind.as_str(), row.key.as_str()) {
            ("engine_source", key) => {
                DueKind::from_tag(key).filter(|kind| kind.lane() == DueLane::OnGrid)
            }
            (kind, "all") => DueKind::from_tag(kind).filter(|kind| kind.lane() == DueLane::Legacy),
            _ => None,
        };
        let Some(kind) = kind else { continue };
        let reference = row.effective_due_ms;
        match projected.remove(&kind) {
            None => differences.push(DueTableDifference::MissingSource { kind, reference }),
            Some(projected) if projected != reference => {
                differences.push(DueTableDifference::WrongSourceTime {
                    kind,
                    reference,
                    projected,
                });
            }
            Some(_) => {}
        }
    }
    differences.extend(
        projected
            .into_iter()
            .map(|(kind, projected)| DueTableDifference::ExtraSource { kind, projected }),
    );
}

/// Compare stored entries, capped dispatch order, and alarm inputs and decision.
/// Run on a read snapshot outside event dispatch.
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
    macro_rules! query_as {
        ($row:ty, $file:literal) => {
            sqlx::query_as::<T::Database, $row>(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/",
                $file
            )))
        };
    }
    macro_rules! all {
        ($query:expr, $context:literal) => {
            $query
                .fetch_all(transaction.executor())
                .await
                .map_err(|error| map_central_query_failure(error, $context))?
        };
    }
    let (alarm, approval_alarm, legacy_alarm, sources, legacy_times, projection, owners) =
        comparison_queries!(query_as);
    let mut differences = Vec::new();
    let projection = Projection::decode(
        all!(projection, "due projection comparison"),
        &mut differences,
    );
    let sources_in_projection =
        projection.compare_items(all!(owners, "due owner comparison"), &mut differences);
    compare_sources(
        sources_in_projection,
        all!(sources, "due source comparison"),
        &mut differences,
    );

    for (kind, list_kind) in [
        (DueKind::WakeTimer, DueListKind::Wakes),
        (DueKind::AutomationTimer, DueListKind::Schedules),
        (DueKind::DirectMessage, DueListKind::DirectMessages),
        (DueKind::SystemMessageExpiry, DueListKind::SystemExpiries),
        (
            DueKind::ApprovalRequestExpiry,
            DueListKind::ApprovalExpiries,
        ),
    ] {
        let query =
            due_list_query(list_kind, horizon, cap).map_err(|_| StoreFailure::Unavailable)?;
        let reference = read_due_list(transaction, query, None).await;
        let projected = read_timed_due_rows(transaction, query).await;
        match (reference, projected) {
            (Ok(reference), Ok(projected)) if reference != projected => {
                differences.push(DueTableDifference::WrongDispatch {
                    kind,
                    reference,
                    projected,
                });
            }
            (Err(StoreFailure::Corrupt { .. }), _) | (_, Err(StoreFailure::Corrupt { .. }))
                if !differences.is_empty() => {}
            (Err(failure), _) | (_, Err(failure)) => return Err(failure),
            _ => {}
        }
    }

    let alarm = alarm
        .fetch_one(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "reference alarm inputs"))?;
    let approval_alarm = approval_alarm
        .fetch_one(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "reference approval alarm input"))?;
    let reference = EngineAlarmTimes {
        earliest_on_grid_due_unix_millis: alarm
            .on_grid_due
            .into_iter()
            .chain(approval_alarm.approval_due_ms)
            .min(),
        earliest_legacy_due_unix_millis: alarm.legacy_due,
    };
    let legacy_alarm = legacy_alarm
        .fetch_optional(transaction.executor())
        .await
        .map_err(|error| map_central_query_failure(error, "reference legacy alarm inputs"))?
        .unwrap_or_default();
    let projected_snapshot = read_alarm_snapshot(transaction).await;
    match projected_snapshot {
        Ok(snapshot) => {
            for ((kind, projected), reference) in snapshot.legacy_by_kind.into_iter().zip([
                legacy_alarm.notice_window_due,
                legacy_alarm.workspace_drain_due,
            ]) {
                if reference != projected {
                    differences.push(DueTableDifference::WrongLegacyContribution {
                        kind,
                        reference,
                        projected,
                    });
                }
            }
            let projected = snapshot.times;
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
        }
        Err(StoreFailure::Corrupt { .. }) if !differences.is_empty() => {}
        Err(error) => return Err(error),
    }

    for LegacyTime {
        kind,
        key,
        stored_time,
    } in all!(legacy_times, "legacy time comparison")
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
