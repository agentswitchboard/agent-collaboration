//! Checked changes to the Engine's due projection.
use crate::{DueEntryKey, DueItemKey, DueKind, DueLane, DueOwnerTiming, DueTableChange};
use central_engine_model::EngineTime;
use central_storage::{GuardedStatement, RowExpectation, SafeInteger, SqlValue, StoreFailure};
use fleet_contracts::EngineSubject;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};
use uuid::{NoContext, Timestamp, Uuid};

pub fn plan_due_table_change(
    change: &DueTableChange,
    changed_at: EngineTime,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let timestamp = format_utc_millis(changed_at);
    let guarded = |statement, expect| GuardedStatement { statement, expect };
    let any = RowExpectation::Any;
    Ok(match change {
        DueTableChange::PublishItem(owner) | DueTableChange::PublishInsertedItem(owner) => {
            validate_item(&owner.key)?;
            let only_after_insert = matches!(change, DueTableChange::PublishInsertedItem(_));
            vec![guarded(
                publish(
                    owner,
                    timestamp,
                    mint_entry_id(changed_at),
                    only_after_insert,
                )?,
                if only_after_insert {
                    any
                } else {
                    RowExpectation::Exactly(1)
                },
            )]
        }
        DueTableChange::RemoveItem(key) => {
            validate_item(key)?;
            vec![guarded(
                crate::checked_engine_statement!(
                    query_file,
                    [],
                    "src/queries/remove_due_item.sql",
                    kind = key.kind.tag().to_owned(),
                    subject = key.subject.0.clone(),
                ),
                any,
            )]
        }
        DueTableChange::RefreshAttempt(DueEntryKey::Item(key)) => {
            validate_item(key)?;
            vec![guarded(
                crate::checked_engine_statement!(
                    query_file,
                    [],
                    "src/queries/refresh_due_item_attempt.sql",
                    kind = key.kind.tag().to_owned(),
                    subject = key.subject.0.clone(),
                    at = timestamp,
                ),
                any,
            )]
        }
        DueTableChange::RefreshAttempt(DueEntryKey::SourceControl(kind)) => {
            let (attempt_kind, attempt_key) = source_attempt(*kind);
            vec![
                guarded(
                    crate::checked_engine_statement!(
                        query_file,
                        [],
                        "src/queries/remove_absent_due_source.sql",
                        kind = kind.tag().to_owned(),
                        attempt_kind = attempt_kind.to_owned(),
                        attempt_key = attempt_key.to_owned(),
                    ),
                    any,
                ),
                guarded(
                    crate::checked_engine_statement!(
                        query_file,
                        [],
                        "src/queries/publish_due_source.sql",
                        entry_id = mint_entry_id(changed_at),
                        kind = kind.tag().to_owned(),
                        attempt_kind = attempt_kind.to_owned(),
                        at = timestamp,
                        attempt_key = attempt_key.to_owned(),
                    ),
                    any,
                ),
            ]
        }
        DueTableChange::RemoveCancelledNotice(key) => vec![guarded(
            crate::checked_engine_statement!(
                query_file,
                [],
                "src/queries/remove_cancelled_notice_due.sql",
                message_id = key.0.clone(),
            ),
            any,
        )],
    })
}

/// Resolve the attempt registry's existing source identity into the due table's
/// typed role. Test fixtures and Engine-only probes have no production entry.
pub fn due_change_for_attempt(subject: &EngineSubject) -> Option<DueTableChange> {
    let kind = match (subject.kind.0, subject.key.0.as_str()) {
        ("engine_source", tag) => DueKind::from_tag(tag)
            .filter(|kind| kind.lane() == DueLane::OnGrid)
            .map(DueEntryKey::SourceControl),
        (tag @ ("legacy_notice_windows" | "legacy_workspace_drains"), "all") => {
            DueKind::from_tag(tag).map(DueEntryKey::SourceControl)
        }
        (tag, _) => DueKind::from_tag(tag)
            .filter(|kind| kind.lane() == DueLane::OnGrid)
            .map(|kind| {
                DueEntryKey::Item(DueItemKey {
                    kind,
                    subject: subject.key.clone(),
                })
            }),
    }?;
    Some(DueTableChange::RefreshAttempt(kind))
}

fn source_attempt(kind: DueKind) -> (&'static str, &'static str) {
    match kind.lane() {
        DueLane::OnGrid => ("engine_source", kind.tag()),
        DueLane::Legacy => (kind.tag(), "all"),
    }
}

fn validate_item(key: &DueItemKey) -> Result<(), StoreFailure> {
    if key.kind.lane() != DueLane::OnGrid || key.subject.0.is_empty() {
        return Err(corrupt("engine_due_entries.subject_key"));
    }
    Ok(())
}

fn safe(value: i64) -> Result<SafeInteger, StoreFailure> {
    SafeInteger::new(value).map_err(|_| corrupt("engine_due_entries.due_time"))
}

fn publish(
    owner: &DueOwnerTiming,
    timestamp: String,
    id: String,
    only_after_insert: bool,
) -> Result<central_storage::Statement, StoreFailure> {
    Ok(crate::checked_engine_statement!(
        query_file,
        [],
        "src/queries/publish_due_item.sql",
        entry_id = id,
        kind = owner.key.kind.tag().to_owned(),
        subject = owner.key.subject.0.clone(),
        occurrence = safe(owner.occurrence.get())?,
        base_due = safe(owner.base_due.get())?,
        at = timestamp,
        only_after_insert = safe(i64::from(only_after_insert))?,
    ))
}

fn mint_entry_id(at: EngineTime) -> String {
    let millis = u64::try_from(at.timestamp_millis()).unwrap_or(0);
    Uuid::new_v7(Timestamp::from_unix(
        NoContext,
        millis / 1_000,
        at.timestamp_subsec_nanos(),
    ))
    .to_string()
}

// Apply on the already-lent transaction. No transaction, clock or alarm is
// started here; the owning caller still commits and publishes the alarm.
pub async fn apply_due_table_change<T: fleet_lent_transaction::CentralTransaction>(
    transaction: &mut T,
    change: &DueTableChange,
    changed_at: EngineTime,
) -> Result<(), StoreFailure>
where
    <T::Database as sqlx::Database>::Arguments: sqlx::IntoArguments<T::Database>,
    String: sqlx::Type<T::Database> + for<'q> sqlx::Encode<'q, T::Database>,
    i64: sqlx::Type<T::Database> + for<'q> sqlx::Encode<'q, T::Database>,
{
    for guarded in plan_due_table_change(change, changed_at)? {
        let mut query = sqlx::query::<T::Database>(guarded.statement.sql);
        for arg in guarded.statement.args {
            query = match arg {
                SqlValue::Text(value) => query.bind(value),
                SqlValue::Integer(value) => query.bind(value.get()),
                SqlValue::Null | SqlValue::Blob(_) => {
                    return Err(corrupt("engine_due_entries.bind"));
                }
            };
        }
        let result = query
            .execute(transaction.executor())
            .await
            .map_err(|_| StoreFailure::Unavailable)?;
        if !guarded.expect.admits(T::rows_affected(result)) {
            return Err(corrupt("engine_due_entries.rows_affected"));
        }
    }
    Ok(())
}
