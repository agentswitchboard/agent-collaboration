use approval_request_protocol::Raise;
use central_storage::{SafeInteger, SqlValue, Statement};

use super::{ApprovalDispatchRollbackHost, Prepared, Result, text};

pub(super) struct DueSubject {
    key: String,
    occurrence: i64,
}

pub(super) async fn read_due_subject(
    host: &impl ApprovalDispatchRollbackHost,
    request: &Raise,
    occurrence: i64,
) -> Result<DueSubject> {
    let due = host
        .read(Statement {
            sql: "SELECT due.subject_key, due.occurrence_ms
                  FROM approval_requests AS request
                  JOIN engine_due_entries AS due ON due.subject_key = request.approval_row_id
                  WHERE request.request_id=?1 AND due.work_kind='approval_request'
                    AND due.entry_role='item'",
            args: vec![text(request.request_id().as_str())],
        })
        .await?;
    let [row] = due.rows.as_slice() else {
        return Err("the raised approval must have exactly one due item".into());
    };
    let [SqlValue::Text(key), SqlValue::Integer(stored_occurrence)] = row.as_slice() else {
        return Err("the raised approval due item has no key or occurrence".into());
    };
    assert_eq!(stored_occurrence.get(), occurrence);
    Ok(DueSubject {
        key: key.clone(),
        occurrence,
    })
}

pub(super) async fn require_repaired(
    host: &impl ApprovalDispatchRollbackHost,
    prepared: &Prepared,
) -> Result<()> {
    let due = prepared
        .due_subject
        .as_ref()
        .ok_or("missing original due item")?;
    let expected_retry = (prepared.at.div_euclid(300_000) + 1) * 300_000;
    let retry_at = chrono::DateTime::from_timestamp_millis(expected_retry)
        .ok_or("retry mark outside fixture time")?
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let repaired = host
        .read(Statement {
            sql: "SELECT request.record_state, attempt.subject_key, due.subject_key,
                         due.occurrence_ms, attempt.last_failure, attempt.last_failure_detail,
                         attempt.failure_count, attempt.next_try_at, due.effective_due_ms
                  FROM approval_requests AS request
                  JOIN engine_work_attempts AS attempt ON attempt.subject_key = request.approval_row_id
                  JOIN engine_due_entries AS due ON due.subject_key = request.approval_row_id
                  WHERE request.request_id=?1 AND attempt.machine_kind='approval_request'
                    AND due.work_kind='approval_request' AND due.entry_role='item'",
            args: vec![text(prepared.request()?.request_id().as_str())],
        })
        .await?;
    assert_eq!(
        repaired.rows,
        [vec![
            text("waiting"),
            text(&due.key),
            text(&due.key),
            SqlValue::Integer(SafeInteger::new(due.occurrence)?),
            text("corrupt_row"),
            text("engine_work_attempts.last_failure\nfixture_unknown_failure"),
            SqlValue::Integer(SafeInteger::new(1)?),
            text(&retry_at),
            SqlValue::Integer(SafeInteger::new(expected_retry)?),
        ]],
        "corrupt checked attempt must repair the exact diagnostic and retry projection",
    );
    Ok(())
}
