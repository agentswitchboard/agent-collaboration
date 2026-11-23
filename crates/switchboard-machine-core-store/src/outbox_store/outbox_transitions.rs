//! Transactional outbox claim, settlement, expiry and retention.
use super::{
    ANSWERED_RETENTION_MILLIS, MacOutbox, OutboxStoreError, StoredOutboxWrite,
    outbox_rows::{
        OnTheWireRow, OutboxRow, StoredPayload, corrupt_row, decode_row, expect_one,
        outbox_content, parse_request_id, payload_content_digest,
    },
};
use crate::{
    EndedWrite, OutboxQueuedWrite, OutboxWriteOutcome, machine_local_store::MachineLocalWrite,
    outbox_deadline::DepartureAtDeadline, outbox_item_state::OutboxItemState,
    outbox_receipt_profile::AnsweredWrite,
};
use sqlite_storage_maintenance::TransactionStart;
use workspace_ids::RequestId;

impl MacOutbox<'_> {
    /// Deletes ended writes (answered, refused, cancelled or given up) 60 days
    /// after they ended, on the injected clock.
    pub async fn delete_ended_before_retention(
        &mut self,
        now_ms: i64,
    ) -> Result<u64, OutboxStoreError> {
        let cutoff = now_ms.saturating_sub(ANSWERED_RETENTION_MILLIS);
        let answered = OutboxItemState::Answered.as_str();
        let refused = OutboxItemState::Refused.as_str();
        let not_sent = OutboxItemState::NotSent.as_str();
        let unconfirmed = OutboxItemState::Unconfirmed.as_str();
        Ok(sqlx::query!(
            "DELETE FROM outbox_items WHERE item_state IN (?, ?, ?, ?) AND updated_at <= ?",
            answered,
            refused,
            not_sent,
            unconfirmed,
            cutoff
        )
        .execute_as_transaction(self.store)
        .await?
        .rows_affected())
    }

    /// Moves outbox writes to sent and returns what goes in the batch frame.
    #[cfg(test)]
    pub(crate) async fn mark_sent(
        &mut self,
        requests: &[RequestId],
        now_ms: i64,
    ) -> Result<Vec<StoredOutboxWrite>, OutboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut writes = Vec::with_capacity(requests.len());
        for request in requests {
            let request_id = request.as_str();
            let (row, created_at) = put_on_the_wire(&mut transaction, request_id, now_ms)
                .await?
                .ok_or_else(|| OutboxStoreError::UnexpectedState {
                    request: request.clone(),
                    expected: "in the outbox before its deadline",
                })?
                .split();
            let stored = decode_row(&row)?;
            let StoredPayload::Unanswered(payload) = stored.payload else {
                return Err(corrupt_row(request_id, "payload_json"));
            };
            writes.push(StoredOutboxWrite {
                request: request.clone(),
                caller: stored.caller,
                sent_at: original_send_stamp(request_id, created_at)?,
                payload,
            });
        }
        transaction.commit().await?;
        Ok(writes)
    }

    /// Claims the next batch: up to `limit` writes waiting for `mark` or
    /// earlier and still before their deadline, oldest first by (created_at,
    /// request_id), moved to sent in one transaction and returned for the
    /// batch frame. The same transaction first ends every waiting write past
    /// its deadline, so an overdue write is never claimed (step 9). The table
    /// is the only list of what waits (Lead A, 2026-11-18).
    pub async fn claim_next_batch(
        &mut self,
        mark_ms: i64,
        limit: u16,
        now_ms: i64,
    ) -> Result<ClaimedBatch, OutboxStoreError> {
        let unsent = OutboxItemState::InOutbox.as_str();
        let answer_lost = OutboxItemState::SentAnswerLost.as_str();
        let limit = i64::from(limit);
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let ended = end_overdue_waiting(&mut transaction, now_ms).await?;
        let due = sqlx::query_scalar!(
            r#"SELECT request_id AS "request_id!" FROM outbox_items
               WHERE item_state IN (?, ?) AND flush_at_ms <= ? AND expires_at_ms > ?
               ORDER BY created_at, request_id LIMIT ?"#,
            unsent,
            answer_lost,
            mark_ms,
            now_ms,
            limit
        )
        .fetch_all(&mut *transaction)
        .await?;
        let mut writes = Vec::with_capacity(due.len());
        for request_id in &due {
            let (row, created_at) = put_on_the_wire(&mut transaction, request_id, now_ms)
                .await?
                .ok_or_else(|| corrupt_row(request_id, "item_state"))?
                .split();
            let stored = decode_row(&row)?;
            let StoredPayload::Unanswered(payload) = stored.payload else {
                return Err(corrupt_row(request_id, "payload_json"));
            };
            writes.push(StoredOutboxWrite {
                request: parse_request_id(request_id)?,
                caller: stored.caller,
                sent_at: original_send_stamp(request_id, created_at)?,
                payload,
            });
        }
        transaction.commit().await?;
        Ok(ClaimedBatch { writes, ended })
    }

    /// Keeps central's answer for one sent write and, in the same transaction,
    /// drops its text: the stored payload becomes the body-free answered record
    /// (owner, 2026-11-15: unsent text stays on the Mac only until central
    /// answers).
    pub async fn record_outcome(
        &mut self,
        request: &RequestId,
        outcome: &OutboxWriteOutcome,
        now_ms: i64,
    ) -> Result<(), OutboxStoreError> {
        let (state, outcome_json) = match outcome {
            OutboxWriteOutcome::Answered(reply) => {
                (OutboxItemState::Answered, serde_json::to_string(reply)?)
            }
            OutboxWriteOutcome::Refused(reason) => {
                (OutboxItemState::Refused, serde_json::to_string(reason)?)
            }
        };
        let to = state.as_str();
        let sent = OutboxItemState::Sent.as_str();
        let resent = OutboxItemState::Resent.as_str();
        let request_id = request.as_str();
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let row = sqlx::query_as!(
            OutboxRow,
            r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                      flush_at_ms, outcome_json
               FROM outbox_items WHERE request_id = ? AND item_state IN (?, ?)"#,
            request_id,
            sent,
            resent
        )
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| OutboxStoreError::UnexpectedState {
            request: request.clone(),
            expected: "sent",
        })?;
        let StoredPayload::Unanswered(payload) = decode_row(&row)?.payload else {
            return Err(corrupt_row(request_id, "payload_json"));
        };
        let answered = AnsweredWrite {
            delivery_outcome: None,
            request_id: request.clone(),
            target: outbox_content(request_id, &payload)?.target,
            content_digest: payload_content_digest(&payload)?,
        };
        let payload_json = serde_json::to_string(&answered)?;
        let from = row.item_state.as_str();
        let changed = sqlx::query!(
            "UPDATE outbox_items SET item_state = ?, outcome_json = ?, payload_json = ?,
                    updated_at = ?
             WHERE request_id = ? AND item_state = ?",
            to,
            outcome_json,
            payload_json,
            now_ms,
            request_id,
            from
        )
        .execute(&mut *transaction)
        .await?
        .rows_affected();
        expect_one(changed, request, "sent")?;
        transaction.commit().await?;
        Ok(())
    }

    /// Returns a claim whose frame never left this Mac. A replay still
    /// carries the uncertainty of the earlier transmission.
    pub async fn return_not_transmitted(
        &mut self,
        request: &RequestId,
        now_ms: i64,
    ) -> Result<(), OutboxStoreError> {
        let sent = OutboxItemState::Sent.as_str();
        let resent = OutboxItemState::Resent.as_str();
        let unsent = OutboxItemState::InOutbox.as_str();
        let answer_lost = OutboxItemState::SentAnswerLost.as_str();
        let request_id = request.as_str();
        let changed = sqlx::query!(
            "UPDATE outbox_items
             SET item_state = CASE item_state WHEN ? THEN ? ELSE ? END, updated_at = ?
             WHERE request_id = ? AND item_state IN (?, ?)",
            sent,
            unsent,
            answer_lost,
            now_ms,
            request_id,
            sent,
            resent
        )
        .execute_as_transaction(self.store)
        .await?
        .rows_affected();
        expect_one(changed, request, "sent")
    }

    /// Sent writes left unanswered go back to the outbox with their receipt
    /// mark, marked as possibly already in central. One past its deadline
    /// ends instead, in the same transaction: it is never replayed (step 9).
    pub async fn return_unanswered(
        &mut self,
        requests: &[RequestId],
        now_ms: i64,
    ) -> Result<Vec<EndedWrite>, OutboxStoreError> {
        let sent = OutboxItemState::Sent.as_str();
        let resent = OutboxItemState::Resent.as_str();
        let to = OutboxItemState::SentAnswerLost.as_str();
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut ended = Vec::new();
        for request in requests {
            let request_id = request.as_str();
            let row = sqlx::query_as!(
                OutboxRow,
                r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                          flush_at_ms, outcome_json
                   FROM outbox_items
                   WHERE request_id = ? AND item_state IN (?, ?) AND expires_at_ms <= ?"#,
                request_id,
                sent,
                resent,
                now_ms
            )
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(row) = row {
                ended.push(
                    end_row(
                        &mut transaction,
                        &row,
                        DepartureAtDeadline::MayHaveArrived,
                        now_ms,
                    )
                    .await?,
                );
                continue;
            }
            let changed = sqlx::query!(
                "UPDATE outbox_items SET item_state = ?, updated_at = ?
                 WHERE request_id = ? AND item_state IN (?, ?)",
                to,
                now_ms,
                request_id,
                sent,
                resent
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            expect_one(changed, request, "sent")?;
        }
        transaction.commit().await?;
        Ok(ended)
    }

    /// Ends every waiting write past its deadline, for the flush tick: a
    /// Router that cannot send still tells its agents.
    pub async fn end_overdue(&mut self, now_ms: i64) -> Result<Vec<EndedWrite>, OutboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let ended = end_overdue_waiting(&mut transaction, now_ms).await?;
        transaction.commit().await?;
        Ok(ended)
    }
}

/// A claimed batch and the writes the same transaction ended unsent.
#[derive(Debug)]
pub struct ClaimedBatch {
    pub writes: Vec<StoredOutboxWrite>,
    pub ended: Vec<EndedWrite>,
}

/// Ends every waiting write (never sent, or sent with a lost answer) whose
/// deadline is at or before `now_ms`, inside the caller's transaction.
async fn end_overdue_waiting(
    transaction: &mut sqlx::SqliteConnection,
    now_ms: i64,
) -> Result<Vec<EndedWrite>, OutboxStoreError> {
    let unsent = OutboxItemState::InOutbox.as_str();
    let answer_lost = OutboxItemState::SentAnswerLost.as_str();
    let rows = sqlx::query_as!(
        OutboxRow,
        r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                  flush_at_ms, outcome_json
           FROM outbox_items WHERE item_state IN (?, ?) AND expires_at_ms <= ?
           ORDER BY created_at, request_id"#,
        unsent,
        answer_lost,
        now_ms
    )
    .fetch_all(&mut *transaction)
    .await?;
    let mut ended = Vec::with_capacity(rows.len());
    for row in &rows {
        let departure = match OutboxItemState::parse(&row.item_state) {
            Some(OutboxItemState::InOutbox) => DepartureAtDeadline::NeverLeft,
            Some(OutboxItemState::SentAnswerLost) => DepartureAtDeadline::MayHaveArrived,
            _ => return Err(corrupt_row(&row.request_id, "item_state")),
        };
        ended.push(end_row(transaction, row, departure, now_ms).await?);
    }
    Ok(ended)
}

/// Moves one overdue row to its end state and drops its text: the stored
/// payload becomes the text-free record of target and digest, so a resubmit
/// of the same write is still recognised.
async fn end_row(
    transaction: &mut sqlx::SqliteConnection,
    row: &OutboxRow,
    departure: DepartureAtDeadline,
    now_ms: i64,
) -> Result<EndedWrite, OutboxStoreError> {
    let request_id = row.request_id.as_str();
    let stored = decode_row(row)?;
    let StoredPayload::Unanswered(payload) = stored.payload else {
        return Err(corrupt_row(request_id, "payload_json"));
    };
    let target = outbox_content(request_id, &payload)?.target;
    let reason = crate::outbox_deadline::end_reason(departure);
    let request = parse_request_id(request_id)?;
    let record = serde_json::to_string(&AnsweredWrite {
        delivery_outcome: None,
        request_id: request.clone(),
        target: target.clone(),
        content_digest: payload_content_digest(&payload)?,
    })?;
    let to = OutboxItemState::ended(reason).as_str();
    let from = row.item_state.as_str();
    let changed = sqlx::query!(
        "UPDATE outbox_items SET item_state = ?, payload_json = ?, updated_at = ?
         WHERE request_id = ? AND item_state = ?",
        to,
        record,
        now_ms,
        request_id,
        from
    )
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    expect_one(changed, &request, "waiting or sent")?;
    Ok(EndedWrite {
        caller: stored.caller,
        request_id: request,
        target,
        reason,
    })
}

/// Moves one waiting row still before its deadline onto the wire: a first
/// attempt (from InOutbox) becomes Sent, a replay (from SentAnswerLost)
/// becomes Resent so an earlier transmission remains distinguishable.
/// None means the row cannot be sent.
async fn put_on_the_wire(
    transaction: &mut sqlx::SqliteConnection,
    request_id: &str,
    now_ms: i64,
) -> Result<Option<OnTheWireRow>, OutboxStoreError> {
    let unsent = OutboxItemState::InOutbox.as_str();
    let answer_lost = OutboxItemState::SentAnswerLost.as_str();
    let sent = OutboxItemState::Sent.as_str();
    let resent = OutboxItemState::Resent.as_str();
    Ok(sqlx::query_as!(
        OnTheWireRow,
        r#"UPDATE outbox_items
           SET item_state = CASE item_state WHEN ? THEN ? ELSE ? END, updated_at = ?
           WHERE request_id = ? AND item_state IN (?, ?) AND expires_at_ms > ?
           RETURNING request_id AS "request_id!", caller_json, payload_json,
                     item_state, flush_at_ms, outcome_json, created_at"#,
        unsent,
        sent,
        resent,
        now_ms,
        request_id,
        unsent,
        answer_lost,
        now_ms
    )
    .fetch_optional(&mut *transaction)
    .await?)
}

fn original_send_stamp(
    request_id: &str,
    created_at_ms: i64,
) -> Result<workspace_model::Timestamp, OutboxStoreError> {
    chrono::DateTime::from_timestamp_millis(created_at_ms)
        .ok_or_else(|| corrupt_row(request_id, "created_at"))
}
