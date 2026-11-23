//! Transactional outbox claim, settlement, expiry and retention.
use super::{
    ANSWERED_RETENTION_MILLIS, MacOutbox, OutboxStoreError, StoredOutboxWrite,
    outbox_rows::{
        DecodedRow, StoredPayload, corrupt_row, decode_row, expect_one, outbox_content,
        payload_content_digest, read_row, select_outbox_rows, waiting_page, waiting_rows,
    },
    validate_write_timestamp,
};
use crate::{
    EndedWrite, OutboxWriteOutcome, StoreScan, outbox_deadline::DepartureAtDeadline,
    outbox_item_state::OutboxItemState, outbox_receipt_profile::AnsweredWrite,
    outbox_settlement::SettledWrite,
};
use sqlite_storage_maintenance::TransactionStart;
use workspace_ids::RequestId;

impl MacOutbox<'_> {
    /// Deletes ended writes (answered, refused, cancelled or given up) 60 days
    /// after they ended, on the injected clock.
    pub async fn delete_ended_before_retention(
        &mut self,
        now_ms: i64,
    ) -> Result<StoreScan<u64>, OutboxStoreError> {
        validate_write_timestamp(now_ms, "now_ms")?;
        let cutoff = now_ms.saturating_sub(ANSWERED_RETENTION_MILLIS);
        let answered = OutboxItemState::Answered.as_str();
        let refused = OutboxItemState::Refused.as_str();
        let not_sent = OutboxItemState::NotSent.as_str();
        let unconfirmed = OutboxItemState::Unconfirmed.as_str();
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let rows = select_outbox_rows!(
            r#"WHERE item_state IN (?, ?, ?, ?)
                    AND (typeof(updated_at) <> 'integer' OR updated_at <= ?)"#,
            answered,
            refused,
            not_sent,
            unconfirmed,
            cutoff,
        )
        .fetch_all(&mut *transaction)
        .await?;
        let StoreScan {
            healthy,
            corrupt_rows,
        } = rows.iter().map(decode_row).collect();
        let mut removed = 0;
        for row in healthy {
            removed += sqlx::query!(
                "DELETE FROM outbox_items WHERE request_id = ?",
                row.request.as_str()
            )
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: removed,
            corrupt_rows,
        })
    }

    /// Moves outbox writes to sent and returns what goes in the batch frame.
    #[cfg(test)]
    pub(crate) async fn mark_sent(
        &mut self,
        requests: &[RequestId],
        now_ms: i64,
    ) -> Result<Vec<StoredOutboxWrite>, OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut writes = Vec::with_capacity(requests.len());
        for request in requests {
            let decoded = read_row(&mut transaction, request)
                .await?
                .as_ref()
                .map(decode_row)
                .transpose()?;
            let Some(row) = decoded.filter(|row| is_waiting(row.state) && row.deadline_ms > now_ms)
            else {
                return Err(unexpected(request, "in the outbox before its deadline"));
            };
            writes.push(claim_row(&mut transaction, row, now_ms).await?);
        }
        transaction.commit().await?;
        Ok(writes)
    }

    /// Claims up to `limit` due writes in creation and request ID order.
    /// A live write is eligible only if its deadline exceeds
    /// `required_deadline_after_ms`. Waiting writes expire at `now_ms`.
    pub async fn claim_next_batch(
        &mut self,
        mark_ms: i64,
        limit: u16,
        now_ms: i64,
        required_deadline_after_ms: i64,
    ) -> Result<StoreScan<ClaimedBatch>, OutboxStoreError> {
        validate_write_timestamp(mark_ms, "mark_ms")?;
        validate_write_timestamp(now_ms, "updated_at")?;
        validate_write_timestamp(required_deadline_after_ms, "required_deadline_after_ms")?;
        if required_deadline_after_ms < now_ms {
            return Err(OutboxStoreError::ClaimDeadlineThresholdBeforeNow {
                required_deadline_after_ms,
                now_ms,
            });
        }
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut cursor = None;
        let mut corrupt_rows = Vec::new();
        let mut ended = Vec::new();
        let mut writes = Vec::new();
        loop {
            let (page, next) = waiting_page(&mut transaction, cursor.as_ref()).await?;
            corrupt_rows.extend(page.corrupt_rows);
            for row in page.healthy {
                if row.deadline_ms <= now_ms {
                    let departure = waiting_departure(row.state);
                    ended.push(end_row(&mut transaction, row, departure, now_ms).await?);
                } else if row.deadline_ms > required_deadline_after_ms
                    && row.flush_at_ms <= mark_ms
                    && writes.len() < usize::from(limit)
                {
                    writes.push(claim_row(&mut transaction, row, now_ms).await?);
                }
            }
            if next.is_none() {
                break;
            }
            cursor = next;
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: ClaimedBatch { writes, ended },
            corrupt_rows,
        })
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
        validate_write_timestamp(now_ms, "updated_at")?;
        let request_id = request.as_str();
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let row = read_in_flight(&mut transaction, request).await?;
        let StoredPayload::Unanswered(payload) = row.payload else {
            return Err(corrupt_row(request_id, "payload_json"));
        };
        let target = outbox_content(request_id, &payload)?.target;
        let (state, outcome_json) = match outcome.for_claim(request, &target).map_err(|_| {
            OutboxStoreError::InvalidOutcome {
                request: request.clone(),
            }
        })? {
            SettledWrite::Answered(reply) => {
                (OutboxItemState::Answered, serde_json::to_string(reply)?)
            }
            SettledWrite::Refused(reason) => {
                (OutboxItemState::Refused, serde_json::to_string(reason)?)
            }
        };
        let to = state.as_str();
        let answered = AnsweredWrite {
            delivery_outcome: None,
            request_id: request.clone(),
            target,
            content_digest: payload_content_digest(&payload)?,
        };
        let payload_json = serde_json::to_string(&answered)?;
        let from = row.state.as_str();
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
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let row = read_in_flight(&mut transaction, request).await?;
        let waiting = if row.state == OutboxItemState::Sent {
            OutboxItemState::InOutbox
        } else {
            OutboxItemState::SentAnswerLost
        };
        update_state(&mut transaction, &row, waiting, now_ms).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Sent writes left unanswered go back to the outbox with their receipt
    /// mark, marked as possibly already in central. One past its deadline
    /// ends instead, in the same transaction: it is never replayed (step 9).
    pub async fn return_unanswered(
        &mut self,
        requests: &[RequestId],
        now_ms: i64,
    ) -> Result<StoreScan<Vec<EndedWrite>>, OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut ended = Vec::new();
        let mut corrupt_rows = Vec::new();
        for request in requests {
            let raw = read_row(&mut transaction, request)
                .await?
                .ok_or_else(|| unexpected(request, "sent"))?;
            let row = match decode_row(&raw) {
                Ok(row) => require_in_flight(row, request)?,
                Err(corruption) => {
                    corrupt_rows.push(corruption);
                    continue;
                }
            };
            if row.deadline_ms <= now_ms {
                ended.push(
                    end_row(
                        &mut transaction,
                        row,
                        DepartureAtDeadline::MayHaveArrived,
                        now_ms,
                    )
                    .await?,
                );
            } else {
                update_state(
                    &mut transaction,
                    &row,
                    OutboxItemState::SentAnswerLost,
                    now_ms,
                )
                .await?;
            }
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: ended,
            corrupt_rows,
        })
    }

    /// Ends every waiting write past its deadline, for the flush tick: a
    /// Router that cannot send still tells its agents.
    pub async fn end_overdue(
        &mut self,
        now_ms: i64,
    ) -> Result<StoreScan<Vec<EndedWrite>>, OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut ended = Vec::new();
        let StoreScan {
            healthy,
            corrupt_rows,
        } = waiting_rows(&mut transaction).await?;
        for row in healthy.into_iter().filter(|row| row.deadline_ms <= now_ms) {
            let departure = waiting_departure(row.state);
            ended.push(end_row(&mut transaction, row, departure, now_ms).await?);
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: ended,
            corrupt_rows,
        })
    }
}

/// A claimed batch and the writes the same transaction ended unsent.
#[derive(Debug)]
pub struct ClaimedBatch {
    pub writes: Vec<StoredOutboxWrite>,
    pub ended: Vec<EndedWrite>,
}

#[cfg(test)]
fn is_waiting(state: OutboxItemState) -> bool {
    matches!(
        state,
        OutboxItemState::InOutbox | OutboxItemState::SentAnswerLost
    )
}

fn waiting_departure(state: OutboxItemState) -> DepartureAtDeadline {
    if state == OutboxItemState::InOutbox {
        DepartureAtDeadline::NeverLeft
    } else {
        DepartureAtDeadline::MayHaveArrived
    }
}

async fn read_in_flight(
    connection: &mut sqlx::SqliteConnection,
    request: &RequestId,
) -> Result<DecodedRow, OutboxStoreError> {
    let raw = read_row(connection, request)
        .await?
        .ok_or_else(|| unexpected(request, "sent"))?;
    require_in_flight(decode_row(&raw)?, request)
}

fn require_in_flight(row: DecodedRow, request: &RequestId) -> Result<DecodedRow, OutboxStoreError> {
    if matches!(row.state, OutboxItemState::Sent | OutboxItemState::Resent) {
        Ok(row)
    } else {
        Err(unexpected(request, "sent"))
    }
}

async fn update_state(
    connection: &mut sqlx::SqliteConnection,
    row: &DecodedRow,
    next: OutboxItemState,
    now_ms: i64,
) -> Result<(), OutboxStoreError> {
    let changed = sqlx::query!(
        "UPDATE outbox_items SET item_state = ?, updated_at = ? WHERE request_id = ? AND item_state = ?",
        next.as_str(), now_ms, row.request.as_str(), row.state.as_str(),
    ).execute(connection).await?.rows_affected();
    expect_one(changed, &row.request, "in its previous state")
}

async fn claim_row(
    connection: &mut sqlx::SqliteConnection,
    row: DecodedRow,
    now_ms: i64,
) -> Result<StoredOutboxWrite, OutboxStoreError> {
    let next = if row.state == OutboxItemState::InOutbox {
        OutboxItemState::Sent
    } else {
        OutboxItemState::Resent
    };
    update_state(connection, &row, next, now_ms).await?;
    let StoredPayload::Unanswered(payload) = row.payload else {
        return Err(corrupt_row(row.request.as_str(), "payload_json"));
    };
    Ok(StoredOutboxWrite {
        request: row.request,
        caller: row.caller,
        payload,
        sent_at: row.sent_at,
    })
}

fn unexpected(request: &RequestId, expected: &'static str) -> OutboxStoreError {
    OutboxStoreError::UnexpectedState {
        request: request.clone(),
        expected,
    }
}

/// Moves one overdue row to its end state and drops its text: the stored
/// payload becomes the text-free record of target and digest, so a resubmit
/// of the same write is still recognised.
async fn end_row(
    transaction: &mut sqlx::SqliteConnection,
    row: DecodedRow,
    departure: DepartureAtDeadline,
    now_ms: i64,
) -> Result<EndedWrite, OutboxStoreError> {
    let request = &row.request;
    let request_id = request.as_str();
    let StoredPayload::Unanswered(payload) = row.payload else {
        return Err(corrupt_row(request_id, "payload_json"));
    };
    let target = outbox_content(request_id, &payload)?.target;
    let reason = crate::outbox_deadline::end_reason(departure);
    let record = serde_json::to_string(&AnsweredWrite {
        delivery_outcome: None,
        request_id: request.clone(),
        target: target.clone(),
        content_digest: payload_content_digest(&payload)?,
    })?;
    let to = OutboxItemState::ended(reason).as_str();
    let from = row.state.as_str();
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
    expect_one(changed, request, "waiting or sent")?;
    Ok(EndedWrite {
        caller: row.caller,
        request_id: row.request,
        target,
        reason,
    })
}
