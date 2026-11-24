//! Transactions that present decoded rows and evidence to the outbox machine.
use super::{
    ANSWERED_RETENTION_MILLIS, MacOutbox, OutboxStoreError, StoredOutboxWrite,
    outbox_row_effects::{RowTransition, apply_row_event, unexpected},
    outbox_row_machine::OutboxRowEvent,
    outbox_row_state::OutboxRowState,
    outbox_rows::{
        corrupt_row, decode_row, in_flight_rows, read_owned_outbox, read_row, select_outbox_rows,
        waiting_page, waiting_rows,
    },
    validate_write_timestamp,
};
use crate::{
    EndedWrite, OutboxWriteOutcome, StoreScan, StoredSenderOutcome,
    outbox_item_state::OutboxItemState,
};
use sqlite_storage_maintenance::TransactionStart;
use std::collections::BTreeSet;
use workspace_ids::RequestId;
use workspace_model::Caller;

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
            let event = OutboxRowEvent::Prune { cutoff_ms: cutoff };
            if let RowTransition::Removed(count) =
                apply_row_event(&mut transaction, &row, event, "retained after ending").await?
            {
                removed += count;
            }
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
            let Some(row) = decoded else {
                return Err(unexpected(request, "in the outbox before its deadline"));
            };
            let claimed = claim_row(&mut transaction, row, now_ms, i64::MAX, now_ms).await?;
            writes.push(
                claimed.ok_or_else(|| unexpected(request, "in the outbox before its deadline"))?,
            );
        }
        transaction.commit().await?;
        Ok(writes)
    }

    /// Claims up to `limit` due writes in creation and request ID order.
    /// A live write is eligible only if its deadline exceeds
    /// `required_deadline_after_ms`. Waiting writes expire at `now_ms`.
    /// Excluded requests are neither claimed nor expired by this call.
    pub async fn claim_next_batch(
        &mut self,
        mark_ms: i64,
        limit: u16,
        now_ms: i64,
        required_deadline_after_ms: i64,
        excluded_request_ids: &BTreeSet<RequestId>,
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
                if excluded_request_ids.contains(&row.request) {
                    continue;
                }
                if row.deadline_ms <= now_ms {
                    collect_ending(&mut transaction, &row, now_ms, &mut ended).await?;
                } else if writes.len() < usize::from(limit)
                    && let Some(write) = claim_row(
                        &mut transaction,
                        row,
                        now_ms,
                        mark_ms,
                        required_deadline_after_ms,
                    )
                    .await?
                {
                    writes.push(write);
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

    /// Keeps Central's checked answer, dropping the queued text in the same transaction.
    pub async fn record_outcome(
        &mut self,
        request: &RequestId,
        outcome: &OutboxWriteOutcome,
        now_ms: i64,
    ) -> Result<(), OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let row = read_request(&mut transaction, request, "sent").await?;
        let event = OutboxRowEvent::RecordOutcome {
            outcome: outcome.clone(),
            now_ms,
        };
        apply_row_event(&mut transaction, &row, event, "sent").await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Saves a permanent result without changing the unconfirmed state or cutoff stamp.
    pub async fn record_late_outcome(
        &mut self,
        request: &RequestId,
        outcome: &OutboxWriteOutcome,
    ) -> Result<(), OutboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let row = read_request(&mut transaction, request, "ended unconfirmed").await?;
        let event = OutboxRowEvent::RecordLateOutcome {
            outcome: outcome.clone(),
        };
        if let RowTransition::Applied =
            apply_row_event(&mut transaction, &row, event, "ended unconfirmed").await?
        {
            transaction.commit().await?;
        }
        Ok(())
    }

    pub async fn record_sender_delivery(
        &mut self,
        caller: &Caller,
        notice: &StoredSenderOutcome,
        now_ms: i64,
    ) -> Result<bool, OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let Some(row) = read_owned_outbox(&mut transaction, caller, &notice.request_id).await?
        else {
            return Ok(false);
        };
        let event = OutboxRowEvent::SenderDelivery {
            notice: notice.clone(),
            now_ms,
        };
        match apply_row_event(
            &mut transaction,
            &row,
            event,
            "eligible for delivery notice",
        )
        .await?
        {
            RowTransition::Applied => {
                transaction.commit().await?;
                Ok(true)
            }
            RowTransition::NoticeUnchanged(retained) => Ok(retained),
            _ => Err(unexpected(&row.request, "eligible for delivery notice")),
        }
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
        let row = read_request(&mut transaction, request, "sent").await?;
        apply_row_event(
            &mut transaction,
            &row,
            OutboxRowEvent::ReturnNotTransmitted { now_ms },
            "sent",
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Sent writes left unanswered go back to the outbox with their receipt
    /// mark, marked as possibly already in Central. A due claim ends instead.
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
                Ok(row) => row,
                Err(corruption) => {
                    corrupt_rows.push(corruption);
                    continue;
                }
            };
            if let RowTransition::Ended(ending) = apply_row_event(
                &mut transaction,
                &row,
                OutboxRowEvent::ReturnUnanswered { now_ms },
                "sent",
            )
            .await?
            {
                ended.push(ending);
            }
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: ended,
            corrupt_rows,
        })
    }

    /// Ends every waiting write due at the supplied time, even while offline.
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
        for row in healthy {
            collect_ending(&mut transaction, &row, now_ms, &mut ended).await?;
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: ended,
            corrupt_rows,
        })
    }

    /// Ends overdue claims that have no live batch ticket as possibly transmitted.
    pub async fn end_overdue_orphans(
        &mut self,
        now_ms: i64,
        live_request_ids: &BTreeSet<RequestId>,
    ) -> Result<StoreScan<Vec<EndedWrite>>, OutboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let StoreScan {
            healthy,
            corrupt_rows,
        } = in_flight_rows(&mut transaction).await?;
        let mut ended = Vec::new();
        for row in healthy
            .into_iter()
            .filter(|row| !live_request_ids.contains(&row.request))
        {
            collect_ending(&mut transaction, &row, now_ms, &mut ended).await?;
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

async fn read_request(
    connection: &mut sqlx::SqliteConnection,
    request: &RequestId,
    expected: &'static str,
) -> Result<OutboxRowState, OutboxStoreError> {
    let raw = read_row(connection, request)
        .await?
        .ok_or_else(|| unexpected(request, expected))?;
    Ok(decode_row(&raw)?)
}

async fn claim_row(
    connection: &mut sqlx::SqliteConnection,
    row: OutboxRowState,
    now_ms: i64,
    eligible_through_ms: i64,
    required_deadline_after_ms: i64,
) -> Result<Option<StoredOutboxWrite>, OutboxStoreError> {
    let event = OutboxRowEvent::Claim {
        now_ms,
        eligible_through_ms,
        required_deadline_after_ms,
    };
    if let RowTransition::NotDue =
        apply_row_event(connection, &row, event, "in the outbox before its deadline").await?
    {
        return Ok(None);
    }
    let payload = row
        .phase
        .into_queued()
        .ok_or_else(|| corrupt_row(row.request.as_str(), "payload_json"))?;
    Ok(Some(StoredOutboxWrite {
        request: row.request,
        caller: row.caller,
        payload,
        sent_at: row.sent_at,
    }))
}

async fn collect_ending(
    connection: &mut sqlx::SqliteConnection,
    row: &OutboxRowState,
    now_ms: i64,
    ended: &mut Vec<EndedWrite>,
) -> Result<(), OutboxStoreError> {
    if let RowTransition::Ended(ending) = apply_row_event(
        connection,
        row,
        OutboxRowEvent::EndDue { now_ms },
        "waiting or sent",
    )
    .await?
    {
        ended.push(ending);
    }
    Ok(())
}
