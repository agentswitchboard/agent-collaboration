//! Caller-scoped reads and selection of pending writes.
use super::{
    MacOutbox, OutboxStoreError, UnansweredOutboxClaim,
    outbox_row_state::OutboxPhase,
    outbox_rows::{
        corrupt_row, decode_owned_row, in_flight_rows, outbox_content, read_owned_outbox,
        select_outbox_rows, waiting_rows,
    },
};
use crate::OutboxRequestKey;
use crate::{
    OutboxRequestStatus, OutgoingOutcome, OutgoingWrite, PendingWrite, PendingWriteState,
    RowCorruption, RowCorruptionReason, StoreScan, StoredSenderOutcome,
    outbox_item_state::OutboxItemState, outbox_settlement::SettledWrite,
};
use workspace_ids::RequestId;
use workspace_model::Caller;

impl MacOutbox<'_> {
    pub async fn scoped_request_status(
        &mut self,
        caller: &Caller,
        key: &OutboxRequestKey,
    ) -> Result<OutboxRequestStatus, OutboxStoreError> {
        let Some(stored) =
            read_owned_outbox(&mut self.store.connection, caller, &key.request_id).await?
        else {
            return Ok(OutboxRequestStatus::NotFound);
        };
        Ok(if stored.phase.request_scope() == key.scope {
            stored.status()
        } else {
            OutboxRequestStatus::NotFound
        })
    }

    /// The earliest expiry among waiting writes, even without a connection.
    pub async fn earliest_deadline(&mut self) -> Result<StoreScan<Option<i64>>, OutboxStoreError> {
        Ok(waiting_rows(&mut self.store.connection)
            .await?
            .map_healthy(|rows| rows.into_iter().map(|row| row.deadline_ms).min()))
    }
    /// The caller's own writes not yet resolved, oldest first: unsent, sent
    /// with a lost answer (or still in flight), and answered ones the read
    /// layer drops once it sees their created ids. A late refusal replaces the
    /// unconfirmed cancellation in this view; ordinary refusals are excluded.
    pub async fn pending_for(
        &mut self,
        caller: &Caller,
    ) -> Result<Vec<PendingWrite>, OutboxStoreError> {
        let caller_json = serde_json::to_string(caller)?;
        let refused = OutboxItemState::Refused.as_str();
        let rows = select_outbox_rows!(
            r#"WHERE caller_json = ? AND item_state <> ?
               ORDER BY created_at, request_id"#,
            caller_json,
            refused
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        let mut pending = Vec::with_capacity(rows.len());
        for row in &rows {
            let stored = decode_owned_row(row, caller)?;
            let (target, state) = match stored.phase {
                OutboxPhase::InOutbox(payload) => {
                    let content = outbox_content(stored.request.as_str(), &payload)?;
                    (
                        content.target,
                        PendingWriteState::Unsent { body: content.body },
                    )
                }
                OutboxPhase::SentAnswerLost(payload)
                | OutboxPhase::Sent(payload)
                | OutboxPhase::Resent(payload) => {
                    let content = outbox_content(stored.request.as_str(), &payload)?;
                    let body = content.body;
                    (content.target, PendingWriteState::SentAnswerLost { body })
                }
                OutboxPhase::Answered(profile, reply)
                | OutboxPhase::Unconfirmed(profile, Some(SettledWrite::Answered(reply))) => {
                    (profile.target, PendingWriteState::Answered(reply))
                }
                OutboxPhase::Unconfirmed(profile, Some(SettledWrite::Refused(refusal))) => {
                    (profile.target, PendingWriteState::Refused(refusal))
                }
                OutboxPhase::NotSent(profile) => (
                    profile.target,
                    PendingWriteState::Cancelled {
                        reason: crate::OutboxCancellation::NotSentWithin15Minutes,
                    },
                ),
                OutboxPhase::Unconfirmed(profile, None) => (
                    profile.target,
                    PendingWriteState::Cancelled {
                        reason: crate::OutboxCancellation::NotConfirmedWithin15Minutes,
                    },
                ),
                OutboxPhase::Refused(..) => {
                    return Err(corrupt_row(stored.request.as_str(), "item_state"));
                }
            };
            pending.push(PendingWrite {
                request_id: stored.request,
                target,
                flush_at_ms: row.flush_at_ms,
                state,
            });
        }
        Ok(pending)
    }

    /// The caller's recorded results, newest known answer first, followed by results with no answer time.
    pub async fn outgoing_for(
        &mut self,
        caller: &Caller,
        limit: u16,
    ) -> Result<Vec<OutgoingWrite>, OutboxStoreError> {
        let caller_json = serde_json::to_string(caller)?;
        let answered = OutboxItemState::Answered.as_str();
        let refused = OutboxItemState::Refused.as_str();
        let unconfirmed = OutboxItemState::Unconfirmed.as_str();
        let limit = i64::from(limit);
        let rows = select_outbox_rows!(
            r#"WHERE caller_json = ?
               AND (item_state IN (?, ?) OR (item_state = ? AND outcome_json IS NOT NULL))
               ORDER BY CASE WHEN item_state = ? THEN NULL ELSE updated_at END DESC,
                        request_id DESC LIMIT ?"#,
            caller_json,
            answered,
            refused,
            unconfirmed,
            unconfirmed,
            limit
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        let mut outgoing = Vec::with_capacity(rows.len());
        for row in rows {
            let stored = decode_owned_row(&row, caller)?;
            let answered_at_ms = (stored.phase.item_state() != OutboxItemState::Unconfirmed)
                .then_some(stored.updated_at_ms);
            let (target, outcome) = match stored.phase {
                OutboxPhase::Answered(profile, reply)
                | OutboxPhase::Unconfirmed(profile, Some(SettledWrite::Answered(reply))) => {
                    (profile.target, OutgoingOutcome::Answered(reply))
                }
                OutboxPhase::Refused(profile, reason)
                | OutboxPhase::Unconfirmed(profile, Some(SettledWrite::Refused(reason))) => {
                    (profile.target, OutgoingOutcome::Refused(reason))
                }
                OutboxPhase::NotSent(_) | OutboxPhase::Unconfirmed(_, None) => {
                    return Err(corrupt_row(stored.request.as_str(), "item_state"));
                }
                OutboxPhase::InOutbox(_)
                | OutboxPhase::SentAnswerLost(_)
                | OutboxPhase::Sent(_)
                | OutboxPhase::Resent(_) => {
                    return Err(corrupt_row(stored.request.as_str(), "payload_json"));
                }
            };
            outgoing.push(OutgoingWrite {
                request_id: stored.request,
                target,
                outcome,
                answered_at_ms,
            });
        }
        Ok(outgoing)
    }

    /// Writes waiting to be sent (never sent, or sent with a lost answer), with
    /// their receipt marks, to load the flush reducer.
    #[cfg(test)]
    pub(crate) async fn waiting(
        &mut self,
    ) -> Result<StoreScan<Vec<(i64, RequestId)>>, OutboxStoreError> {
        Ok(waiting_rows(&mut self.store.connection)
            .await?
            .map_healthy(|rows| {
                let mut results: Vec<_> = rows
                    .into_iter()
                    .map(|row| (row.flush_at_ms, row.request))
                    .collect();
                results.sort();
                results
            }))
    }

    /// Claims awaiting an answer, for custody to reconcile after reopen.
    pub async fn sent_without_answer(
        &mut self,
    ) -> Result<StoreScan<Vec<UnansweredOutboxClaim>>, OutboxStoreError> {
        let StoreScan {
            healthy,
            mut corrupt_rows,
        } = in_flight_rows(&mut self.store.connection).await?;
        let mut claims = Vec::with_capacity(healthy.len());
        for row in healthy {
            let Some(write) = row.phase.into_queued() else {
                corrupt_rows.push(RowCorruption {
                    key: row.request.to_string(),
                    field: "payload_json",
                    reason: RowCorruptionReason::InconsistentState,
                });
                continue;
            };
            let Some(content) = write.content() else {
                corrupt_rows.push(RowCorruption {
                    key: row.request.to_string(),
                    field: "payload_json",
                    reason: RowCorruptionReason::UnsupportedOperation,
                });
                continue;
            };
            claims.push(UnansweredOutboxClaim {
                key: OutboxRequestKey {
                    request_id: row.request,
                    scope: content.target.request_scope(),
                },
                caller: row.caller,
                target: content.target,
                deadline_ms: row.deadline_ms,
                payload: write,
            });
        }
        claims.sort_unstable_by(|first, second| first.key.request_id.cmp(&second.key.request_id));
        Ok(StoreScan {
            healthy: claims,
            corrupt_rows,
        })
    }

    pub async fn earliest_waiting(&mut self) -> Result<StoreScan<Option<i64>>, OutboxStoreError> {
        Ok(waiting_rows(&mut self.store.connection)
            .await?
            .map_healthy(|rows| rows.into_iter().map(|row| row.flush_at_ms).min()))
    }

    /// A retained sender outcome, scoped to the original caller and request.
    pub async fn sender_outcome(
        &mut self,
        caller: &Caller,
        request: &RequestId,
    ) -> Result<Option<StoredSenderOutcome>, OutboxStoreError> {
        let row = read_owned_outbox(&mut self.store.connection, caller, request).await?;
        Ok(row.and_then(|row| row.phase.profile()?.delivery_outcome.clone()))
    }
}
