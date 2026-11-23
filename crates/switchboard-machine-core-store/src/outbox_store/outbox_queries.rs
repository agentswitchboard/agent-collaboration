//! Caller-scoped reads and selection of pending writes.
use super::{
    MacOutbox, OutboxStoreError,
    outbox_rows::{
        OutboxRow, StoredPayload, corrupt_row, decode_owned_row, outbox_content, parse_request_id,
        read_owned_outbox, read_rows_in_states, waiting_rows,
    },
};
use crate::OutboxRequestKey;
use crate::{
    OutboxRequestStatus, OutgoingOutcome, OutgoingWrite, PendingWrite, PendingWriteState,
    StoreScan, outbox_item_state::OutboxItemState,
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
        let scope = match &stored.payload {
            StoredPayload::Unanswered(write) => write.request_scope(),
            StoredPayload::Answered(profile) => profile.target.request_scope(),
        };
        Ok(if scope == key.scope {
            stored.status
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
    /// layer drops once it sees their created ids. Refused writes are excluded.
    pub async fn pending_for(
        &mut self,
        caller: &Caller,
    ) -> Result<Vec<PendingWrite>, OutboxStoreError> {
        let caller_json = serde_json::to_string(caller)?;
        let refused = OutboxItemState::Refused.as_str();
        let rows = sqlx::query_as!(
            OutboxRow,
            r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                      flush_at_ms, outcome_json, created_at, expires_at_ms, updated_at
               FROM outbox_items WHERE caller_json = ? AND item_state <> ?
               ORDER BY created_at, request_id"#,
            caller_json,
            refused
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        let mut pending = Vec::with_capacity(rows.len());
        for row in &rows {
            let stored = decode_owned_row(row, caller)?;
            let (target, state) = match (stored.state, stored.status, stored.payload) {
                (OutboxItemState::InOutbox, _, StoredPayload::Unanswered(payload)) => {
                    let content = outbox_content(&row.request_id, &payload)?;
                    (
                        content.target,
                        PendingWriteState::Unsent { body: content.body },
                    )
                }
                (
                    OutboxItemState::SentAnswerLost
                    | OutboxItemState::Sent
                    | OutboxItemState::Resent,
                    _,
                    StoredPayload::Unanswered(payload),
                ) => {
                    let content = outbox_content(&row.request_id, &payload)?;
                    let body = content.body;
                    (content.target, PendingWriteState::SentAnswerLost { body })
                }
                (
                    OutboxItemState::Answered,
                    OutboxRequestStatus::Answered(reply),
                    StoredPayload::Answered(answered),
                ) => (answered.target, PendingWriteState::Answered(reply)),
                (
                    OutboxItemState::NotSent | OutboxItemState::Unconfirmed,
                    OutboxRequestStatus::Ended { reason },
                    StoredPayload::Answered(cancelled),
                ) => (cancelled.target, PendingWriteState::Cancelled { reason }),
                _ => return Err(corrupt_row(&row.request_id, "item_state")),
            };
            pending.push(PendingWrite {
                request_id: parse_request_id(&row.request_id)?,
                target,
                flush_at_ms: row.flush_at_ms,
                state,
            });
        }
        Ok(pending)
    }

    /// The caller's recently answered writes, newest answer first.
    pub async fn outgoing_for(
        &mut self,
        caller: &Caller,
        limit: u16,
    ) -> Result<Vec<OutgoingWrite>, OutboxStoreError> {
        let caller_json = serde_json::to_string(caller)?;
        let answered = OutboxItemState::Answered.as_str();
        let refused = OutboxItemState::Refused.as_str();
        let limit = i64::from(limit);
        let rows = sqlx::query_as!(
            OutboxRow,
            r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                      flush_at_ms, outcome_json, created_at, expires_at_ms, updated_at
               FROM outbox_items WHERE caller_json = ? AND item_state IN (?, ?)
               ORDER BY updated_at DESC, request_id DESC LIMIT ?"#,
            caller_json,
            answered,
            refused,
            limit
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        let mut outgoing = Vec::with_capacity(rows.len());
        for row in rows {
            let request_id = row.request_id.clone();
            let stored = decode_owned_row(&row, caller)?;
            let answered_at_ms = stored.updated_at_ms;
            let StoredPayload::Answered(answered) = stored.payload else {
                return Err(corrupt_row(&request_id, "payload_json"));
            };
            let outcome = match stored.status {
                OutboxRequestStatus::Answered(reply) => OutgoingOutcome::Answered(reply),
                OutboxRequestStatus::Refused(reason) => OutgoingOutcome::Refused(reason),
                OutboxRequestStatus::NotFound
                | OutboxRequestStatus::InOutbox { .. }
                | OutboxRequestStatus::Sent
                | OutboxRequestStatus::Ended { .. } => {
                    return Err(corrupt_row(&request_id, "item_state"));
                }
            };
            outgoing.push(OutgoingWrite {
                request_id: parse_request_id(&request_id)?,
                target: answered.target,
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

    /// Claims without an answer, including ones recovered after reopen.
    pub async fn sent_without_answer(
        &mut self,
    ) -> Result<StoreScan<Vec<RequestId>>, OutboxStoreError> {
        Ok(read_rows_in_states(
            &mut self.store.connection,
            OutboxItemState::Sent,
            OutboxItemState::Resent,
        )
        .await?
        .map_healthy(|rows| {
            let mut requests: Vec<_> = rows.into_iter().map(|row| row.request).collect();
            requests.sort();
            requests
        }))
    }

    pub async fn earliest_waiting(&mut self) -> Result<StoreScan<Option<i64>>, OutboxStoreError> {
        Ok(waiting_rows(&mut self.store.connection)
            .await?
            .map_healthy(|rows| rows.into_iter().map(|row| row.flush_at_ms).min()))
    }
}
