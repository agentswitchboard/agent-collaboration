//! Caller-scoped reads and selection of pending writes.
use super::{
    MacOutbox, OutboxStoreError,
    outbox_rows::{
        OutboxRow, OutgoingRow, StoredPayload, corrupt_row, decode_owned_row, outbox_content,
        parse_request_id, read_decoded_outbox,
    },
};
use crate::{
    OutboxRequestStatus, OutgoingOutcome, OutgoingWrite, PendingWrite, PendingWriteState,
    outbox_item_state::OutboxItemState,
};
use central_link_protocol::RequestStatusQuery;
use workspace_ids::RequestId;
use workspace_model::Caller;

impl MacOutbox<'_> {
    pub async fn scoped_request_status(
        &mut self,
        caller: &Caller,
        query: &RequestStatusQuery,
    ) -> Result<OutboxRequestStatus, OutboxStoreError> {
        let Some(stored) =
            read_decoded_outbox(&mut self.store.connection, &query.request_id).await?
        else {
            return Ok(OutboxRequestStatus::NotFound);
        };
        if stored.caller != *caller {
            return Ok(OutboxRequestStatus::NotFound);
        }
        let scope = match &stored.payload {
            StoredPayload::Unanswered(write) => write.request_scope(),
            StoredPayload::Answered(profile) => profile.target.request_scope(),
        };
        Ok(if scope == query.scope {
            stored.status
        } else {
            OutboxRequestStatus::NotFound
        })
    }

    /// The earliest expiry among waiting writes, even without a connection.
    pub async fn earliest_deadline(&mut self) -> Result<Option<i64>, OutboxStoreError> {
        let waiting = OutboxItemState::InOutbox.as_str();
        let answer_lost = OutboxItemState::SentAnswerLost.as_str();
        Ok(sqlx::query_scalar!(
            r#"SELECT min(expires_at_ms) AS "deadline?: i64" FROM outbox_items
               WHERE item_state IN (?, ?)"#,
            waiting,
            answer_lost
        )
        .fetch_one(&mut self.store.connection)
        .await?)
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
                      flush_at_ms, outcome_json
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
            OutgoingRow,
            r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                      flush_at_ms, outcome_json, updated_at
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
            let answered_at_ms = row.updated_at;
            let row = OutboxRow {
                request_id: row.request_id,
                caller_json: row.caller_json,
                payload_json: row.payload_json,
                item_state: row.item_state,
                flush_at_ms: row.flush_at_ms,
                outcome_json: row.outcome_json,
            };
            let stored = decode_owned_row(&row, caller)?;
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
    pub async fn waiting(&mut self) -> Result<Vec<(i64, RequestId)>, OutboxStoreError> {
        let unsent = OutboxItemState::InOutbox.as_str();
        let answer_lost = OutboxItemState::SentAnswerLost.as_str();
        let rows = sqlx::query!(
            r#"SELECT request_id AS "request_id!", flush_at_ms FROM outbox_items
               WHERE item_state IN (?, ?) ORDER BY flush_at_ms, request_id"#,
            unsent,
            answer_lost
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        rows.into_iter()
            .map(|row| Ok((row.flush_at_ms, parse_request_id(&row.request_id)?)))
            .collect()
    }

    /// Writes sent whose answer never came, e.g. across a restart; the Host
    /// returns them to the outbox to resend under the same RequestId.
    pub async fn sent_without_answer(&mut self) -> Result<Vec<RequestId>, OutboxStoreError> {
        let sent = OutboxItemState::Sent.as_str();
        let resent = OutboxItemState::Resent.as_str();
        let rows = sqlx::query_scalar!(
            r#"SELECT request_id AS "request_id!" FROM outbox_items
               WHERE item_state IN (?, ?) ORDER BY request_id"#,
            sent,
            resent
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        rows.iter().map(|id| parse_request_id(id)).collect()
    }
    /// The earliest receipt mark among waiting writes, for arming the flush.
    pub async fn earliest_waiting(&mut self) -> Result<Option<i64>, OutboxStoreError> {
        let unsent = OutboxItemState::InOutbox.as_str();
        let answer_lost = OutboxItemState::SentAnswerLost.as_str();
        let earliest = sqlx::query_scalar!(
            r#"SELECT MIN(flush_at_ms) AS "earliest: i64" FROM outbox_items
               WHERE item_state IN (?, ?)"#,
            unsent,
            answer_lost
        )
        .fetch_one(&mut self.store.connection)
        .await?;
        Ok(earliest)
    }
}
