//! The Mac outbox in machine_local.sqlite (owner 2026-11-15, record:210). Every
//! time comes from the caller's injected clock; stored values are decoded
//! through Rust types and an invalid row is refused, never coerced.
use crate::{
    LocalOutboxRefusal, MachineLocalStore, OutboxQueuedWrite, OutboxRequestStatus,
    outbox_item_state::OutboxItemState,
};
use sqlite_storage_maintenance::TransactionStart;
use workspace_ids::RequestId;
use workspace_model::{Caller, Timestamp};

mod outbox_queries;
mod outbox_rows;
mod outbox_transitions;
mod sender_outcome_store;

use outbox_rows::{
    OutboxRow, StoredPayload, decode_row, outbox_content, payload_content_digest,
    read_decoded_outbox,
};
pub use outbox_transitions::ClaimedBatch;

#[derive(Debug, thiserror::Error)]
pub enum OutboxStoreError {
    #[error("central delivery outcome does not match request {request}")]
    DeliveryOutcomeMismatch { request: RequestId },
    #[error("the outbox insert did not start")]
    InsertNotStarted(#[source] sqlx::Error),
    #[error("outbox storage unavailable")]
    Database(#[from] sqlx::Error),
    #[error("outbox write could not be encoded")]
    Encode(#[from] serde_json::Error),
    #[error("outbox item {request_id} has an invalid stored {field}")]
    CorruptItem {
        request_id: String,
        field: &'static str,
    },
    #[error("outbox write content could not be digested")]
    Digest,
    #[error("the payload does not carry request id {request}")]
    RequestIdMismatch { request: RequestId },
    #[error("outbox item {request} is not {expected}")]
    UnexpectedState {
        request: RequestId,
        expected: &'static str,
    },
}

/// A write entering the outbox, with the mark its receipt promises.
#[derive(Clone, Debug)]
pub struct NewOutboxItem {
    pub request: RequestId,
    pub caller: Caller,
    pub payload: OutboxQueuedWrite,
    pub flush_at_ms: i64,
}

#[derive(Debug, Eq, PartialEq)]
pub enum OutboxInsert {
    Stored,
    /// The same caller already stored this exact write.
    AlreadyStored(OutboxRequestStatus),
    /// The id is taken by a different write or another caller.
    Refused(LocalOutboxRefusal),
}

/// A write loaded for sending in a batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOutboxWrite {
    pub request: RequestId,
    pub caller: Caller,
    pub payload: OutboxQueuedWrite,
    pub sent_at: Timestamp,
}

/// Answered writes are kept this long after their answer, then deleted.
pub(crate) const ANSWERED_RETENTION_MILLIS: i64 = 60 * 24 * 60 * 60 * 1000;

/// The outbox table, borrowed from the machine-local store whose
/// connection and signalled transactions it uses.
pub struct MacOutbox<'store> {
    store: &'store mut MachineLocalStore,
}

impl<'store> MacOutbox<'store> {
    pub const fn new(store: &'store mut MachineLocalStore) -> Self {
        Self { store }
    }

    /// Stores a write in the outbox, or reports the earlier identical write.
    pub async fn insert(
        &mut self,
        item: &NewOutboxItem,
        now_ms: i64,
    ) -> Result<OutboxInsert, OutboxStoreError> {
        if item.payload.request_id() != Some(&item.request) {
            return Err(OutboxStoreError::RequestIdMismatch {
                request: item.request.clone(),
            });
        }
        let request_id = item.request.as_str();
        let caller_json = serde_json::to_string(&item.caller)?;
        let payload_json = serde_json::to_string(&item.payload)?;
        let state = OutboxItemState::InOutbox.as_str();
        let flush_at_ms = item.flush_at_ms;
        let mut transaction = self
            .store
            .transaction(TransactionStart::Immediate)
            .await
            .map_err(OutboxStoreError::InsertNotStarted)?;
        let existing = sqlx::query_as!(
            OutboxRow,
            r#"SELECT request_id AS "request_id!", caller_json, payload_json, item_state,
                      flush_at_ms, outcome_json
               FROM outbox_items WHERE request_id = ?"#,
            request_id
        )
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(row) = existing {
            let stored = decode_row(&row)?;
            let same_write = match &stored.payload {
                StoredPayload::Unanswered(payload) => payload == &item.payload,
                // The text is gone once answered. The same write (replay its answer)
                // has the same target, workspace included, and the same content
                // digest; any difference is a conflict. The outbox spans workspaces.
                StoredPayload::Answered(answered) => {
                    item.payload
                        .content()
                        .is_some_and(|content| content.target == answered.target)
                        && payload_content_digest(&item.payload)? == answered.content_digest
                }
            };
            return Ok(if stored.caller == item.caller && same_write {
                OutboxInsert::AlreadyStored(stored.status)
            } else {
                OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict)
            });
        }
        // The creation stamp is the agent's send time (design 2026-11-17).
        outbox_content(request_id, &item.payload)?;
        let expires_at_ms = crate::outbox_deadline::expires_at_ms(now_ms);
        sqlx::query!(
            "INSERT INTO outbox_items
               (request_id, caller_json, payload_json, item_state, flush_at_ms, expires_at_ms,
                outcome_json, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            request_id,
            caller_json,
            payload_json,
            state,
            flush_at_ms,
            expires_at_ms,
            now_ms,
            now_ms
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(OutboxInsert::Stored)
    }

    /// The caller's own write; another caller's id answers NotFound.
    pub async fn request_status(
        &mut self,
        caller: &Caller,
        request: &RequestId,
    ) -> Result<OutboxRequestStatus, OutboxStoreError> {
        let Some(stored) = read_decoded_outbox(&mut self.store.connection, request).await? else {
            return Ok(OutboxRequestStatus::NotFound);
        };
        Ok(if stored.caller == *caller {
            stored.status
        } else {
            OutboxRequestStatus::NotFound
        })
    }
}
#[cfg(test)]
#[path = "outbox_store_test.rs"]
mod tests;
