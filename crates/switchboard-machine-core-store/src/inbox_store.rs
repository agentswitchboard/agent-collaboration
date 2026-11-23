//! Durable delivery custody in machine_local.sqlite.
use crate::{MachineLocalStore, RowCorruption, RowCorruptionReason, StoreScan};
use serde::{Deserialize, Serialize};
use sqlite_storage_maintenance::TransactionStart;
use sqlx::SqliteConnection;
use workspace_ids::MessageId;
use workspace_model::central_mail::MessageRecord;

const RETENTION_MILLIS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, thiserror::Error)]
pub enum InboxStoreError {
    #[error("inbox storage unavailable")]
    Database(#[from] sqlx::Error),
    #[error("inbox state could not be encoded")]
    Encode(#[from] serde_json::Error),
    #[error("inbox message {message_id} has invalid stored {field}")]
    Corrupt {
        message_id: String,
        field: &'static str,
    },
    #[error("inbox message {message_id} was not {expected}")]
    UnexpectedState {
        message_id: MessageId,
        expected: &'static str,
    },
}

impl From<RowCorruption> for InboxStoreError {
    fn from(row: RowCorruption) -> Self {
        Self::Corrupt {
            message_id: row.key,
            field: row.field,
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RecordDelivery {
    Inserted,
    Duplicate,
}

/// The inbox's durable decision, independent of the wire receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum InboxDecision {
    Accepted,
    Refused { reason: InboxRefusalReason },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InboxRefusalReason {
    UnknownSession,
    AmbiguousSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxReceipt {
    pub message_id: MessageId,
    pub decision: InboxDecision,
    pub received_at_ms: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InboxRowState {
    decision: InboxDecision,
    receipt: ReceiptConfirmation,
    ended_at_ms: Option<i64>,
}

/// Handoff and central's receipt confirmation are independent.
#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum ReceiptConfirmation {
    Pending,
    Confirmed,
}

struct StoredDelivery {
    message_id: MessageId,
    created_at: i64,
    message: Option<MessageRecord>,
    state: InboxRowState,
}

impl StoredDelivery {
    fn into_receipt(self) -> InboxReceipt {
        InboxReceipt {
            message_id: self.message_id,
            decision: self.state.decision,
            received_at_ms: self.created_at,
        }
    }

    fn end_if_complete(&mut self, now_ms: i64) {
        if self.message.is_none() && self.state.receipt == ReceiptConfirmation::Confirmed {
            self.state.ended_at_ms.get_or_insert(now_ms);
        }
    }
}

struct InboxRow {
    message_id: String,
    created_at: i64,
    updated_at: i64,
    message_json: Option<String>,
    delivery_state_json: String,
}

impl InboxRow {
    fn decode(self) -> Result<StoredDelivery, RowCorruption> {
        use RowCorruptionReason::{
            InconsistentState, InvalidIdentifier, InvalidJson, KeyMismatch, TimestampOutOfRange,
        };
        let corrupt = |field, reason| RowCorruption {
            key: self.message_id.clone(),
            field,
            reason,
        };
        let message_id: MessageId = self
            .message_id
            .parse()
            .map_err(|_| corrupt("message_id", InvalidIdentifier))?;
        for (value, field) in [
            (self.created_at, "created_at"),
            (self.updated_at, "updated_at"),
        ] {
            workspace_model::Timestamp::from_timestamp_millis(value)
                .ok_or_else(|| corrupt(field, TimestampOutOfRange))?;
        }
        let state: InboxRowState = serde_json::from_str(&self.delivery_state_json)
            .map_err(|_| corrupt("delivery_state_json", InvalidJson))?;
        let message: Option<MessageRecord> = self
            .message_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|_| corrupt("message_json", InvalidJson))?;
        if let Some(message) = &message {
            if message.message_id != message_id {
                return Err(corrupt("message_json.message_id", KeyMismatch));
            }
            if state.decision != InboxDecision::Accepted {
                return Err(corrupt("message_json", InconsistentState));
            }
        }
        if state.ended_at_ms.is_some()
            != (message.is_none() && state.receipt == ReceiptConfirmation::Confirmed)
            || state
                .ended_at_ms
                .is_some_and(|ended| ended != self.updated_at)
        {
            return Err(corrupt(
                "delivery_state_json.ended_at_ms",
                InconsistentState,
            ));
        }
        Ok(StoredDelivery {
            message_id,
            created_at: self.created_at,
            message,
            state,
        })
    }
}

pub struct InboxStore {
    store: MachineLocalStore,
}

impl InboxStore {
    pub const fn new(store: MachineLocalStore) -> Self {
        Self { store }
    }

    /// A repeat can use the original decision without looking up the recipient again.
    /// An empty lookup with a diagnostic identifies a damaged existing row.
    pub async fn receipt_for(
        &mut self,
        message_id: &MessageId,
    ) -> Result<StoreScan<Option<InboxReceipt>>, InboxStoreError> {
        let row = read_delivery_row(&mut self.store.connection, message_id).await?;
        match row.map(InboxRow::decode).transpose() {
            Ok(delivery) => Ok(StoreScan {
                healthy: delivery.map(StoredDelivery::into_receipt),
                corrupt_rows: Vec::new(),
            }),
            Err(corruption) => Ok(StoreScan {
                healthy: None,
                corrupt_rows: vec![corruption],
            }),
        }
    }

    /// Commits the first decision and any retained payload in one transaction.
    pub async fn record_delivery(
        &mut self,
        message: &MessageRecord,
        decision: InboxDecision,
        now_ms: i64,
    ) -> Result<RecordDelivery, InboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let recorded = match read_delivery(&mut transaction, &message.message_id).await? {
            None => {
                let payload = (decision == InboxDecision::Accepted)
                    .then(|| serde_json::to_string(message))
                    .transpose()?;
                let state = serde_json::to_string(&InboxRowState {
                    decision,
                    receipt: ReceiptConfirmation::Pending,
                    ended_at_ms: None,
                })?;
                sqlx::query!(
                    "INSERT INTO inbox_messages
                     (message_id, message_json, delivery_state_json, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?)",
                    message.message_id.as_str(),
                    payload,
                    state,
                    now_ms,
                    now_ms,
                )
                .execute(&mut *transaction)
                .await?;
                RecordDelivery::Inserted
            }
            Some(_) => RecordDelivery::Duplicate,
        };
        transaction.commit().await?;
        Ok(recorded)
    }

    pub async fn record_handoff(
        &mut self,
        message_id: &MessageId,
        now_ms: i64,
    ) -> Result<(), InboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut delivery = read_delivery(&mut transaction, message_id)
            .await?
            .ok_or_else(|| unexpected(message_id, "pending handoff"))?;
        if delivery.message.take().is_none() {
            return Err(unexpected(message_id, "pending handoff"));
        }
        delivery.end_if_complete(now_ms);
        update_delivery(&mut transaction, &delivery, now_ms).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Records central's confirmation of the receipt.
    pub async fn mark_receipt_confirmed(
        &mut self,
        message_id: &MessageId,
        now_ms: i64,
    ) -> Result<(), InboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut delivery = read_delivery(&mut transaction, message_id)
            .await?
            .ok_or_else(|| unexpected(message_id, "pending receipt"))?;
        if delivery.state.receipt != ReceiptConfirmation::Pending {
            return Err(unexpected(message_id, "pending receipt"));
        }
        delivery.state.receipt = ReceiptConfirmation::Confirmed;
        delivery.end_if_complete(now_ms);
        update_delivery(&mut transaction, &delivery, now_ms).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn pending_handoffs(
        &mut self,
    ) -> Result<StoreScan<Vec<MessageRecord>>, InboxStoreError> {
        let rows = sqlx::query_as!(
            InboxRow,
            "SELECT message_id, created_at, updated_at, message_json, delivery_state_json FROM inbox_messages
             WHERE message_json IS NOT NULL ORDER BY created_at, message_id",
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        Ok(rows
            .into_iter()
            .map(InboxRow::decode)
            .collect::<StoreScan<Vec<_>>>()
            .map_healthy(|rows| {
                rows.into_iter()
                    .filter_map(|delivery| delivery.message)
                    .collect()
            }))
    }

    /// Decisions still awaiting confirmation, in first-receipt order.
    pub async fn pending_receipts(
        &mut self,
    ) -> Result<StoreScan<Vec<InboxReceipt>>, InboxStoreError> {
        let rows = sqlx::query_as!(
            InboxRow,
            "SELECT message_id, created_at, updated_at, message_json, delivery_state_json FROM inbox_messages
             WHERE CASE WHEN json_valid(delivery_state_json)
                        THEN json_extract(delivery_state_json, '$.receipt.kind') IS NOT 'confirmed'
                        ELSE 1 END
             ORDER BY created_at, message_id",
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.decode().map(StoredDelivery::into_receipt))
            .collect())
    }

    /// First time an ended row is eligible for pruning, in Unix milliseconds.
    pub async fn next_prune_at_ms(&mut self) -> Result<StoreScan<Option<i64>>, InboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Deferred).await?;
        let scan = retention_candidates(&mut transaction).await?;
        transaction.commit().await?;
        Ok(scan.map_healthy(|rows| {
            rows.into_iter()
                .filter_map(|delivery| delivery.state.ended_at_ms)
                .min()
                .and_then(|ended| ended.checked_add(RETENTION_MILLIS + 1))
        }))
    }

    pub async fn prune_ended(&mut self, now_ms: i64) -> Result<StoreScan<u64>, InboxStoreError> {
        let cutoff = now_ms.saturating_sub(RETENTION_MILLIS);
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let StoreScan {
            healthy,
            corrupt_rows,
        } = retention_candidates(&mut transaction).await?;
        let mut removed = 0;
        for delivery in healthy {
            if delivery
                .state
                .ended_at_ms
                .is_some_and(|ended| ended < cutoff)
            {
                removed += sqlx::query!(
                    "DELETE FROM inbox_messages WHERE message_id = ?",
                    delivery.message_id.as_str()
                )
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            }
        }
        transaction.commit().await?;
        Ok(StoreScan {
            healthy: removed,
            corrupt_rows,
        })
    }
}

async fn retention_candidates(
    connection: &mut SqliteConnection,
) -> Result<StoreScan<Vec<StoredDelivery>>, InboxStoreError> {
    // Include malformed JSON and incomplete completion markers so the row
    // decoder can report corruption before any custody is removed.
    Ok(sqlx::query_as!(
        InboxRow,
        "SELECT message_id, created_at, updated_at, message_json, delivery_state_json FROM inbox_messages
         WHERE CASE WHEN json_valid(delivery_state_json)
               THEN json_type(delivery_state_json, '$.ended_at_ms') IS NOT 'null'
                    OR (message_json IS NULL AND json_extract(delivery_state_json, '$.receipt.kind') = 'confirmed')
               ELSE 1 END",
    ).fetch_all(connection).await?.into_iter().map(InboxRow::decode).collect())
}

async fn read_delivery(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<Option<StoredDelivery>, InboxStoreError> {
    Ok(read_delivery_row(connection, message_id)
        .await?
        .map(InboxRow::decode)
        .transpose()?)
}

async fn read_delivery_row(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<Option<InboxRow>, InboxStoreError> {
    let key = message_id.as_str();
    Ok(sqlx::query_as!(
        InboxRow,
        "SELECT message_id, created_at, updated_at, message_json, delivery_state_json FROM inbox_messages
         WHERE message_id = ?",
        key,
    )
    .fetch_optional(connection)
    .await?)
}

async fn update_delivery(
    connection: &mut SqliteConnection,
    delivery: &StoredDelivery,
    now_ms: i64,
) -> Result<(), InboxStoreError> {
    let clear_message = delivery.message.is_none();
    let state = serde_json::to_string(&delivery.state)?;
    sqlx::query!(
        "UPDATE inbox_messages SET message_json = CASE WHEN ? THEN NULL ELSE message_json END,
            delivery_state_json = ?, updated_at = ? WHERE message_id = ?",
        clear_message,
        state,
        now_ms,
        delivery.message_id.as_str(),
    )
    .execute(connection)
    .await?;
    Ok(())
}

fn unexpected(message_id: &MessageId, expected: &'static str) -> InboxStoreError {
    InboxStoreError::UnexpectedState {
        message_id: message_id.clone(),
        expected,
    }
}

#[cfg(test)]
#[path = "inbox_store_test.rs"]
mod tests;
