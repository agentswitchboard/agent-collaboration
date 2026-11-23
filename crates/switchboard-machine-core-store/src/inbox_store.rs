//! Durable delivery custody in machine_local.sqlite.
use crate::{
    MachineLocalStore, RowCorruption, RowCorruptionReason, StoreScan, store_scan::diagnostic_key,
};
use serde::{Deserialize, Serialize};
use sqlite_storage_maintenance::TransactionStart;
use sqlx::SqliteConnection;
use std::collections::{BTreeMap, BTreeSet};
use workspace_ids::MessageId;
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

const INBOX_RETENTION_MILLIS: i64 = 60 * 24 * 60 * 60 * 1_000;
/// The inbox store uses the longest delivery window even when the body is absent.
/// This must equal the wire's TimeLeft bound. The store is independent of link protocol types.
pub const MAX_INBOX_DELIVERY_WINDOW_MS: u32 = 15 * 60 * 1_000;

/// The inbox store reports failures to read or change durable custody.
#[derive(Debug, thiserror::Error)]
pub enum InboxStoreError {
    #[error("inbox timestamp {field} is out of range")]
    InvalidTimestamp { field: &'static str },
    #[error("the retained payload does not match its offered inbox family")]
    OfferFamilyMismatch,
    #[error("direct inbox expiry must follow arrival by at most fifteen minutes")]
    InvalidExpiry,
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

/// Custody supplies the expiry for a validated direct offer or the system offer's lack of expiry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxOfferExpiry {
    DirectWithExpiry(Timestamp),
    SystemWithoutExpiry,
}

impl InboxOfferExpiry {
    fn into_parts(self) -> (InboxOfferFamily, Option<Timestamp>) {
        match self {
            Self::DirectWithExpiry(expires_at) => (InboxOfferFamily::Direct, Some(expires_at)),
            Self::SystemWithoutExpiry => (InboxOfferFamily::System, None),
        }
    }
}

impl From<RowCorruption> for InboxStoreError {
    fn from(row: RowCorruption) -> Self {
        Self::Corrupt {
            message_id: row.key,
            field: row.field,
        }
    }
}

/// The inbox store distinguishes the first insert from a repeated offer.
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

/// A recipient's durable reason for refusing a Central delivery.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum InboxRefusalReason {
    UnknownSession,
    AmbiguousSession,
}

/// The store supplies the recipient decision and its original arrival for Central's receipt.
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
    handoff: HandoffState,
    family: InboxOfferFamily,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum InboxOfferFamily {
    Direct,
    System,
}

impl InboxOfferFamily {
    fn matches_payload(self, body: &MessageBody) -> bool {
        matches!(
            (self, body),
            (Self::Direct, MessageBody::Direct { .. })
                | (
                    Self::System,
                    MessageBody::Notice { .. } | MessageBody::DeliveryOutcome { .. }
                )
        )
    }

    fn valid_expiry(self, arrival_ms: i64, expires_at: Option<Timestamp>) -> bool {
        match (self, expires_at) {
            (Self::Direct, Some(expiry)) => (1..=i64::from(MAX_INBOX_DELIVERY_WINDOW_MS))
                .contains(&expiry.timestamp_millis().saturating_sub(arrival_ms)),
            (Self::System, None) => true,
            (Self::Direct, None) | (Self::System, Some(_)) => false,
        }
    }
}

/// Handoff and central's receipt confirmation are independent.
#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum ReceiptConfirmation {
    Pending,
    Confirmed,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum HandoffState {
    Pending,
    Completed,
    NotRequired,
}

struct StoredDelivery {
    message_id: MessageId,
    created_at: i64,
    message: Option<MessageRecord>,
    expires_at: Option<Timestamp>,
    state: InboxRowState,
}

struct DeliveryHeader {
    message_id: MessageId,
    created_at: i64,
    expires_at: Option<Timestamp>,
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
}

struct InboxRow {
    message_id: Vec<u8>,
    created_at: i64,
    updated_at: i64,
    expires_at: Option<i64>,
    message_json: Option<Vec<u8>>,
    has_message: bool,
    delivery_state_json: Vec<u8>,
    key_is_text: bool,
    integer_fault: i64,
}

// Read SQLite TEXT as bytes before interpreting a stored delivery.
macro_rules! select_inbox_rows {
    ($payload:literal, $clause:literal $(, $argument:expr)* $(,)?) => {
        sqlx::query_as!(
            InboxRow,
            r#"SELECT CAST(message_id AS BLOB) AS "message_id!: Vec<u8>", created_at, updated_at,
                    CAST(expires_at AS INTEGER) AS "expires_at?: i64",
                    CAST("# + $payload + r#" AS BLOB) AS "message_json?: Vec<u8>",
                    (message_json IS NOT NULL) AS "has_message!: bool",
                    CAST(delivery_state_json AS BLOB) AS "delivery_state_json!: Vec<u8>",
                    (typeof(message_id) = 'text') AS "key_is_text!: bool",
                    CASE WHEN typeof(created_at) <> 'integer' THEN 1
                         WHEN typeof(updated_at) <> 'integer' THEN 2
                         WHEN typeof(expires_at) NOT IN ('integer', 'null') THEN 3
                         ELSE 0 END AS "integer_fault!: i64"
             FROM inbox_messages "# + $clause
            $(, $argument)*
        )
    };
}

impl InboxRow {
    fn fault(&self, field: &'static str, reason: RowCorruptionReason) -> RowCorruption {
        RowCorruption {
            key: diagnostic_key(&self.message_id),
            field,
            reason,
        }
    }

    fn decode_header(&self) -> Result<DeliveryHeader, RowCorruption> {
        use RowCorruptionReason::{
            InconsistentState, InvalidIdentifier, InvalidJson, InvalidStorageType,
            TimestampOutOfRange,
        };
        let corrupt = |field, reason| self.fault(field, reason);
        if !self.key_is_text {
            return Err(corrupt("message_id", InvalidIdentifier));
        }
        let message_id: MessageId = std::str::from_utf8(&self.message_id)
            .ok()
            .and_then(|key| key.parse().ok())
            .ok_or_else(|| corrupt("message_id", InvalidIdentifier))?;
        let invalid_integer = match self.integer_fault {
            1 => Some("created_at"),
            2 => Some("updated_at"),
            3 => Some("expires_at"),
            _ => None,
        };
        if let Some(field) = invalid_integer {
            return Err(corrupt(field, InvalidStorageType));
        }
        for (value, field) in [
            (self.created_at, "created_at"),
            (self.updated_at, "updated_at"),
        ] {
            Timestamp::from_timestamp_millis(value)
                .ok_or_else(|| corrupt(field, TimestampOutOfRange))?;
        }
        let expires_at = self
            .expires_at
            .map(|value| {
                Timestamp::from_timestamp_millis(value)
                    .ok_or_else(|| corrupt("expires_at", TimestampOutOfRange))
            })
            .transpose()?;
        let state: InboxRowState = serde_json::from_slice(&self.delivery_state_json)
            .map_err(|_| corrupt("delivery_state_json", InvalidJson))?;
        if !state.family.valid_expiry(self.created_at, expires_at) {
            return Err(corrupt("expires_at", InconsistentState));
        }
        if !matches!(
            (state.decision, state.handoff, self.has_message),
            (
                InboxDecision::Accepted,
                HandoffState::Pending | HandoffState::Completed,
                true
            ) | (
                InboxDecision::Refused { .. },
                HandoffState::NotRequired,
                false
            )
        ) {
            return Err(corrupt("delivery_state_json.handoff", InconsistentState));
        }
        Ok(DeliveryHeader {
            message_id,
            created_at: self.created_at,
            expires_at,
            state,
        })
    }

    fn decode(self) -> Result<StoredDelivery, RowCorruption> {
        let header = self.decode_header()?;
        let message: Option<MessageRecord> = self
            .message_json
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()
            .map_err(|_| self.fault("message_json", RowCorruptionReason::InvalidJson))?;
        if let Some(message) = &message {
            if message.message_id != header.message_id {
                return Err(self.fault("message_json.message_id", RowCorruptionReason::KeyMismatch));
            }
            if !header.state.family.matches_payload(&message.body) {
                return Err(self.fault(
                    "message_json.body.kind",
                    RowCorruptionReason::InconsistentState,
                ));
            }
        }
        Ok(StoredDelivery {
            message_id: header.message_id,
            created_at: header.created_at,
            expires_at: header.expires_at,
            state: header.state,
            message,
        })
    }
}

/// The Mac opens this store to keep Central deliveries and their custody state.
pub struct InboxStore {
    store: MachineLocalStore,
    #[cfg(all(feature = "test-support", debug_assertions))]
    held_commit: Option<crate::inbox_commit_pause::InboxCommitPause>,
    // Payloads are immutable. A failed due-row decode must not leave an
    // already elapsed retention wake armed for the lifetime of this store.
    corrupt_retention_payloads: BTreeSet<MessageId>,
}

impl InboxStore {
    pub const fn new(store: MachineLocalStore) -> Self {
        Self {
            store,
            #[cfg(all(feature = "test-support", debug_assertions))]
            held_commit: None,
            corrupt_retention_payloads: BTreeSet::new(),
        }
    }

    /// Pause the next new delivery after its insert and immediately before commit.
    #[cfg(all(feature = "test-support", debug_assertions))]
    pub fn hold_next_commit(&mut self) -> crate::HeldInboxCommit {
        let (pause, control) = crate::inbox_commit_pause::InboxCommitPause::new();
        self.held_commit = Some(pause);
        control
    }

    /// A repeat can use the original decision without looking up the recipient again.
    /// An empty lookup with a diagnostic identifies a damaged existing row.
    pub async fn receipt_for(
        &mut self,
        message_id: &MessageId,
    ) -> Result<StoreScan<Option<InboxReceipt>>, InboxStoreError> {
        self.project_delivery(message_id, |delivery| Some(delivery.into_receipt()))
            .await
    }

    /// The original direct-message cutoff. System offers have no expiry.
    /// A damaged row yields a diagnostic instead of an instant.
    pub async fn expiry_for(
        &mut self,
        message_id: &MessageId,
    ) -> Result<StoreScan<Option<Timestamp>>, InboxStoreError> {
        self.project_delivery(message_id, |delivery| delivery.expires_at)
            .await
    }

    /// A kept payload remains readable after hand-off until retention removes the row.
    pub async fn message_for(
        &mut self,
        message_id: &MessageId,
    ) -> Result<StoreScan<Option<MessageRecord>>, InboxStoreError> {
        self.project_delivery(message_id, |delivery| delivery.message)
            .await
    }

    async fn project_delivery<Value>(
        &mut self,
        message_id: &MessageId,
        project: impl FnOnce(StoredDelivery) -> Option<Value>,
    ) -> Result<StoreScan<Option<Value>>, InboxStoreError> {
        let row = read_delivery_row(&mut self.store.connection, message_id).await?;
        match row.map(InboxRow::decode).transpose() {
            Ok(delivery) => Ok(StoreScan {
                healthy: delivery.and_then(project),
                corrupt_rows: Vec::new(),
            }),
            Err(corruption) => Ok(StoreScan {
                healthy: None,
                corrupt_rows: vec![corruption],
            }),
        }
    }

    /// Commits the first decision, payload and expiry in one transaction.
    pub async fn record_delivery(
        &mut self,
        message: &MessageRecord,
        decision: InboxDecision,
        arrival_ms: i64,
        expiry: InboxOfferExpiry,
    ) -> Result<RecordDelivery, InboxStoreError> {
        validate_write_timestamp(arrival_ms, "created_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let recorded = match read_delivery(&mut transaction, &message.message_id).await? {
            None => {
                let (family, expires_at) = expiry.into_parts();
                if !family.matches_payload(&message.body) {
                    return Err(InboxStoreError::OfferFamilyMismatch);
                }
                if !family.valid_expiry(arrival_ms, expires_at) {
                    return Err(InboxStoreError::InvalidExpiry);
                }
                let expires_at_ms = expires_at.map(|instant| instant.timestamp_millis());
                let payload = (decision == InboxDecision::Accepted)
                    .then(|| serde_json::to_string(message))
                    .transpose()?;
                let state = serde_json::to_string(&InboxRowState {
                    decision,
                    receipt: ReceiptConfirmation::Pending,
                    family,
                    handoff: match decision {
                        InboxDecision::Accepted => HandoffState::Pending,
                        InboxDecision::Refused { .. } => HandoffState::NotRequired,
                    },
                })?;
                sqlx::query!(
                    "INSERT INTO inbox_messages
                     (message_id, message_json, delivery_state_json, created_at, updated_at, expires_at)
                     VALUES (?, ?, ?, ?, ?, ?)",
                    message.message_id.as_str(),
                    payload,
                    state,
                    arrival_ms,
                    arrival_ms,
                    expires_at_ms,
                )
                .execute(&mut *transaction)
                .await?;
                RecordDelivery::Inserted
            }
            Some(_) => RecordDelivery::Duplicate,
        };
        #[cfg(all(feature = "test-support", debug_assertions))]
        if recorded == RecordDelivery::Inserted
            && let Some(pause) = self.held_commit.take()
        {
            pause.wait_for_release().await;
        }
        transaction.commit().await?;
        Ok(recorded)
    }

    pub async fn record_handoff(
        &mut self,
        message_id: &MessageId,
        now_ms: i64,
    ) -> Result<(), InboxStoreError> {
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut delivery = read_delivery(&mut transaction, message_id)
            .await?
            .ok_or_else(|| unexpected(message_id, "pending handoff"))?;
        if delivery.state.handoff != HandoffState::Pending {
            return Err(unexpected(message_id, "pending handoff"));
        }
        delivery.state.handoff = HandoffState::Completed;
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
        validate_write_timestamp(now_ms, "updated_at")?;
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut delivery = read_delivery(&mut transaction, message_id)
            .await?
            .ok_or_else(|| unexpected(message_id, "pending receipt"))?;
        if delivery.state.receipt != ReceiptConfirmation::Pending {
            return Err(unexpected(message_id, "pending receipt"));
        }
        delivery.state.receipt = ReceiptConfirmation::Confirmed;
        update_delivery(&mut transaction, &delivery, now_ms).await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn pending_handoffs(
        &mut self,
    ) -> Result<StoreScan<Vec<MessageRecord>>, InboxStoreError> {
        let rows = select_inbox_rows!(
            "message_json",
            r#"WHERE CASE WHEN json_valid(delivery_state_json)
               THEN json_extract(delivery_state_json, '$.handoff.kind') IS NOT 'completed'
                    AND (message_json IS NOT NULL
                         OR json_extract(delivery_state_json, '$.handoff.kind') IS NOT 'notRequired')
               ELSE 1 END
             ORDER BY created_at, message_id"#,
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        Ok(rows
            .into_iter()
            .map(InboxRow::decode)
            .collect::<StoreScan<Vec<_>>>()
            .map_healthy(|rows| {
                rows.into_iter()
                    .filter(|delivery| delivery.state.handoff == HandoffState::Pending)
                    .filter_map(|delivery| delivery.message)
                    .collect()
            }))
    }

    /// Decisions still awaiting confirmation, in first-receipt order.
    pub async fn pending_receipts(
        &mut self,
    ) -> Result<StoreScan<Vec<InboxReceipt>>, InboxStoreError> {
        let rows = select_inbox_rows!(
            "message_json",
            r#"WHERE CASE WHEN json_valid(delivery_state_json)
                        THEN json_extract(delivery_state_json, '$.receipt.kind') IS NOT 'confirmed'
                        ELSE 1 END
             ORDER BY created_at, message_id"#,
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| row.decode().map(StoredDelivery::into_receipt))
            .collect())
    }

    /// First time a receipt-confirmed row passes its arrival-based retention, in Unix milliseconds.
    pub async fn next_prune_at_ms(&mut self) -> Result<StoreScan<Option<i64>>, InboxStoreError> {
        let mut transaction = self.store.transaction(TransactionStart::Deferred).await?;
        let scan = retention_candidates(&mut transaction).await?;
        transaction.commit().await?;
        Ok(scan.map_healthy(|rows| {
            rows.into_iter()
                .filter(|delivery| {
                    !self
                        .corrupt_retention_payloads
                        .contains(&delivery.message_id)
                })
                .map(|delivery| delivery.created_at)
                .min()
                .and_then(|arrival| arrival.checked_add(INBOX_RETENTION_MILLIS))
        }))
    }

    /// Prune rows at the arrival retention boundary once Central confirms the receipt.
    pub async fn prune_past_retention(
        &mut self,
        now_ms: i64,
    ) -> Result<StoreScan<u64>, InboxStoreError> {
        validate_write_timestamp(now_ms, "now_ms")?;
        let cutoff = now_ms.saturating_sub(INBOX_RETENTION_MILLIS);
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let StoreScan {
            healthy,
            mut corrupt_rows,
        } = retention_candidates(&mut transaction).await?;
        let due: BTreeMap<_, _> = healthy
            .into_iter()
            .filter(|delivery| delivery.created_at <= cutoff)
            .map(|delivery| (delivery.message_id.to_string(), delivery.message_id))
            .collect();
        let mut removed = 0;
        if !due.is_empty() {
            // Only due, confirmed rows need a payload read before deletion.
            let candidates = select_inbox_rows!(
                "message_json",
                r#"WHERE created_at <= ? AND CASE WHEN json_valid(delivery_state_json)
                     THEN json_extract(delivery_state_json, '$.receipt.kind') = 'confirmed'
                     ELSE 0 END"#,
                cutoff,
            )
            .fetch_all(&mut *transaction)
            .await?;
            for row in candidates {
                let Some(id) = std::str::from_utf8(&row.message_id)
                    .ok()
                    .and_then(|key| due.get(key))
                else {
                    continue;
                };
                let delivery = match row.decode() {
                    Ok(delivery) => delivery,
                    Err(error) => {
                        self.corrupt_retention_payloads.insert(id.clone());
                        corrupt_rows.push(error);
                        continue;
                    }
                };
                self.corrupt_retention_payloads.remove(id);
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
) -> Result<StoreScan<Vec<DeliveryHeader>>, InboxStoreError> {
    // Inspect the state and expiry without loading historical payload bytes.
    let rows: StoreScan<Vec<_>> = select_inbox_rows!("NULL", "")
        .fetch_all(connection)
        .await?
        .into_iter()
        .map(|row| row.decode_header())
        .collect();
    Ok(rows.map_healthy(|rows| {
        rows.into_iter()
            .filter(|delivery| delivery.state.receipt == ReceiptConfirmation::Confirmed)
            .collect()
    }))
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
    Ok(
        select_inbox_rows!("message_json", r#"WHERE message_id = ?"#, key,)
            .fetch_optional(connection)
            .await?,
    )
}

async fn update_delivery(
    connection: &mut SqliteConnection,
    delivery: &StoredDelivery,
    now_ms: i64,
) -> Result<(), InboxStoreError> {
    let state = serde_json::to_string(&delivery.state)?;
    sqlx::query!(
        "UPDATE inbox_messages SET delivery_state_json = ?, updated_at = ? WHERE message_id = ?",
        state,
        now_ms,
        delivery.message_id.as_str(),
    )
    .execute(connection)
    .await?;
    Ok(())
}

fn validate_write_timestamp(value: i64, field: &'static str) -> Result<(), InboxStoreError> {
    Timestamp::from_timestamp_millis(value)
        .map(|_| ())
        .ok_or(InboxStoreError::InvalidTimestamp { field })
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
