//! Durable direct handover state in the existing inbox row.

use serde::{Deserialize, Serialize};
use sqlite_storage_maintenance::TransactionStart;
use std::collections::BTreeSet;
use uuid::{Uuid, Variant};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageDelivery},
};

use super::InboxRow;
use super::{
    HandoffState, InboxOfferFamily, InboxStore, InboxStoreError, StoredDelivery, read_delivery,
    update_delivery,
};
use crate::StoreScan;

/// A single notice attempt can cover several messages for one recipient.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct NoticeAttemptId(Uuid);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("notice attempt id must be a canonical UUIDv7")]
pub struct InvalidNoticeAttemptId;

impl TryFrom<Uuid> for NoticeAttemptId {
    type Error = InvalidNoticeAttemptId;

    fn try_from(uuid: Uuid) -> Result<Self, Self::Error> {
        if uuid.get_version_num() != 7 || uuid.get_variant() != Variant::RFC4122 {
            return Err(InvalidNoticeAttemptId);
        }
        Ok(Self(uuid))
    }
}

impl TryFrom<String> for NoticeAttemptId {
    type Error = InvalidNoticeAttemptId;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let uuid = Uuid::parse_str(&value).map_err(|_| InvalidNoticeAttemptId)?;
        if uuid.hyphenated().to_string() != value {
            return Err(InvalidNoticeAttemptId);
        }
        Self::try_from(uuid)
    }
}

impl From<NoticeAttemptId> for String {
    fn from(attempt: NoticeAttemptId) -> Self {
        attempt.0.hyphenated().to_string()
    }
}

impl NoticeAttemptId {
    pub fn mint(now: Timestamp) -> Self {
        let millis = u64::try_from(now.timestamp_millis()).unwrap_or_default();
        let timestamp = uuid::Timestamp::from_unix(
            uuid::NoContext,
            millis / 1_000,
            u32::try_from(millis % 1_000).unwrap_or_default() * 1_000_000,
        );
        Self(Uuid::new_v7(timestamp))
    }

    #[must_use]
    pub const fn uuid(self) -> Uuid {
        self.0
    }
}

/// Handover is independent of the original keep or refusal receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DirectHandoff {
    Waiting,
    Writing {
        attempt: NoticeAttemptId,
    },
    NotKnown,
    NotSeen,
    NoticeRefused {
        reason: NoticeRefusalReason,
    },
    Seen {
        completed_at: Timestamp,
        receipt: SeenReceiptState,
    },
}

/// A definitive local refusal settles the notice without a final Central receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum NoticeRefusalReason {
    CannotSteer,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum SeenReceiptState {
    Owed,
    Recorded,
    Rejected { reason: SeenReceiptRejection },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum SeenReceiptRejection {
    AlreadySettled,
    NoSuchMessage,
    NotApplicable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectHandoffRow {
    pub message_id: MessageId,
    pub target: SessionRef,
    pub expires_at: Timestamp,
    pub delivery: MessageDelivery,
    pub delivery_receipt_confirmed: bool,
    pub handoff: DirectHandoff,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectHandoffChange {
    pub message_id: MessageId,
    pub expected: DirectHandoff,
    pub next: DirectHandoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffCommit {
    Committed,
    ChangedSinceRead,
}

impl StoredDelivery {
    fn direct_handoff(&self) -> Option<DirectHandoff> {
        if self.state.family != InboxOfferFamily::Direct {
            return None;
        }
        match self.state.handoff {
            HandoffState::Pending => Some(DirectHandoff::Waiting),
            // Completion predates the named writer-take evidence. Keep it
            // readable and suppress another notice without inventing Seen.
            HandoffState::Completed => Some(DirectHandoff::NotKnown),
            HandoffState::NotRequired => None,
            HandoffState::Direct { state } => Some(state),
        }
    }

    fn into_direct_handoff(self) -> Option<DirectHandoffRow> {
        let handoff = self.direct_handoff()?;
        let message = self.message?;
        let MessageBody::Direct { delivery, .. } = message.body else {
            return None;
        };
        Some(DirectHandoffRow {
            message_id: self.message_id,
            target: message.target,
            expires_at: self.expires_at?,
            delivery,
            delivery_receipt_confirmed: self.state.receipt == super::ReceiptConfirmation::Confirmed,
            handoff,
        })
    }
}

impl InboxStore {
    /// Lists the newest direct messages retained for the recipient.
    pub async fn direct_inbox_messages(
        &mut self,
        target: &SessionRef,
        limit: usize,
    ) -> Result<StoreScan<Vec<workspace_model::central_mail::MessageRecord>>, InboxStoreError> {
        Ok(self.scan_direct_rows().await?.map_healthy(|rows| {
            rows.into_iter()
                .rev()
                .filter_map(|row| row.message)
                .filter(|message| &message.target == target)
                .take(limit)
                .collect()
        }))
    }

    /// Resolves an exact kept direct message for the recipient of an inbox acknowledgment.
    pub async fn direct_handoff_for_recipient(
        &mut self,
        target: &SessionRef,
        message_id: &MessageId,
    ) -> Result<Option<DirectHandoffRow>, InboxStoreError> {
        Ok(read_delivery(&mut self.store.connection, message_id)
            .await?
            .and_then(StoredDelivery::into_direct_handoff)
            .filter(|row| &row.target == target))
    }

    pub async fn has_seen_receipt_owed(&mut self) -> Result<bool, InboxStoreError> {
        let owed = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM inbox_messages
                WHERE CASE WHEN json_valid(delivery_state_json)
                    THEN json_extract(delivery_state_json, '$.handoff.state.receipt.kind') = 'owed'
                    ELSE 0 END)",
        )
        .fetch_one(&mut self.store.connection)
        .await?;
        Ok(owed)
    }

    pub async fn scan_direct_handoffs(
        &mut self,
    ) -> Result<StoreScan<Vec<DirectHandoffRow>>, InboxStoreError> {
        Ok(self.scan_direct_rows().await?.map_healthy(|rows| {
            rows.into_iter()
                .filter_map(StoredDelivery::into_direct_handoff)
                .collect()
        }))
    }

    /// Selects kept direct rows that can owe an automatic notice at the next mark.
    pub async fn scan_waiting_notice_handoffs(
        &mut self,
    ) -> Result<StoreScan<Vec<DirectHandoffRow>>, InboxStoreError> {
        let rows = select_inbox_rows!(
            "message_json",
            r#"WHERE CASE WHEN json_valid(delivery_state_json) THEN
                    json_extract(delivery_state_json, '$.family') IS NOT 'system' AND
                    CASE WHEN json_extract(delivery_state_json, '$.handoff.kind') = 'direct' THEN
                        COALESCE(json_extract(delivery_state_json, '$.handoff.state.kind')
                            NOT IN ('writing', 'notKnown', 'notSeen', 'noticeRefused', 'seen'), 1)
                    ELSE COALESCE(json_extract(delivery_state_json, '$.handoff.kind')
                            NOT IN ('completed', 'notRequired'), 1) END
                    ELSE 1 END
                 AND CASE WHEN json_valid(message_json) THEN
                    json_extract(message_json, '$.body.delivery') IS NOT 'queue' AND
                    json_extract(message_json, '$.body.delivery') IS NOT 'steer'
                    ELSE 1 END
                 ORDER BY created_at, message_id"#,
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        let scan: StoreScan<Vec<_>> = rows.into_iter().map(InboxRow::decode).collect();
        Ok(scan.map_healthy(|rows| {
            rows.into_iter()
                .filter_map(StoredDelivery::into_direct_handoff)
                .collect()
        }))
    }

    async fn scan_direct_rows(
        &mut self,
    ) -> Result<StoreScan<Vec<StoredDelivery>>, InboxStoreError> {
        let rows = select_inbox_rows!(
            "message_json",
            r#"WHERE CASE WHEN json_valid(delivery_state_json)
                    THEN json_extract(delivery_state_json, '$.family') IS NOT 'system'
                    ELSE 1 END
                 ORDER BY created_at, message_id"#,
        )
        .fetch_all(&mut self.store.connection)
        .await?;
        Ok(rows.into_iter().map(InboxRow::decode).collect())
    }

    /// Compare every row before committing all changes for one recipient.
    pub async fn compare_and_set_direct_handoffs(
        &mut self,
        target: &SessionRef,
        changes: &[DirectHandoffChange],
        now: Timestamp,
    ) -> Result<HandoffCommit, InboxStoreError> {
        let mut keys = BTreeSet::new();
        for change in changes {
            if !keys.insert(&change.message_id) {
                return Err(InboxStoreError::DuplicateHandoffChange {
                    message_id: change.message_id.clone(),
                });
            }
        }
        if changes.is_empty() {
            return Ok(HandoffCommit::Committed);
        }
        let mut transaction = self.store.transaction(TransactionStart::Immediate).await?;
        let mut deliveries = Vec::with_capacity(changes.len());
        for change in changes {
            let Some(mut delivery) = read_delivery(&mut transaction, &change.message_id).await?
            else {
                return Ok(HandoffCommit::ChangedSinceRead);
            };
            if delivery.direct_handoff() != Some(change.expected)
                || delivery
                    .message
                    .as_ref()
                    .is_none_or(|message| &message.target != target)
            {
                return Ok(HandoffCommit::ChangedSinceRead);
            }
            delivery.state.handoff = HandoffState::Direct { state: change.next };
            deliveries.push(delivery);
        }
        for delivery in deliveries {
            update_delivery(&mut transaction, &delivery, now.timestamp_millis()).await?;
        }
        transaction.commit().await?;
        Ok(HandoffCommit::Committed)
    }
}

#[cfg(test)]
#[path = "inbox_handoff_test.rs"]
mod tests;
