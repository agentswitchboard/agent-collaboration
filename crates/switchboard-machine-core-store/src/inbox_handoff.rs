//! Durable direct handover state in the existing inbox row.

use serde::{Deserialize, Serialize};
use sqlite_storage_maintenance::TransactionStart;
use std::collections::BTreeSet;
use uuid::{Uuid, Variant};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::Timestamp;

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
    Seen {
        completed_at: Timestamp,
        receipt: SeenReceiptState,
    },
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
        Some(DirectHandoffRow {
            message_id: self.message_id,
            target: self.message?.target,
            expires_at: self.expires_at?,
            handoff,
        })
    }
}

impl InboxStore {
    pub async fn scan_direct_handoffs(
        &mut self,
    ) -> Result<StoreScan<Vec<DirectHandoffRow>>, InboxStoreError> {
        let rows = select_inbox_rows!(
            "message_json",
            r#"WHERE CASE WHEN json_valid(delivery_state_json)
                    THEN json_extract(delivery_state_json, '$.family') IS NOT 'system'
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
                    .filter_map(StoredDelivery::into_direct_handoff)
                    .collect()
            }))
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
