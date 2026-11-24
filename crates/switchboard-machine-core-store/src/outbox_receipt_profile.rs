//! Body-free outbox data retained after a request ends or central answers it.
use crate::{OutboxAccepted, OutboxRequestStatus, OutboxWriteTarget, StoredSenderOutcome};
use serde::{Deserialize, Serialize};
use workspace_ids::RequestId;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AnsweredWrite {
    pub(crate) request_id: RequestId,
    pub(crate) target: OutboxWriteTarget,
    /// A resubmit must still name the original target and content.
    pub(crate) content_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) delivery_outcome: Option<StoredSenderOutcome>,
}

impl AnsweredWrite {
    pub(crate) fn matches_delivery_receipt(&self, status: &OutboxRequestStatus) -> bool {
        self.delivery_outcome.as_ref().is_none_or(|notice| {
            self.request_id == notice.request_id
                && matches!(self.target, OutboxWriteTarget::Session { .. })
                && matches!(status, OutboxRequestStatus::Answered(OutboxAccepted::SendMessage { message_id }) if *message_id == notice.message_id)
        })
    }
}
