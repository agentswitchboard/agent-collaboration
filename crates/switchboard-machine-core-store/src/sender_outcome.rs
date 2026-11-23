//! The body-free sender view of a delivery outcome reported by central.
use serde::{Deserialize, Serialize};
use workspace_ids::{MessageId, RequestId};
use workspace_model::{Timestamp, central_mail::MessageOutcome};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoredSenderOutcome {
    pub inbox_message_id: MessageId,
    pub message_id: MessageId,
    pub request_id: RequestId,
    pub outcome: MessageOutcome,
    /// Central's creation time of the outcome notice.
    pub reported_at: Timestamp,
}

impl StoredSenderOutcome {
    pub(crate) fn supersedes(&self, previous: &Self) -> bool {
        (!is_final(previous.outcome) || is_final(self.outcome))
            && (&self.reported_at, &self.inbox_message_id)
                > (&previous.reported_at, &previous.inbox_message_id)
    }
}

// Central's FinalReceipt creates Refused or NotSeen. Its deadline observation
// lives in PastTheMark and carries either unconfirmed outcome.
fn is_final(outcome: MessageOutcome) -> bool {
    match outcome {
        MessageOutcome::Refused { .. } | MessageOutcome::NotSeen { .. } => true,
        MessageOutcome::DeliveryNotConfirmed | MessageOutcome::DeliveredSeenNotConfirmed { .. } => {
            false
        }
    }
}
