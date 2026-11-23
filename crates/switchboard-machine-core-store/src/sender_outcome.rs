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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutcomePrecedence {
    Keep,
    Replace,
    ContradictoryFinal,
}

impl StoredSenderOutcome {
    pub(crate) fn precedence_over(&self, previous: &Self) -> OutcomePrecedence {
        use OutcomePrecedence::{ContradictoryFinal, Keep, Replace};
        match (is_final(previous.outcome), is_final(self.outcome)) {
            (true, true) if self.outcome != previous.outcome => ContradictoryFinal,
            (true, _) => Keep,
            (false, true) => Replace,
            (false, false) => {
                if matches!(previous.outcome, MessageOutcome::DeliveryNotConfirmed)
                    && matches!(
                        self.outcome,
                        MessageOutcome::DeliveredSeenNotConfirmed { .. }
                    )
                {
                    Replace
                } else {
                    Keep
                }
            }
        }
    }
}

// A final receipt is distinct from the provisional deadline observation.
fn is_final(outcome: MessageOutcome) -> bool {
    match outcome {
        MessageOutcome::Refused { .. } => true,
        MessageOutcome::DeliveryNotConfirmed | MessageOutcome::DeliveredSeenNotConfirmed { .. } => {
            false
        }
    }
}
