//! Direct-message offer timing and machine-authored receipt batches.
use crate::ProtocolError;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    PostText, Timestamp,
    central_mail::{
        MessageBody, MessageDelivery, MessageRecord, MessageSender, MessageState, NoticeSource,
    },
};

pub use workspace_model::central_mail::{ReceiptKind, ReceiptResult};

pub const MAX_RECEIPTS: usize = 64;
pub const MAX_TIME_LEFT_MS: u32 = 900_000;

/// A direct offer carries this many whole milliseconds at the sending boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "u32", into = "u32")]
pub struct TimeLeft(NonZeroU32);

impl TimeLeft {
    pub fn at_handoff(accepted: Timestamp, cutoff: Timestamp, now: Timestamp) -> Option<Self> {
        if cutoff <= accepted {
            return None;
        }
        let millis = (cutoff - now)
            .num_milliseconds()
            .min(i64::from(MAX_TIME_LEFT_MS));
        Self::try_from(u32::try_from(millis).ok()?).ok()
    }

    pub(crate) fn validate_offer(
        message: &DeliveryMessage,
        time_left: Option<Self>,
    ) -> Result<(), ProtocolError> {
        match (message, time_left) {
            (DeliveryMessage::Direct(_), Some(_)) => Ok(()),
            (DeliveryMessage::System(record), None)
                if matches!(
                    record.body,
                    MessageBody::Notice { .. } | MessageBody::DeliveryOutcome { .. }
                ) =>
            {
                Ok(())
            }
            _ => Err(ProtocolError::InvalidOffer),
        }
    }
}

impl TryFrom<u32> for TimeLeft {
    type Error = ProtocolError;

    fn try_from(millis: u32) -> Result<Self, Self::Error> {
        NonZeroU32::new(millis)
            .filter(|duration| duration.get() <= MAX_TIME_LEFT_MS)
            .map(Self)
            .ok_or(ProtocolError::InvalidOffer)
    }
}

impl From<TimeLeft> for u32 {
    fn from(value: TimeLeft) -> Self {
        value.0.get()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct Receipt {
    pub message_id: MessageId,
    pub kind: ReceiptKind,
}

/// Ordered, with duplicates allowed for idempotent settlement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "Vec<Receipt>", into = "Vec<Receipt>")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ReceiptBatch(Vec<Receipt>);

impl ReceiptBatch {
    pub fn new(receipts: Vec<Receipt>) -> Result<Self, ProtocolError> {
        if receipts.is_empty() || receipts.len() > MAX_RECEIPTS {
            return Err(ProtocolError::InvalidReceiptBatch);
        }
        Ok(Self(receipts))
    }

    #[must_use]
    pub fn receipts(&self) -> &[Receipt] {
        &self.0
    }
}

impl TryFrom<Vec<Receipt>> for ReceiptBatch {
    type Error = ProtocolError;

    fn try_from(receipts: Vec<Receipt>) -> Result<Self, Self::Error> {
        Self::new(receipts)
    }
}

impl From<ReceiptBatch> for Vec<Receipt> {
    fn from(batch: ReceiptBatch) -> Self {
        batch.0
    }
}

/// A direct offer carries delivery data only; the handoff's duration carries the time limit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectOfferMessage {
    pub message_id: MessageId,
    pub sender: MessageSender,
    pub target: SessionRef,
    pub body: DirectOfferBody,
    pub state: MessageState,
    pub source: Option<NoticeSource>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DirectOfferBody {
    Direct {
        body: PostText,
        delivery: MessageDelivery,
        #[serde(deserialize_with = "Option::deserialize")]
        reply_to: Option<MessageId>,
    },
}

/// Keep the established system message shape while giving direct messages their own wire record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum DeliveryMessage {
    Direct(DirectOfferMessage),
    System(MessageRecord),
}

impl DeliveryMessage {
    #[must_use]
    pub fn message_id(&self) -> &MessageId {
        match self {
            Self::Direct(message) => &message.message_id,
            Self::System(message) => &message.message_id,
        }
    }

    #[must_use]
    pub fn target(&self) -> &SessionRef {
        match self {
            Self::Direct(message) => &message.target,
            Self::System(message) => &message.target,
        }
    }
}

impl From<MessageRecord> for DeliveryMessage {
    fn from(message: MessageRecord) -> Self {
        match message.body {
            MessageBody::Direct {
                body,
                delivery,
                reply_to,
            } => Self::Direct(DirectOfferMessage {
                message_id: message.message_id,
                sender: message.sender,
                target: message.target,
                body: DirectOfferBody::Direct {
                    body,
                    delivery,
                    reply_to,
                },
                state: message.state,
                source: message.source,
            }),
            MessageBody::Notice { .. }
            | MessageBody::DeliveryOutcome { .. }
            | MessageBody::Wake { .. }
            | MessageBody::Automation { .. } => Self::System(message),
        }
    }
}

#[cfg(all(test, feature = "wire-export", not(target_arch = "wasm32")))]
#[path = "message_settlement_typescript_test.rs"]
mod message_settlement_typescript_test;
