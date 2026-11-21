//! Direct-message offer timing and machine-authored receipt batches.
use crate::ProtocolError;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use workspace_ids::MessageId;
use workspace_model::{Timestamp, central_mail::MessageBody};

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
        body: &MessageBody,
        time_left: Option<Self>,
    ) -> Result<(), ProtocolError> {
        match (body, time_left) {
            (MessageBody::Direct { .. }, Some(_))
            | (MessageBody::Notice { .. } | MessageBody::DeliveryOutcome { .. }, None) => Ok(()),
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
pub struct Receipt {
    pub message_id: MessageId,
    pub kind: ReceiptKind,
}

/// Ordered, with duplicates allowed for idempotent settlement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "Vec<Receipt>", into = "Vec<Receipt>")]
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
