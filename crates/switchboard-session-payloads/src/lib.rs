//! Private payloads for session loads and the typed items a session carries.

mod decimal_count;
mod first_seen_at_ms;
mod item_identifiers;
mod money_reading;
mod object_reader;
mod session_item;
mod tool_call;
mod usage_reading;

pub use decimal_count::{DecimalCount, DecimalTextError};
pub use first_seen_at_ms::{FirstSeenAtMs, FirstSeenAtMsError};
pub use item_identifiers::{GroupId, ItemId, ItemIdentifierError, ItemOrder, TurnId};
pub use money_reading::{MoneyAmount, MoneyAmountError, MoneyCurrency, MoneyCurrencyError};
pub use session_item::{
    AssistantBody, GroupPlace, Item, ItemBody, PlanEntry, PlanEntryPriority, PlanEntryState,
    SessionBody, UserBody,
};
pub use tool_call::{
    BoundedText, FileChange, FileChangeKind, FileDiff, FileLocation, LineRange, LineRangeError,
    RawInput, SearchOver, ToolAction, ToolCall, ToolKind, ToolState,
};
pub use usage_reading::{UsageAmount, UsageMetric, UsageReading, UsageScope};

use object_reader::deserialize_object;
use serde::{Deserialize, Deserializer, Serialize};
use workspace_ids::{SessionOperationId, SessionRef};

/// The caller's request to start a session load under an operation ID it already holds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadRequest {
    pub target: SessionRef,
    pub operation_id: SessionOperationId,
}

/// The caller's read-only query for a load identified by its original target and operation ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadQuery {
    pub target: SessionRef,
    pub operation_id: SessionOperationId,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LoadFields {
    #[serde(deserialize_with = "deserialize_object")]
    target: SessionRef,
    operation_id: SessionOperationId,
}

impl<'de> Deserialize<'de> for LoadRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let LoadFields {
            target,
            operation_id,
        } = deserialize_object(deserializer)?;
        Ok(Self {
            target,
            operation_id,
        })
    }
}

impl<'de> Deserialize<'de> for LoadQuery {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let LoadFields {
            target,
            operation_id,
        } = deserialize_object(deserializer)?;
        Ok(Self {
            target,
            operation_id,
        })
    }
}
