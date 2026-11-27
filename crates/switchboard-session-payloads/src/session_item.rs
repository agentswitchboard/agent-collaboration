//! The identity, stable place and typed body of a session item.

use crate::first_seen_at_ms::FirstSeenAtMs;
use crate::item_identifiers::{GroupId, ItemId, ItemOrder, TurnId};
use crate::object_reader::{wire_enum, wire_object, wire_string_enum};
use crate::tool_call::ToolCall;
use crate::usage_reading::UsageReading;

wire_object! {
    /// The latest body of an item at its fixed place in session history.
    pub struct Item {
        pub id: ItemId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub turn: Option<TurnId>,
        pub order: ItemOrder,
        /// Set when the Router first sees this item and carried forward on updates.
        pub first_seen_at_ms: FirstSeenAtMs,
        pub body: ItemBody,
    }
}

wire_enum! {
    /// The role an item is drawn under determines the bodies it can carry.
    pub enum ItemBody {
        User { body: UserBody },
        Assistant { group: GroupPlace, body: AssistantBody },
        Session { body: SessionBody },
    }
}

wire_object! {
    /// The assistant group and its fixed place in session history.
    pub struct GroupPlace {
        pub id: GroupId,
        pub order: ItemOrder,
    }
}

wire_enum! {
    pub enum UserBody {
        Message { text: String },
    }
}

wire_enum! {
    pub enum AssistantBody {
        Message { text: String },
        Thinking { text: String },
        ToolCall { call: ToolCall },
        Plan { entries: Vec<PlanEntry> },
        Other {
            source_kind: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            text: Option<String>,
        },
    }
}

wire_enum! {
    pub enum SessionBody {
        Notice { text: String },
        SettingChange { text: String },
        Usage { reading: UsageReading },
        Other {
            source_kind: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            text: Option<String>,
        },
    }
}

wire_object! {
    pub struct PlanEntry {
        pub text: String,
        pub state: PlanEntryState,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub priority: Option<PlanEntryPriority>,
    }
}

wire_string_enum! {
    pub enum PlanEntryState {
        Pending,
        InProgress,
        Completed,
    }
}

wire_string_enum! {
    pub enum PlanEntryPriority {
        High,
        Medium,
        Low,
    }
}
