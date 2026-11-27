//! Reported usage values and the scope they describe.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::item_identifiers::ItemOrder;
use crate::object_reader::{deserialize_object, wire_enum, wire_object};

wire_object! {
    pub struct UsageReading {
        pub metric: UsageMetric,
        pub scope: UsageScope,
        pub amount: UsageAmount,
    }
}

wire_enum! {
    pub enum UsageMetric {
        ContextUsed,
        InputTokens,
        OutputTokens,
        ReasoningTokens,
        CacheReadTokens,
        CacheWriteTokens,
        Cost,
        Other { source_name: String },
    }
}

wire_enum! {
    pub enum UsageScope {
        Turn,
        Session,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum UsageAmount {
    Count {
        #[serde(serialize_with = "serialize_count")]
        #[schemars(with = "String")]
        value: u64,
        #[serde(
            serialize_with = "serialize_capacity",
            skip_serializing_if = "Option::is_none"
        )]
        #[schemars(with = "Option<String>")]
        capacity: Option<u64>,
    },
    Money {
        /// The decimal text supplied by the reporter, with its original precision.
        amount: String,
        currency: String,
    },
}

impl<'de> Deserialize<'de> for UsageAmount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
        enum Wire {
            Count {
                value: ItemOrder,
                capacity: Option<ItemOrder>,
            },
            Money {
                amount: String,
                currency: String,
            },
        }

        Ok(match deserialize_object(deserializer)? {
            Wire::Count { value, capacity } => Self::Count {
                value: value.get(),
                capacity: capacity.map(ItemOrder::get),
            },
            Wire::Money { amount, currency } => Self::Money { amount, currency },
        })
    }
}

#[expect(
    clippy::trivially_copy_pass_by_ref,
    reason = "Serde's serialize_with passes the field by reference."
)]
fn serialize_count<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

fn serialize_capacity<S: Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    value.map(ItemOrder::from).serialize(serializer)
}
