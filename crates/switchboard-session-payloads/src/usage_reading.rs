//! Reported usage values and the scope they describe.

use crate::decimal_count::DecimalCount;
use crate::money_reading::{MoneyAmount, MoneyCurrency};
use crate::object_reader::{wire_enum, wire_object};

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

wire_enum! {
    pub enum UsageAmount {
        Count {
            value: DecimalCount,
            #[serde(skip_serializing_if = "Option::is_none")]
            capacity: Option<DecimalCount>,
        },
        Money {
            amount: MoneyAmount,
            currency: MoneyCurrency,
        },
    }
}
