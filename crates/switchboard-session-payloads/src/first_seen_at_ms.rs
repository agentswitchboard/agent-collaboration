//! Checked time for the Router's first observation of a session item.

use std::fmt;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch when the Router first saw an item.
///
/// This u64 is a JSON integer because its maximum is the largest safe JavaScript integer.
/// The producer sets it once and carries the value forward when updating the item.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct FirstSeenAtMs(u64);

impl FirstSeenAtMs {
    pub const MAX: u64 = 9_007_199_254_740_991;

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for FirstSeenAtMs {
    type Error = FirstSeenAtMsError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value <= Self::MAX {
            Ok(Self(value))
        } else {
            Err(FirstSeenAtMsError)
        }
    }
}

impl From<FirstSeenAtMs> for u64 {
    fn from(value: FirstSeenAtMs) -> Self {
        value.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FirstSeenAtMsError;

impl fmt::Display for FirstSeenAtMsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "first-seen time exceeds the maximum of {} milliseconds",
            FirstSeenAtMs::MAX
        )
    }
}

impl std::error::Error for FirstSeenAtMsError {}

impl JsonSchema for FirstSeenAtMs {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "FirstSeenAtMs".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": "integer", "minimum": 0, "maximum": Self::MAX})
    }
}
