//! Exact unsigned counts carried as decimal text on the session item wire.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// The reason an unsigned decimal position or count was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecimalTextError {
    Empty,
    NonDecimal,
    LeadingZero,
    OutOfRange,
}

impl fmt::Display for DecimalTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "decimal text is empty",
            Self::NonDecimal => "decimal text must contain only ASCII digits",
            Self::LeadingZero => "decimal text has a leading zero",
            Self::OutOfRange => "decimal text exceeds the maximum u64 value",
        })
    }
}

impl std::error::Error for DecimalTextError {}

/// A nonnegative integer represented on the wire by its exact decimal digits.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct DecimalCount(u64);

impl DecimalCount {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl FromStr for DecimalCount {
    type Err = DecimalTextError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(DecimalTextError::Empty);
        }
        if !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(DecimalTextError::NonDecimal);
        }
        if value.len() > 1 && value.starts_with('0') {
            return Err(DecimalTextError::LeadingZero);
        }
        value
            .parse()
            .map(Self)
            .map_err(|_| DecimalTextError::OutOfRange)
    }
}

impl TryFrom<String> for DecimalCount {
    type Error = DecimalTextError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<u64> for DecimalCount {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<DecimalCount> for String {
    fn from(value: DecimalCount) -> Self {
        value.0.to_string()
    }
}
