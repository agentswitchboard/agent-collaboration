//! Exact decimal text for a Router-issued epoch or position.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Why supplied text could not be used as a Router-issued position number.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DecimalPositionError {
    #[error("decimal text is empty")]
    Empty,
    #[error("decimal text must contain only ASCII digits")]
    NonDecimal,
    #[error("decimal text has a leading zero")]
    LeadingZero,
    #[error("decimal text exceeds the maximum u64 value")]
    OutOfRange,
}

fn parse_decimal(value: &str) -> Result<u64, DecimalPositionError> {
    if value.is_empty() {
        return Err(DecimalPositionError::Empty);
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DecimalPositionError::NonDecimal);
    }
    if value.len() > 1 && value.starts_with('0') {
        return Err(DecimalPositionError::LeadingZero);
    }
    value.parse().map_err(|_| DecimalPositionError::OutOfRange)
}

macro_rules! decimal_position_number {
    ($(#[$attribute:meta])* $name:ident) => {
        $(#[$attribute])*
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(u64);

        impl FromStr for $name {
            type Err = DecimalPositionError;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                parse_decimal(value).map(Self)
            }
        }

        impl TryFrom<String> for $name {
            type Error = DecimalPositionError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0.to_string()
            }
        }
    };
}

decimal_position_number!(
    /// The issuing Router's epoch for the history to which a watch position belongs.
    HistoryEpoch
);
decimal_position_number!(
    /// The issuing Router's exact position within the history of a session.
    IssuedPosition
);
