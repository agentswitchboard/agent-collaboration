//! Checked names and exact positions belonging to session items.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// The reason a session item, turn or group identifier was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemIdentifierError {
    Empty,
    TooLong,
    ControlCharacter,
}

impl fmt::Display for ItemIdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "item identifier text is empty",
            Self::TooLong => "item identifier text exceeds 256 bytes",
            Self::ControlCharacter => "item identifier text contains a control character",
        })
    }
}

impl std::error::Error for ItemIdentifierError {}

fn check_identifier(value: &str) -> Result<(), ItemIdentifierError> {
    if value.is_empty() {
        return Err(ItemIdentifierError::Empty);
    }
    if value.len() > 256 {
        return Err(ItemIdentifierError::TooLong);
    }
    if value.chars().any(char::is_control) {
        return Err(ItemIdentifierError::ControlCharacter);
    }
    Ok(())
}

macro_rules! item_identifier {
    ($(#[$attribute:meta])* $name:ident) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl FromStr for $name {
            type Err = ItemIdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                check_identifier(value)?;
                Ok(Self(value.to_owned()))
            }
        }

        impl TryFrom<String> for $name {
            type Error = ItemIdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                check_identifier(&value)?;
                Ok(Self(value))
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

item_identifier!(/// A stable name for an item within a session.
    ItemId);
item_identifier!(/// The name of a turn to which an item belongs.
    TurnId);
item_identifier!(/// The name of an assistant group within a session.
    GroupId);

/// The reason exact unsigned decimal text was refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ItemOrderError {
    Empty,
    NonDecimal,
    LeadingZero,
    OutOfRange,
}

impl fmt::Display for ItemOrderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "decimal text is empty",
            Self::NonDecimal => "decimal text must contain only ASCII digits",
            Self::LeadingZero => "decimal text has a leading zero",
            Self::OutOfRange => "decimal text exceeds the maximum u64 value",
        })
    }
}

impl std::error::Error for ItemOrderError {}

fn parse_decimal(value: &str) -> Result<u64, ItemOrderError> {
    if value.is_empty() {
        return Err(ItemOrderError::Empty);
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ItemOrderError::NonDecimal);
    }
    if value.len() > 1 && value.starts_with('0') {
        return Err(ItemOrderError::LeadingZero);
    }
    value.parse().map_err(|_| ItemOrderError::OutOfRange)
}

/// An item's or group's fixed place, written as exact decimal text.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(try_from = "String", into = "String")]
pub struct ItemOrder(u64);

impl ItemOrder {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl FromStr for ItemOrder {
    type Err = ItemOrderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        parse_decimal(value).map(Self)
    }
}

impl TryFrom<String> for ItemOrder {
    type Error = ItemOrderError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<u64> for ItemOrder {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<ItemOrder> for String {
    fn from(value: ItemOrder) -> Self {
        value.0.to_string()
    }
}
