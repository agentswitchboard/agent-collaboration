//! Checked names and exact positions belonging to session items.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::decimal_count::{DecimalCount, DecimalTextError};

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

/// An item's or group's fixed place, written as exact decimal text.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(transparent)]
pub struct ItemOrder(DecimalCount);

impl ItemOrder {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl FromStr for ItemOrder {
    type Err = DecimalTextError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl TryFrom<String> for ItemOrder {
    type Error = DecimalTextError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<u64> for ItemOrder {
    fn from(value: u64) -> Self {
        Self(value.into())
    }
}

impl From<ItemOrder> for String {
    fn from(value: ItemOrder) -> Self {
        value.0.into()
    }
}
