//! Checked text that preserves a reported money amount and its currency.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// A reported nonnegative decimal amount with its original fractional precision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "String", into = "String")]
pub struct MoneyAmount(String);

impl MoneyAmount {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for MoneyAmount {
    type Err = MoneyAmountError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        check_amount(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for MoneyAmount {
    type Error = MoneyAmountError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        check_amount(&value)?;
        Ok(Self(value))
    }
}

impl From<MoneyAmount> for String {
    fn from(value: MoneyAmount) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoneyAmountError {
    Empty,
    TooLong,
    InvalidDecimal,
    LeadingZero,
}

impl fmt::Display for MoneyAmountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "money amount is empty",
            Self::TooLong => "money amount exceeds 40 bytes",
            Self::InvalidDecimal => {
                "money amount must be ASCII digits with an optional fractional part"
            }
            Self::LeadingZero => "money amount has a leading zero in the whole part",
        })
    }
}

impl std::error::Error for MoneyAmountError {}

fn check_amount(value: &str) -> Result<(), MoneyAmountError> {
    if value.is_empty() {
        return Err(MoneyAmountError::Empty);
    }
    if value.len() > 40 {
        return Err(MoneyAmountError::TooLong);
    }

    let whole = match value.split_once('.') {
        Some((whole, fraction)) if has_only_ascii_digits(fraction) => whole,
        Some(_) => return Err(MoneyAmountError::InvalidDecimal),
        None => value,
    };
    if !has_only_ascii_digits(whole) {
        return Err(MoneyAmountError::InvalidDecimal);
    }
    if whole.len() > 1 && whole.starts_with('0') {
        return Err(MoneyAmountError::LeadingZero);
    }
    Ok(())
}

fn has_only_ascii_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

/// The currency name supplied with a money amount.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "String", into = "String")]
pub struct MoneyCurrency(String);

impl MoneyCurrency {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for MoneyCurrency {
    type Err = MoneyCurrencyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        check_currency(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for MoneyCurrency {
    type Error = MoneyCurrencyError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        check_currency(&value)?;
        Ok(Self(value))
    }
}

impl From<MoneyCurrency> for String {
    fn from(value: MoneyCurrency) -> Self {
        value.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoneyCurrencyError {
    Empty,
    TooLong,
    ControlCharacter,
}

impl fmt::Display for MoneyCurrencyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "money currency is empty",
            Self::TooLong => "money currency exceeds 16 bytes",
            Self::ControlCharacter => "money currency contains a control character",
        })
    }
}

impl std::error::Error for MoneyCurrencyError {}

fn check_currency(value: &str) -> Result<(), MoneyCurrencyError> {
    if value.is_empty() {
        return Err(MoneyCurrencyError::Empty);
    }
    if value.len() > 16 {
        return Err(MoneyCurrencyError::TooLong);
    }
    if value.chars().any(char::is_control) {
        return Err(MoneyCurrencyError::ControlCharacter);
    }
    Ok(())
}
