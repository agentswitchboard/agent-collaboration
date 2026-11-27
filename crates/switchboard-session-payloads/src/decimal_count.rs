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
#[schemars(extend("pattern" = canonical_decimal_pattern(), "maxLength" = 20))]
pub struct DecimalCount(u64);

fn canonical_decimal_pattern() -> String {
    let maximum = u64::MAX.to_string();
    // All shorter canonical values fit; full-length alternatives either equal
    // the maximum or first differ at a digit that is smaller.
    let mut alternatives = vec![
        "0".to_owned(),
        format!("[1-9][0-9]{{0,{}}}", maximum.len() - 2),
    ];
    let mut prefix = String::new();
    for (index, ceiling) in maximum.bytes().enumerate() {
        let floor = if index == 0 { b'1' } else { b'0' };
        if ceiling > floor {
            let lower = char::from(floor);
            let upper = char::from(ceiling - 1);
            let digit = if lower == upper {
                lower.to_string()
            } else {
                format!("[{lower}-{upper}]")
            };
            let remaining = maximum.len() - index - 1;
            alternatives.push(format!("{prefix}{digit}[0-9]{{{remaining}}}"));
        }
        prefix.push(char::from(ceiling));
    }
    alternatives.push(maximum);
    format!(r"^(?:{})$(?![\s\S])", alternatives.join("|"))
}

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
