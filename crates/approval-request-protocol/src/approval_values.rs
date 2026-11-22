use serde::{Deserialize, Serialize};
use serde_json::Number;

const MAX_SAFE_JSON_INTEGER: u64 = (1 << 53) - 1;

pub const TITLE_MAX_CHARACTERS: usize = 200;
pub const DESCRIPTION_MAX_CHARACTERS: usize = 2_000;
pub const SUBJECT_MAX_CHARACTERS: usize = 500;
pub const LABEL_MAX_CHARACTERS: usize = 200;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidApprovalText {
    #[error("{field} cannot be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds {maximum} characters")]
    TooLong { field: &'static str, maximum: usize },
}

fn validate_text(
    value: &str,
    field: &'static str,
    maximum: Option<usize>,
    nonempty: bool,
) -> Result<(), InvalidApprovalText> {
    if nonempty && value.is_empty() {
        return Err(InvalidApprovalText::Empty { field });
    }
    if let Some(maximum) = maximum
        && value.chars().take(maximum + 1).count() > maximum
    {
        return Err(InvalidApprovalText::TooLong { field, maximum });
    }
    Ok(())
}

macro_rules! approval_text {
    ($name:ident, $maximum:expr, $nonempty:expr) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        #[cfg_attr(
            all(feature = "wire-export", not(target_arch = "wasm32")),
            derive(specta::Type)
        )]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidApprovalText> {
                Self::try_from(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidApprovalText;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                validate_text(&value, stringify!($name), $maximum, $nonempty)?;
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

approval_text!(Title, Some(TITLE_MAX_CHARACTERS), false);
approval_text!(Description, Some(DESCRIPTION_MAX_CHARACTERS), false);
approval_text!(Subject, Some(SUBJECT_MAX_CHARACTERS), false);
approval_text!(Label, Some(LABEL_MAX_CHARACTERS), false);
// Provider IDs remain opaque and compare exactly, including whitespace.
approval_text!(OfferedOptionId, None, true);
approval_text!(QuestionFieldId, None, true);
approval_text!(CallOperationName, Some(TITLE_MAX_CHARACTERS), true);

/// SHA-256 of the parked invocation, in canonical lowercase hexadecimal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct InvocationDigest(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invocation digest must be 64 lowercase hexadecimal characters")]
pub struct InvalidInvocationDigest;

impl TryFrom<String> for InvocationDigest {
    type Error = InvalidInvocationDigest;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            Ok(Self(value))
        } else {
            Err(InvalidInvocationDigest)
        }
    }
}

impl From<InvocationDigest> for String {
    fn from(value: InvocationDigest) -> Self {
        value.0
    }
}

impl InvocationDigest {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub type CentralTime = chrono::DateTime<chrono::Utc>;

/// Exact on both sides of the JSON transport, including TypeScript.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct WaitingRevision(u64);

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
impl specta::Type for WaitingRevision {
    fn definition(types: &mut specta::Types) -> specta::datatype::DataType {
        specta_typescript::Number::<()>::definition(types)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("waiting revision exceeds the JavaScript safe integer maximum")]
pub struct InvalidWaitingRevision;

impl TryFrom<u64> for WaitingRevision {
    type Error = InvalidWaitingRevision;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value <= MAX_SAFE_JSON_INTEGER {
            Ok(Self(value))
        } else {
            Err(InvalidWaitingRevision)
        }
    }
}

impl From<WaitingRevision> for u64 {
    fn from(value: WaitingRevision) -> Self {
        value.0
    }
}

/// A finite JSON number. Integers must lie within ±(2^53 − 1), the range
/// Rust and JavaScript can round-trip exactly without changing their value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Number", into = "Number")]
pub struct QuestionNumber(Number);

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
impl specta::Type for QuestionNumber {
    fn definition(types: &mut specta::Types) -> specta::datatype::DataType {
        specta_typescript::Number::<()>::definition(types)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidQuestionNumber {
    #[error("question number must be finite")]
    NonFinite,
    #[error("question integer is outside the JavaScript safe integer range")]
    UnsafeInteger,
}

impl TryFrom<Number> for QuestionNumber {
    type Error = InvalidQuestionNumber;

    fn try_from(value: Number) -> Result<Self, Self::Error> {
        // Outside the safe integer range a finite binary64 number has no fraction.
        // Conversion here is only for validation. The original JSON number is retained.
        let binary_number = value
            .as_f64()
            .filter(|number| number.is_finite())
            .ok_or(InvalidQuestionNumber::NonFinite)?;
        if binary_number.abs() > MAX_SAFE_JSON_INTEGER as f64 {
            return Err(InvalidQuestionNumber::UnsafeInteger);
        }
        Ok(Self(value))
    }
}

impl TryFrom<f64> for QuestionNumber {
    type Error = InvalidQuestionNumber;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Number::from_f64(value)
            .ok_or(InvalidQuestionNumber::NonFinite)?
            .try_into()
    }
}

impl From<QuestionNumber> for Number {
    fn from(value: QuestionNumber) -> Self {
        value.0
    }
}

impl QuestionNumber {
    #[must_use]
    pub const fn as_json_number(&self) -> &Number {
        &self.0
    }
}
