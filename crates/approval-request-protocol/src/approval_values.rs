use crate::approval_wire_codec::approval_wire_record;
use serde::{Deserialize, Serialize};

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
const MAX_OPTION_ORDINAL: u8 = 16;
const MAX_FIELD_ORDINAL: u8 = 32;
pub const MAX_OFFERED_OPTIONS: usize = MAX_OPTION_ORDINAL as usize;
pub const MAX_QUESTION_FIELDS: usize = MAX_FIELD_ORDINAL as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("approval ordinal {value} must be between 1 and {maximum}")]
pub struct InvalidApprovalOrdinal {
    value: u8,
    maximum: u8,
}

macro_rules! approval_ordinal {
    ($name:ident, $maximum:expr) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(try_from = "u8", into = "u8")]
        #[cfg_attr(
            all(feature = "wire-export", not(target_arch = "wasm32")),
            derive(specta::Type)
        )]
        pub struct $name(u8);

        impl $name {
            pub fn new(value: u8) -> Result<Self, InvalidApprovalOrdinal> {
                Self::try_from(value)
            }

            #[must_use]
            pub const fn get(&self) -> u8 {
                self.0
            }
        }

        impl TryFrom<u8> for $name {
            type Error = InvalidApprovalOrdinal;

            fn try_from(value: u8) -> Result<Self, Self::Error> {
                if (1..=$maximum).contains(&value) {
                    Ok(Self(value))
                } else {
                    Err(InvalidApprovalOrdinal {
                        value,
                        maximum: $maximum,
                    })
                }
            }
        }

        impl From<$name> for u8 {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

approval_ordinal!(OfferedOptionId, MAX_OPTION_ORDINAL);
approval_ordinal!(QuestionFieldId, MAX_FIELD_ORDINAL);
approval_text!(CallOperationName, Some(TITLE_MAX_CHARACTERS), true);

pub type CentralTime = chrono::DateTime<chrono::Utc>;

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    struct ApprovalWindowFields {
        raised_at: CentralTime,
        deadline: CentralTime,
    }
}

/// The immutable first-save time and exact one-hour deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ApprovalWindowFields", into = "ApprovalWindowFields")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalWindow(ApprovalWindowFields);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidApprovalWindow {
    #[error("the one-hour approval deadline is out of range")]
    DeadlineOutOfRange,
    #[error("the approval deadline must be exactly one hour after first save")]
    DeadlineMismatch,
}

impl ApprovalWindow {
    pub fn from_first_save(raised_at: CentralTime) -> Result<Self, InvalidApprovalWindow> {
        let deadline = raised_at
            .checked_add_signed(chrono::Duration::hours(1))
            .ok_or(InvalidApprovalWindow::DeadlineOutOfRange)?;
        Ok(Self(ApprovalWindowFields {
            raised_at,
            deadline,
        }))
    }

    pub fn new(
        raised_at: CentralTime,
        deadline: CentralTime,
    ) -> Result<Self, InvalidApprovalWindow> {
        let window = Self::from_first_save(raised_at)?;
        if window.deadline() == deadline {
            Ok(window)
        } else {
            Err(InvalidApprovalWindow::DeadlineMismatch)
        }
    }

    #[must_use]
    pub const fn raised_at(self) -> CentralTime {
        self.0.raised_at
    }

    #[must_use]
    pub const fn deadline(self) -> CentralTime {
        self.0.deadline
    }
}

impl TryFrom<ApprovalWindowFields> for ApprovalWindow {
    type Error = InvalidApprovalWindow;

    fn try_from(fields: ApprovalWindowFields) -> Result<Self, Self::Error> {
        Self::new(fields.raised_at, fields.deadline)
    }
}

impl From<ApprovalWindow> for ApprovalWindowFields {
    fn from(window: ApprovalWindow) -> Self {
        window.0
    }
}

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

/// An exact ASCII decimal spelling. The wire transports the string unchanged.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct QuestionNumber(String);

pub const MAX_QUESTION_NUMBER_LENGTH: usize = 40;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidQuestionNumber {
    #[error("a question number can contain at most {MAX_QUESTION_NUMBER_LENGTH} characters")]
    TooLong,
    #[error("a question number must use the approved decimal spelling")]
    InvalidSpelling,
}

impl QuestionNumber {
    pub fn new(spelling: impl Into<String>) -> Result<Self, InvalidQuestionNumber> {
        Self::try_from(spelling.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for QuestionNumber {
    type Error = InvalidQuestionNumber;

    fn try_from(spelling: String) -> Result<Self, Self::Error> {
        if spelling.len() > MAX_QUESTION_NUMBER_LENGTH {
            return Err(InvalidQuestionNumber::TooLong);
        }
        let digits = spelling.strip_prefix('-').unwrap_or(&spelling);
        let (whole, fraction) = match digits.split_once('.') {
            Some((whole, fraction)) => (whole, Some(fraction)),
            None => (digits, None),
        };
        if spelling == "-0"
            || !is_decimal_digits(whole)
            || (whole.starts_with('0') && whole.len() > 1)
            || fraction.is_some_and(|fraction| !is_decimal_digits(fraction))
        {
            return Err(InvalidQuestionNumber::InvalidSpelling);
        }
        Ok(Self(spelling))
    }
}

impl From<QuestionNumber> for String {
    fn from(value: QuestionNumber) -> Self {
        value.0
    }
}

fn is_decimal_digits(part: &str) -> bool {
    !part.is_empty() && part.bytes().all(|digit| digit.is_ascii_digit())
}
