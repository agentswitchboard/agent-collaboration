//! Non-wrapping frame numbering for one watch call.

use std::num::NonZeroU32;

use serde::{Deserialize, Serialize};

/// The positive frame number assigned by the Router within one watch call.
#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(try_from = "u64", into = "u32")]
pub struct FrameOrdinal(NonZeroU32);

impl FrameOrdinal {
    pub const FIRST: Self = Self(NonZeroU32::MIN);
    pub const MAX: u32 = u32::MAX;

    pub fn checked_next(self) -> Result<Self, FrameOrdinalExhausted> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(FrameOrdinalExhausted::AtMaximum)
    }
}

/// The checked frame-ordinal constructor returns this for a supplied number outside the call's range.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FrameOrdinalError {
    #[error("frame ordinal must be positive")]
    Zero,
    #[error("frame ordinal exceeds the maximum u32 value")]
    OutOfRange,
}

/// The Router's checked frame increment returns this when the call has reached its last ordinal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum FrameOrdinalExhausted {
    #[error("frame ordinal is at its maximum")]
    AtMaximum,
}

impl TryFrom<u64> for FrameOrdinal {
    type Error = FrameOrdinalError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let value = u32::try_from(value).map_err(|_| FrameOrdinalError::OutOfRange)?;
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(FrameOrdinalError::Zero)
    }
}

impl From<FrameOrdinal> for u32 {
    fn from(ordinal: FrameOrdinal) -> Self {
        ordinal.0.get()
    }
}
