//! Checked scalars used in the session lifecycle protocol.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, SecondsFormat, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use zeroize::Zeroizing;

const MAXIMUM_EXACT_JSON_INTEGER: u64 = 9_007_199_254_740_991;
pub(super) const SESSION_SECRET_BYTES: usize = 32;

/// The request attempt is also the generation of a secret returned for that attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[serde(transparent)]
#[schemars(extend("minimum" = 1, "maximum" = 9_007_199_254_740_991_u64))]
pub struct CreateSessionAttempt(
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = specta_typescript::Number))]
     u64,
);

impl CreateSessionAttempt {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn checked_next(self) -> Option<Self> {
        Self::try_from(self.0 + 1).ok()
    }
}

impl TryFrom<u64> for CreateSessionAttempt {
    type Error = InvalidCreateSessionAttempt;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if (1..=MAXIMUM_EXACT_JSON_INTEGER).contains(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidCreateSessionAttempt)
        }
    }
}

impl From<CreateSessionAttempt> for u64 {
    fn from(attempt: CreateSessionAttempt) -> Self {
        attempt.get()
    }
}

impl<'de> Deserialize<'de> for CreateSessionAttempt {
    fn deserialize<Decoder: Deserializer<'de>>(decoder: Decoder) -> Result<Self, Decoder::Error> {
        Self::try_from(u64::deserialize(decoder)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("create session attempt must be a positive exact JSON integer")]
pub struct InvalidCreateSessionAttempt;

/// A central timestamp in the same fixed-width UTC millisecond form used for stored times.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[serde(try_from = "String", into = "String")]
#[schemars(extend("pattern" = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$"))]
pub struct SessionTime(String);

impl SessionTime {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SessionTime {
    type Error = InvalidSessionTime;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parsed = value
            .parse::<DateTime<Utc>>()
            .map_err(|_| InvalidSessionTime)?;
        if value.len() == 24 && parsed.to_rfc3339_opts(SecondsFormat::Millis, true) == value {
            Ok(Self(value))
        } else {
            Err(InvalidSessionTime)
        }
    }
}

impl From<SessionTime> for String {
    fn from(time: SessionTime) -> Self {
        time.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("session time must be fixed-width UTC with millisecond precision")]
pub struct InvalidSessionTime;

/// The 32-byte bearer secret, kept in its canonical wire spelling.
#[derive(Clone, JsonSchema)]
#[schemars(with = "String", extend("pattern" = "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$"))]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    specta(transparent)
)]
pub struct SessionSecret(
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    Zeroizing<String>,
);

impl SessionSecret {
    #[must_use]
    pub fn from_bytes(bytes: &[u8; SESSION_SECRET_BYTES]) -> Self {
        Self(Zeroizing::new(URL_SAFE_NO_PAD.encode(bytes)))
    }

    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SessionSecret {
    type Error = InvalidSessionSecret;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let value = Zeroizing::new(value);
        if is_base64url_32(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidSessionSecret)
        }
    }
}

impl fmt::Debug for SessionSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionSecret([REDACTED])")
    }
}

impl PartialEq for SessionSecret {
    fn eq(&self, other: &Self) -> bool {
        self.expose_secret() == other.expose_secret()
    }
}

impl Eq for SessionSecret {}

impl Serialize for SessionSecret {
    fn serialize<Encoder: Serializer>(
        &self,
        encoder: Encoder,
    ) -> Result<Encoder::Ok, Encoder::Error> {
        encoder.serialize_str(self.expose_secret())
    }
}

impl<'de> Deserialize<'de> for SessionSecret {
    fn deserialize<Decoder: Deserializer<'de>>(decoder: Decoder) -> Result<Self, Decoder::Error> {
        Self::try_from(String::deserialize(decoder)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("session secret must be 32 bytes encoded as unpadded base64url")]
pub struct InvalidSessionSecret;

fn is_base64url_32(text: &str) -> bool {
    let mut bytes = Zeroizing::new([0_u8; SESSION_SECRET_BYTES]);
    text.len() == 43 && URL_SAFE_NO_PAD.decode_slice(text, &mut *bytes) == Ok(SESSION_SECRET_BYTES)
}

/// SHA-256 of the canonical create contents, in unpadded base64url.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[serde(try_from = "String", into = "String")]
#[schemars(extend("pattern" = "^[A-Za-z0-9_-]{42}[AEIMQUYcgkosw048]$"))]
pub struct CreateSessionDigest(String);

impl CreateSessionDigest {
    pub(super) fn from_sha256(digest: &[u8; SESSION_SECRET_BYTES]) -> Self {
        Self(URL_SAFE_NO_PAD.encode(digest))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for CreateSessionDigest {
    type Error = InvalidCreateSessionDigest;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if is_base64url_32(&value) {
            Ok(Self(value))
        } else {
            Err(InvalidCreateSessionDigest)
        }
    }
}

impl From<CreateSessionDigest> for String {
    fn from(digest: CreateSessionDigest) -> Self {
        digest.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("create session digest must be 32 bytes encoded as unpadded base64url")]
pub struct InvalidCreateSessionDigest;

#[cfg(test)]
#[path = "session_wire_values_test.rs"]
mod tests;
