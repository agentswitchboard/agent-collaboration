//! Bounded values shared by signed stamps, issuer keys and digest replies.
use fleet_identity_protocol::UnixSeconds;
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct OrganizationStatementDigest(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("organization statement digest must be 64 lowercase hexadecimal digits")]
pub struct StatementDigestShapeError;

impl TryFrom<String> for OrganizationStatementDigest {
    type Error = StatementDigestShapeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(StatementDigestShapeError);
        }
        Ok(Self(value))
    }
}

impl From<OrganizationStatementDigest> for String {
    fn from(value: OrganizationStatementDigest) -> Self {
        value.0
    }
}

impl From<[u8; 32]> for OrganizationStatementDigest {
    fn from(bytes: [u8; 32]) -> Self {
        let digit = |nibble| {
            char::from(if nibble < 10 {
                b'0' + nibble
            } else {
                b'a' + nibble - 10
            })
        };
        Self(
            bytes
                .into_iter()
                .flat_map(|byte| [digit(byte >> 4), digit(byte & 0xf)])
                .collect(),
        )
    }
}

impl OrganizationStatementDigest {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl JsonSchema for OrganizationStatementDigest {
    fn schema_name() -> Cow<'static, str> {
        "OrganizationStatementDigest".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": "string", "pattern": "^[0-9a-f]{64}$"})
    }
}

/// The provider's opaque, nonempty signing key identifier.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct IssuerKeyId(String);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("issuer signing key ID must not be empty")]
pub struct IssuerKeyIdShapeError;

impl TryFrom<String> for IssuerKeyId {
    type Error = IssuerKeyIdShapeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(IssuerKeyIdShapeError);
        }
        Ok(Self(value))
    }
}

impl From<IssuerKeyId> for String {
    fn from(value: IssuerKeyId) -> Self {
        value.0
    }
}

impl IssuerKeyId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl JsonSchema for IssuerKeyId {
    fn schema_name() -> Cow<'static, str> {
        "IssuerKeyId".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": "string", "minLength": 1})
    }
}

/// A signed stamp time in nonnegative, exact JSON integer seconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "UnixSeconds", into = "UnixSeconds")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct StatementSeconds(UnixSeconds);

const MAXIMUM_EXACT_SECONDS: i64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("stamp time must be a nonnegative exact JSON integer")]
pub struct StatementSecondsShapeError;

impl TryFrom<UnixSeconds> for StatementSeconds {
    type Error = StatementSecondsShapeError;
    fn try_from(value: UnixSeconds) -> Result<Self, Self::Error> {
        if !(0..=MAXIMUM_EXACT_SECONDS).contains(&value.0) {
            return Err(StatementSecondsShapeError);
        }
        Ok(Self(value))
    }
}

impl From<StatementSeconds> for UnixSeconds {
    fn from(value: StatementSeconds) -> Self {
        value.0
    }
}

impl StatementSeconds {
    #[must_use]
    pub const fn unix_seconds(self) -> UnixSeconds {
        self.0
    }
}

impl JsonSchema for StatementSeconds {
    fn schema_name() -> Cow<'static, str> {
        "StatementSeconds".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"type": "integer", "format": "int64", "minimum": 0, "maximum": MAXIMUM_EXACT_SECONDS})
    }
}
