//! Validated identifiers and launch values shared across runtime boundaries.
use message_board::{IdentityValidationError, RecordId, ServiceId};
use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

macro_rules! identity_value {
    ($(#[$attribute:meta])* $name:ident, $inner:ty) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        $(#[$attribute])*
        pub struct $name($inner);
        impl JsonSchema for $name {
            fn schema_name() -> Cow<'static, str> {
                stringify!($name).into()
            }
            fn json_schema(generator: &mut SchemaGenerator) -> Schema {
                <$inner as JsonSchema>::json_schema(generator)
            }
        }
        impl TryFrom<String> for $name {
            type Error = IdentityValidationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                <$inner>::try_from(value).map(Self)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0.into()
            }
        }
        impl $name {
            #[must_use]
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }
    };
}
identity_value!(
    #[cfg_attr(
        all(feature = "wire-export", not(target_arch = "wasm32")),
        derive(specta::Type)
    )]
    OrganizationId,
    RecordId
);
identity_value!(
    #[cfg_attr(
        all(feature = "wire-export", not(target_arch = "wasm32")),
        derive(specta::Type)
    )]
    EnrollmentId,
    RecordId
);
identity_value!(RemoteStartId, RecordId);
identity_value!(InstallationId, ServiceId);

macro_rules! bounded_value {
    ($name:ident, $max:expr, $pattern:expr, $validate:expr) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl TryFrom<String> for $name {
            type Error = IdentityValidationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                if value.is_empty() || value.len() > $max || !($validate)(&value) {
                    return Err(IdentityValidationError { field: stringify!($name), requirement: "is not a valid bounded value" });
                }
                Ok(Self(value))
            }
        }
        impl From<$name> for String { fn from(value: $name) -> Self { value.0 } }
        impl $name { #[must_use] pub fn as_str(&self) -> &str { &self.0 } }
        impl JsonSchema for $name {
            fn schema_name() -> Cow<'static, str> { stringify!($name).into() }
            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!({"type":"string","minLength":1,"maxLength":$max,"pattern":$pattern,"x-maxUtf8Bytes":$max})
            }
        }
    };
}
// The wire uses Unix paths on every host; std::path::is_absolute is false on wasm32-unknown-unknown.
bounded_value!(AbsolutePath, 4096, "^/[^\\u0000]*$", |value: &str| !value
    .contains('\0')
    && value.starts_with('/'));
const MAXIMUM_WIRE_TIMESTAMP: i64 = 9_007_199_254_740_991;

macro_rules! unix_wire_timestamp {
    ($timestamp:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
        #[serde(transparent)]
        #[cfg_attr(
            all(feature = "wire-export", not(target_arch = "wasm32")),
            derive(specta::Type)
        )]
        pub struct $timestamp(
            #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = specta_typescript::Number))]
            pub i64,
        );

        impl TryFrom<i64> for $timestamp {
            type Error = IdentityValidationError;

            fn try_from(timestamp: i64) -> Result<Self, Self::Error> {
                if (0..=MAXIMUM_WIRE_TIMESTAMP).contains(&timestamp) {
                    Ok(Self(timestamp))
                } else {
                    Err(IdentityValidationError {
                        field: stringify!($timestamp),
                        requirement: "must be a nonnegative exact JSON integer",
                    })
                }
            }
        }

        impl<'de> Deserialize<'de> for $timestamp {
            fn deserialize<Decoder: serde::Deserializer<'de>>(
                decoder: Decoder,
            ) -> Result<Self, Decoder::Error> {
                Self::try_from(i64::deserialize(decoder)?).map_err(serde::de::Error::custom)
            }
        }

        impl JsonSchema for $timestamp {
            fn schema_name() -> Cow<'static, str> {
                stringify!($timestamp).into()
            }

            fn json_schema(generator: &mut SchemaGenerator) -> Schema {
                let mut schema = i64::json_schema(generator);
                schema.insert("minimum".into(), 0.into());
                schema.insert("maximum".into(), MAXIMUM_WIRE_TIMESTAMP.into());
                schema
            }
        }
    };
}

unix_wire_timestamp!(UnixSeconds);
unix_wire_timestamp!(UnixMillis);

/// A report or attach counter that every JSON consumer can read exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct SequenceNumber(u64);

impl TryFrom<u64> for SequenceNumber {
    type Error = IdentityValidationError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value <= 9_007_199_254_740_991 {
            Ok(Self(value))
        } else {
            Err(IdentityValidationError {
                field: "sequence",
                requirement: "must remain below 2^53 for exact JSON transport",
            })
        }
    }
}
impl From<SequenceNumber> for u64 {
    fn from(value: SequenceNumber) -> Self {
        value.0
    }
}
impl SequenceNumber {
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}
impl JsonSchema for SequenceNumber {
    fn schema_name() -> Cow<'static, str> {
        "SequenceNumber".into()
    }
    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({"type":"integer","minimum":0,"maximum":9007199254740991_u64})
    }
}
