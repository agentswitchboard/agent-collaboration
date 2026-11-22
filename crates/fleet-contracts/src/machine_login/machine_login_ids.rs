use message_board::{IdentityValidationError, RecordId};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

macro_rules! central_identity {
    ($($(#[$attribute:meta])* $name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        $(#[$attribute])*
        pub struct $name(RecordId);
        impl JsonSchema for $name {
            fn schema_name() -> std::borrow::Cow<'static, str> { stringify!($name).into() }
            fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                RecordId::json_schema(generator)
            }
        }
        impl TryFrom<String> for $name {
            type Error = IdentityValidationError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                RecordId::try_from(value).map(Self)
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self { value.0.into() }
        }
        impl $name { #[must_use] pub fn as_str(&self) -> &str { self.0.as_str() } }
    )+};
}
central_identity!(
    #[cfg_attr(
        all(feature = "wire-export", not(target_arch = "wasm32")),
        derive(specta::Type)
    )]
    PersonId,
    #[cfg_attr(
        all(feature = "wire-export", not(target_arch = "wasm32")),
        derive(specta::Type)
    )]
    DeviceLoginRequestId,
);
