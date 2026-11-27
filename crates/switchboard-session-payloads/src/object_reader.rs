//! Deserialize a named wire object through its derived field rules.

use std::{fmt, marker::PhantomData};

use serde::{
    Deserialize, Deserializer,
    de::{MapAccess, Visitor, value::MapAccessDeserializer},
};

pub(super) fn deserialize_object<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct ObjectVisitor<T>(PhantomData<fn() -> T>);

    impl<'de, T: Deserialize<'de>> Visitor<'de> for ObjectVisitor<T> {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an object map")
        }

        fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
            T::deserialize(MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_map(ObjectVisitor(PhantomData))
}

// Keep the declared fields in one place while giving the derived reader only
// a map. Serde's usual struct reader also permits positional arrays.
macro_rules! wire_object {
    ($(#[$attribute:meta])* $vis:vis struct $name:ident {
        $($(#[$field_attribute:meta])* $field_vis:vis $field:ident: $ty:ty),* $(,)?
    }) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        $vis struct $name {
            $($(#[$field_attribute])* $field_vis $field: $ty,)*
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(serde::Deserialize)]
                #[serde(rename_all = "camelCase", deny_unknown_fields)]
                struct Wire {
                    $($(#[$field_attribute])* $field: $ty,)*
                }

                let Wire { $($field),* } = $crate::object_reader::deserialize_object(deserializer)?;
                Ok(Self { $($field),* })
            }
        }
    };
}

// A unit variant reads through an empty named-fields variant so that it has
// the same unknown-field check as variants with payloads.
macro_rules! wire_enum {
    ($(#[$attribute:meta])* $vis:vis enum $name:ident {
        $($(#[$variant_attribute:meta])* $variant:ident $(
            { $($(#[$field_attribute:meta])* $field:ident: $ty:ty),* $(,)? }
        )?),* $(,)?
    }) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, schemars::JsonSchema)]
        #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
        $vis enum $name {
            $($(#[$variant_attribute])* $variant $({ $($(#[$field_attribute])* $field: $ty,)* })?,)*
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(serde::Deserialize)]
                #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
                enum Wire {
                    $($variant { $($($(#[$field_attribute])* $field: $ty,)*)? },)*
                }

                let wire = $crate::object_reader::deserialize_object(deserializer)?;
                Ok(match wire {
                    $(Wire::$variant { $($($field),*)? } => Self::$variant $({ $($field),* })?,)*
                })
            }
        }
    };
}

// A fieldless enum accepts only a string. Serde's usual externally tagged
// enum reader also permits a single-key map with a null value.
macro_rules! wire_string_enum {
    ($(#[$attribute:meta])* $vis:vis enum $name:ident {
        $($variant:ident),* $(,)?
    }) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase")]
        $vis enum $name {
            $($variant,)*
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(serde::Deserialize)]
                #[serde(rename_all = "camelCase")]
                enum Wire {
                    $($variant,)*
                }

                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                let wire = <Wire as serde::Deserialize>::deserialize(
                    serde::de::value::StringDeserializer::<D::Error>::new(value),
                )?;
                Ok(match wire {
                    $(Wire::$variant => Self::$variant,)*
                })
            }
        }
    };
}

pub(super) use {wire_enum, wire_object, wire_string_enum};
