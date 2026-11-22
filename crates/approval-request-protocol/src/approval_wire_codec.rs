//! JSON objects are accepted only in their named map form.
use std::{fmt, marker::PhantomData};

use serde::de::{Deserializer, MapAccess, Visitor, value::MapAccessDeserializer};

pub(crate) fn decode_named_map<'de, Record: serde::Deserialize<'de>, Input: Deserializer<'de>>(
    deserializer: Input,
) -> Result<Record, Input::Error> {
    struct NamedMap<Record>(PhantomData<Record>);

    impl<'de, Record: serde::Deserialize<'de>> Visitor<'de> for NamedMap<Record> {
        type Value = Record;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an object with named fields")
        }

        fn visit_map<Fields: MapAccess<'de>>(
            self,
            fields: Fields,
        ) -> Result<Record, Fields::Error> {
            Record::deserialize(MapAccessDeserializer::new(fields))
        }
    }

    deserializer.deserialize_map(NamedMap(PhantomData))
}

/// Declare a wire struct and its named-field decoder from the same field list.
/// Field attributes apply to the exported shape. An optional decoder also
/// constrains a nested shared struct whose own codec is outside this crate.
macro_rules! approval_wire_record {
    (
        [$($serde_option:ident),*]
        $(#[$attribute:meta])*
        $visibility:vis struct $record:ident {
            $($(#[$field_attribute:meta])* $field_visibility:vis $field:ident: $field_type:ty $(=> $field_decoder:literal)?),* $(,)?
        }
    ) => {
        $(#[$attribute])*
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase", $($serde_option),*)]
        $visibility struct $record {
            $($(#[$field_attribute])* $field_visibility $field: $field_type),*
        }

        impl<'de> serde::Deserialize<'de> for $record {
            fn deserialize<Input: serde::Deserializer<'de>>(
                deserializer: Input,
            ) -> Result<Self, Input::Error> {
                #[derive(serde::Deserialize)]
                #[serde(rename_all = "camelCase", $($serde_option),*)]
                struct NamedFields {
                    $($(#[serde(deserialize_with = $field_decoder)])? $field: $field_type),*
                }

                let fields = $crate::approval_wire_codec::decode_named_map::<NamedFields, _>(deserializer)?;
                Ok(Self { $($field: fields.$field),* })
            }
        }
    };
}
pub(crate) use approval_wire_record;

/// Use the same named-arm list for the wire union and its map-only decoder.
/// Serde's built-in internally tagged decoder also accepts a sequence.
macro_rules! approval_wire_union {
    (
        $(#[$attribute:meta])*
        $visibility:vis enum $union:ident $(<$($parameter:ident),+>)? {
            $(
                $(#[$variant_attribute:meta])*
                $variant:ident {
                    $($(#[$field_attribute:meta])* $field:ident: $field_type:ty $(=> $field_decoder:literal)?),* $(,)?
                }
            ),+ $(,)?
        }
    ) => {
        $(#[$attribute])*
        #[derive(serde::Serialize)]
        #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
        $visibility enum $union $(<$($parameter),+>)? {
            $(
                $(#[$variant_attribute])*
                $variant { $($(#[$field_attribute])* $field: $field_type),* }
            ),+
        }

        impl<'de $(, $($parameter: serde::Deserialize<'de>),+)?> serde::Deserialize<'de>
            for $union $(<$($parameter),+>)?
        {
            fn deserialize<Input: serde::Deserializer<'de>>(
                deserializer: Input,
            ) -> Result<Self, Input::Error> {
                #[derive(serde::Deserialize)]
                #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
                enum NamedArms $(<$($parameter),+>)? {
                    $($variant { $($(#[serde(deserialize_with = $field_decoder)])? $field: $field_type),* }),+
                }

                Ok(match $crate::approval_wire_codec::decode_named_map::<NamedArms $(<$($parameter),+>)?, _>(deserializer)? {
                    $(NamedArms::$variant { $($field),* } => Self::$variant { $($field),* }),+
                })
            }
        }
    };
}
pub(crate) use approval_wire_union;
