//! Keep Serde's field and tag validation while reading records only as maps.

use serde::{Deserializer, de::Visitor};

pub(super) struct NamedMap<Decoder>(pub Decoder);

impl<'de, Decoder: Deserializer<'de>> Deserializer<'de> for NamedMap<Decoder> {
    type Error = Decoder::Error;

    fn deserialize_any<Reader: Visitor<'de>>(
        self,
        visitor: Reader,
    ) -> Result<Reader::Value, Self::Error> {
        self.0.deserialize_map(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}

// Derive the private field reader from the same declaration as the public type.
macro_rules! named_wire_record {
    ($(#[$attribute:meta])* pub $record:ident $name:ident as $remote:literal
        [$($serde_rules:tt)*] { $($body:tt)* }
    ) => {
        $(#[$attribute])*
        #[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, schemars::JsonSchema)]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
        #[serde($($serde_rules)*)]
        pub $record $name { $($body)* }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
                #[derive(serde::Deserialize)]
                #[serde(remote = $remote, $($serde_rules)*)]
                $record Fields { $($body)* }

                Fields::deserialize(crate::named_wire_record::NamedMap(decoder))
            }
        }
    };
}
