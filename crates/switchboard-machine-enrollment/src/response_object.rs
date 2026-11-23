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

pub(crate) fn from_json<'de, Body: Deserialize<'de>>(
    body: &'de [u8],
) -> Result<Body, crate::DecodeAnswerError> {
    let mut reader = serde_json::Deserializer::from_slice(body);
    let value = deserialize_object(&mut reader).map_err(|_| crate::DecodeAnswerError::Malformed)?;
    reader
        .end()
        .map_err(|_| crate::DecodeAnswerError::Malformed)?;
    Ok(value)
}

#[derive(Default)]
pub(crate) struct DiscardText;

impl<'de> Deserialize<'de> for DiscardText {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;
        impl serde::de::Visitor<'_> for TextVisitor {
            type Value = DiscardText;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<DiscardText, E> {
                Ok(DiscardText)
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

pub(crate) struct DiscardTimestamp;

impl<'de> Deserialize<'de> for DiscardTimestamp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TimestampVisitor;
        impl serde::de::Visitor<'_> for TimestampVisitor {
            type Value = DiscardTimestamp;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a timestamp string or number")
            }

            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<DiscardTimestamp, E> {
                Ok(DiscardTimestamp)
            }

            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<DiscardTimestamp, E> {
                Ok(DiscardTimestamp)
            }

            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<DiscardTimestamp, E> {
                Ok(DiscardTimestamp)
            }

            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<DiscardTimestamp, E> {
                Ok(DiscardTimestamp)
            }
        }
        deserializer.deserialize_any(TimestampVisitor)
    }
}
