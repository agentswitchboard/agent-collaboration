use serde::{Deserialize, Deserializer, Serialize, de};

use crate::approval_values::{OfferedOptionId, QuestionFieldId, QuestionNumber};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Answer {
    Approval { option_id: OfferedOptionId },
    Question { answer: QuestionAnswer },
    Call { option_id: OfferedOptionId },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum QuestionAnswer {
    Answered { fields: Vec<QuestionFieldAnswer> },
    Declined {},
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct QuestionFieldAnswer {
    pub field_id: QuestionFieldId,
    pub value: QuestionAnswerValue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum QuestionAnswerValue {
    SelectedOptions(ChoiceSelection),
    Text(String),
    Number(QuestionNumber),
    Boolean(bool),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ChoiceSelection {
    pub selected_option_ids: Vec<OfferedOptionId>,
}

// Dispatch by the JSON value's actual shape so an invalid number keeps its
// validation error, which a derived untagged decoder would discard.
impl<'de> Deserialize<'de> for QuestionAnswerValue {
    fn deserialize<DeserializerType: Deserializer<'de>>(
        deserializer: DeserializerType,
    ) -> Result<Self, DeserializerType::Error> {
        deserializer.deserialize_any(QuestionAnswerValueVisitor)
    }
}

struct QuestionAnswerValueVisitor;

impl<'de> de::Visitor<'de> for QuestionAnswerValueVisitor {
    type Value = QuestionAnswerValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an answer string, number, boolean, or choice selection")
    }

    fn visit_i64<Error: de::Error>(self, value: i64) -> Result<Self::Value, Error> {
        QuestionNumber::try_from(serde_json::Number::from(value))
            .map(QuestionAnswerValue::Number)
            .map_err(Error::custom)
    }

    fn visit_u64<Error: de::Error>(self, value: u64) -> Result<Self::Value, Error> {
        QuestionNumber::try_from(serde_json::Number::from(value))
            .map(QuestionAnswerValue::Number)
            .map_err(Error::custom)
    }

    fn visit_f64<Error: de::Error>(self, value: f64) -> Result<Self::Value, Error> {
        QuestionNumber::try_from(value)
            .map(QuestionAnswerValue::Number)
            .map_err(Error::custom)
    }

    fn visit_str<Error: de::Error>(self, value: &str) -> Result<Self::Value, Error> {
        Ok(QuestionAnswerValue::Text(value.to_owned()))
    }

    fn visit_string<Error: de::Error>(self, value: String) -> Result<Self::Value, Error> {
        Ok(QuestionAnswerValue::Text(value))
    }

    fn visit_bool<Error: de::Error>(self, value: bool) -> Result<Self::Value, Error> {
        Ok(QuestionAnswerValue::Boolean(value))
    }

    fn visit_map<Access: de::MapAccess<'de>>(
        self,
        fields: Access,
    ) -> Result<Self::Value, Access::Error> {
        ChoiceSelection::deserialize(de::value::MapAccessDeserializer::new(fields))
            .map(QuestionAnswerValue::SelectedOptions)
    }
}
