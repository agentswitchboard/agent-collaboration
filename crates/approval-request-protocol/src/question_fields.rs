use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::approval_values::{Description, Label, OfferedOptionId, QuestionFieldId};

pub const MAX_OFFERED_OPTIONS: usize = 16;
pub const MAX_QUESTION_FIELDS: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidOfferedOptions {
    #[error("options must contain 1 to {MAX_OFFERED_OPTIONS} entries")]
    Count,
    #[error("option IDs must be unique")]
    DuplicateOption,
}

pub(crate) fn validate_options<'a>(
    count: usize,
    ids: impl Iterator<Item = &'a OfferedOptionId>,
) -> Result<(), InvalidOfferedOptions> {
    if !(1..=MAX_OFFERED_OPTIONS).contains(&count) {
        return Err(InvalidOfferedOptions::Count);
    }
    let mut seen = BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            return Err(InvalidOfferedOptions::DuplicateOption);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ChoiceOption {
    pub option_id: OfferedOptionId,
    pub label: Label,
}

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
pub enum QuestionField {
    Text {
        field_id: QuestionFieldId,
        label: Label,
        description: Option<Description>,
        required: bool,
    },
    Number {
        field_id: QuestionFieldId,
        label: Label,
        description: Option<Description>,
        required: bool,
    },
    Boolean {
        field_id: QuestionFieldId,
        label: Label,
        description: Option<Description>,
        required: bool,
    },
    SingleChoice {
        field_id: QuestionFieldId,
        label: Label,
        description: Option<Description>,
        required: bool,
        options: Vec<ChoiceOption>,
    },
    MultiChoice {
        field_id: QuestionFieldId,
        label: Label,
        description: Option<Description>,
        required: bool,
        options: Vec<ChoiceOption>,
        min: Option<u8>,
        max: Option<u8>,
    },
}

impl QuestionField {
    #[must_use]
    pub const fn field_id(&self) -> &QuestionFieldId {
        match self {
            Self::Text { field_id, .. }
            | Self::Number { field_id, .. }
            | Self::Boolean { field_id, .. }
            | Self::SingleChoice { field_id, .. }
            | Self::MultiChoice { field_id, .. } => field_id,
        }
    }

    #[must_use]
    pub const fn required(&self) -> bool {
        match self {
            Self::Text { required, .. }
            | Self::Number { required, .. }
            | Self::Boolean { required, .. }
            | Self::SingleChoice { required, .. }
            | Self::MultiChoice { required, .. } => *required,
        }
    }

    fn validate(&self) -> Result<(), InvalidQuestionFields> {
        match self {
            Self::Text { .. } | Self::Number { .. } | Self::Boolean { .. } => Ok(()),
            Self::SingleChoice { options, .. } => {
                validate_options(
                    options.len(),
                    options.iter().map(|option| &option.option_id),
                )?;
                Ok(())
            }
            Self::MultiChoice {
                options, min, max, ..
            } => {
                validate_options(
                    options.len(),
                    options.iter().map(|option| &option.option_id),
                )?;
                let minimum = min.map_or(0, usize::from);
                let maximum = max.map_or(options.len(), usize::from);
                if minimum > maximum || maximum > options.len() {
                    Err(InvalidQuestionFields::SelectionRange)
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidQuestionFields {
    #[error("question fields must contain 1 to {MAX_QUESTION_FIELDS} entries")]
    Count,
    #[error("question field IDs must be unique")]
    DuplicateField,
    #[error("invalid field options: {0}")]
    Options(#[from] InvalidOfferedOptions),
    #[error("selection range must fit the field's options")]
    SelectionRange,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<QuestionField>", into = "Vec<QuestionField>")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct QuestionFields(Vec<QuestionField>);

impl QuestionFields {
    pub fn new(fields: Vec<QuestionField>) -> Result<Self, InvalidQuestionFields> {
        Self::try_from(fields)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[QuestionField] {
        &self.0
    }
}

impl TryFrom<Vec<QuestionField>> for QuestionFields {
    type Error = InvalidQuestionFields;

    fn try_from(fields: Vec<QuestionField>) -> Result<Self, Self::Error> {
        if !(1..=MAX_QUESTION_FIELDS).contains(&fields.len()) {
            return Err(InvalidQuestionFields::Count);
        }
        let mut seen = BTreeSet::new();
        for field in &fields {
            if !seen.insert(field.field_id()) {
                return Err(InvalidQuestionFields::DuplicateField);
            }
            field.validate()?;
        }
        Ok(Self(fields))
    }
}

impl From<QuestionFields> for Vec<QuestionField> {
    fn from(fields: QuestionFields) -> Self {
        fields.0
    }
}
