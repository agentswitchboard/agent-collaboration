use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::approval_values::{
    Description, Label, MAX_OFFERED_OPTIONS, MAX_QUESTION_FIELDS, OfferedOptionId, QuestionFieldId,
};

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

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ChoiceOption {
    pub option_id: OfferedOptionId,
    pub label: Label,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
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
        options: ChoiceOptions,
    },
    MultiChoice {
        field_id: QuestionFieldId,
        label: Label,
        description: Option<Description>,
        required: bool,
        selection: MultiChoiceSelection,
    },
}
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(try_from = "Vec<ChoiceOption>", into = "Vec<ChoiceOption>")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ChoiceOptions(Vec<ChoiceOption>);

impl ChoiceOptions {
    pub fn new(options: Vec<ChoiceOption>) -> Result<Self, InvalidOfferedOptions> {
        Self::try_from(options)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[ChoiceOption] {
        &self.0
    }
}

impl TryFrom<Vec<ChoiceOption>> for ChoiceOptions {
    type Error = InvalidOfferedOptions;

    fn try_from(options: Vec<ChoiceOption>) -> Result<Self, Self::Error> {
        validate_options(
            options.len(),
            options.iter().map(|option| &option.option_id),
        )?;
        Ok(Self(options))
    }
}

impl From<ChoiceOptions> for Vec<ChoiceOption> {
    fn from(options: ChoiceOptions) -> Self {
        options.0
    }
}

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    struct MultiChoiceSelectionFields {
        options: ChoiceOptions,
        min: Option<u8>,
        max: Option<u8>,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    try_from = "MultiChoiceSelectionFields",
    into = "MultiChoiceSelectionFields"
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct MultiChoiceSelection(MultiChoiceSelectionFields);

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("selection range must fit the field's options")]
pub struct InvalidChoiceRange;

impl MultiChoiceSelection {
    pub fn new(
        options: ChoiceOptions,
        min: Option<u8>,
        max: Option<u8>,
    ) -> Result<Self, InvalidChoiceRange> {
        Self::try_from(MultiChoiceSelectionFields { options, min, max })
    }

    #[must_use]
    pub fn options(&self) -> &[ChoiceOption] {
        self.0.options.as_slice()
    }

    #[must_use]
    pub fn counts(&self) -> std::ops::RangeInclusive<usize> {
        self.0.min.map_or(0, usize::from)..=self.0.max.map_or(self.options().len(), usize::from)
    }
}

impl TryFrom<MultiChoiceSelectionFields> for MultiChoiceSelection {
    type Error = InvalidChoiceRange;

    fn try_from(fields: MultiChoiceSelectionFields) -> Result<Self, Self::Error> {
        let selection = Self(fields);
        let bounds = selection.counts();
        if bounds.is_empty() || *bounds.end() > selection.options().len() {
            Err(InvalidChoiceRange)
        } else {
            Ok(selection)
        }
    }
}

impl From<MultiChoiceSelection> for MultiChoiceSelectionFields {
    fn from(selection: MultiChoiceSelection) -> Self {
        selection.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidQuestionFields {
    #[error("question fields must contain 1 to {MAX_QUESTION_FIELDS} entries")]
    Count,
    #[error("question field IDs must be unique")]
    DuplicateField,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
        }
        Ok(Self(fields))
    }
}

impl From<QuestionFields> for Vec<QuestionField> {
    fn from(fields: QuestionFields) -> Self {
        fields.0
    }
}
