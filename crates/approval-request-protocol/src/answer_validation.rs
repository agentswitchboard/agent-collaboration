use crate::approval_wire_codec::approval_wire_union;
use std::collections::BTreeSet;

use crate::{
    Answer, ChoiceOption, OfferView, OfferedOptionId, OfferedOptions, QuestionAnswer,
    QuestionAnswerValue, QuestionField, QuestionFieldId, QuestionFields,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum InvalidAnswer {
    WrongKind {},
    UnofferedOption {
        option_id: OfferedOptionId,
    },
    UnofferedField {
        field_id: QuestionFieldId,
    },
    MissingRequiredField {
        field_id: QuestionFieldId,
    },
    RepeatedField {
        field_id: QuestionFieldId,
    },
    WrongValueType {
        field_id: QuestionFieldId,
    },
    RepeatedSelection {
        field_id: QuestionFieldId,
        option_id: OfferedOptionId,
    },
    WrongSelectionCount {
        field_id: QuestionFieldId,
    },
}
}

pub fn check_answer(offer: &OfferView, answer: &Answer) -> Result<(), InvalidAnswer> {
    match (offer, answer) {
        (OfferView::ProviderApproval { offer }, Answer::Approval { option_id }) => {
            check_offered_option(&offer.options, option_id)
        }
        (OfferView::ProviderQuestion { offer }, Answer::Question { answer }) => {
            check_question_answer(&offer.fields, answer)
        }
        (OfferView::UnprovenCall { .. }, Answer::UnprovenCall { .. }) => Ok(()),
        (
            OfferView::ProviderApproval { .. },
            Answer::Question { .. } | Answer::UnprovenCall { .. },
        )
        | (
            OfferView::ProviderQuestion { .. },
            Answer::Approval { .. } | Answer::UnprovenCall { .. },
        )
        | (OfferView::UnprovenCall { .. }, Answer::Approval { .. } | Answer::Question { .. }) => {
            Err(InvalidAnswer::WrongKind {})
        }
    }
}

fn check_offered_option(
    options: &OfferedOptions,
    option_id: &OfferedOptionId,
) -> Result<(), InvalidAnswer> {
    if options
        .as_slice()
        .iter()
        .any(|offered| &offered.id == option_id)
    {
        Ok(())
    } else {
        Err(InvalidAnswer::UnofferedOption {
            option_id: option_id.clone(),
        })
    }
}

fn check_question_answer(
    fields: &QuestionFields,
    answer: &QuestionAnswer,
) -> Result<(), InvalidAnswer> {
    let QuestionAnswer::Answered {
        fields: answered_fields,
    } = answer
    else {
        return Ok(());
    };
    let mut answered_ids = BTreeSet::new();
    for answered_field in answered_fields {
        let field_id = &answered_field.field_id;
        if !answered_ids.insert(field_id) {
            return Err(InvalidAnswer::RepeatedField {
                field_id: field_id.clone(),
            });
        }
        let field = fields
            .as_slice()
            .iter()
            .find(|field| field.field_id() == field_id)
            .ok_or_else(|| InvalidAnswer::UnofferedField {
                field_id: field_id.clone(),
            })?;
        check_field_value(field, &answered_field.value)?;
    }
    for field in fields.as_slice() {
        if field.required() && !answered_ids.contains(field.field_id()) {
            return Err(InvalidAnswer::MissingRequiredField {
                field_id: field.field_id().clone(),
            });
        }
    }
    Ok(())
}

fn check_field_value(
    field: &QuestionField,
    value: &QuestionAnswerValue,
) -> Result<(), InvalidAnswer> {
    match (field, value) {
        (QuestionField::Text { .. }, QuestionAnswerValue::Text { .. })
        | (QuestionField::Number { .. }, QuestionAnswerValue::Number { .. })
        | (QuestionField::Boolean { .. }, QuestionAnswerValue::Boolean { .. }) => Ok(()),
        (
            QuestionField::SingleChoice {
                field_id, options, ..
            },
            QuestionAnswerValue::SelectedOptions {
                selected_option_ids,
            },
        ) => check_selection(field_id, options.as_slice(), selected_option_ids, 1..=1),
        (
            QuestionField::MultiChoice {
                field_id,
                selection,
                ..
            },
            QuestionAnswerValue::SelectedOptions {
                selected_option_ids,
            },
        ) => check_selection(
            field_id,
            selection.options(),
            selected_option_ids,
            selection.counts(),
        ),
        (
            QuestionField::Text { .. },
            QuestionAnswerValue::Number { .. }
            | QuestionAnswerValue::Boolean { .. }
            | QuestionAnswerValue::SelectedOptions { .. },
        )
        | (
            QuestionField::Number { .. },
            QuestionAnswerValue::Text { .. }
            | QuestionAnswerValue::Boolean { .. }
            | QuestionAnswerValue::SelectedOptions { .. },
        )
        | (
            QuestionField::Boolean { .. },
            QuestionAnswerValue::Text { .. }
            | QuestionAnswerValue::Number { .. }
            | QuestionAnswerValue::SelectedOptions { .. },
        )
        | (
            QuestionField::SingleChoice { .. } | QuestionField::MultiChoice { .. },
            QuestionAnswerValue::Text { .. }
            | QuestionAnswerValue::Number { .. }
            | QuestionAnswerValue::Boolean { .. },
        ) => Err(InvalidAnswer::WrongValueType {
            field_id: field.field_id().clone(),
        }),
    }
}

fn check_selection(
    field_id: &QuestionFieldId,
    options: &[ChoiceOption],
    selected: &[OfferedOptionId],
    counts: std::ops::RangeInclusive<usize>,
) -> Result<(), InvalidAnswer> {
    let mut selected_ids = BTreeSet::new();
    for option_id in selected {
        if !selected_ids.insert(option_id) {
            return Err(InvalidAnswer::RepeatedSelection {
                field_id: field_id.clone(),
                option_id: option_id.clone(),
            });
        }
        if !options.iter().any(|option| &option.option_id == option_id) {
            return Err(InvalidAnswer::UnofferedOption {
                option_id: option_id.clone(),
            });
        }
    }
    if !counts.contains(&selected.len()) {
        return Err(InvalidAnswer::WrongSelectionCount {
            field_id: field_id.clone(),
        });
    }
    Ok(())
}
