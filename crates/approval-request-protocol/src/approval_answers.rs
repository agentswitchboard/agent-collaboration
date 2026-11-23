use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;

use crate::approval_values::{OfferedOptionId, QuestionFieldId, QuestionNumber};

approval_wire_union! {
/// The two answer kinds a parent session can send or receive.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ProviderAnswer {
    Approval { option_id: OfferedOptionId },
    Question { answer: QuestionAnswer },
}
}

impl From<ProviderAnswer> for Answer {
    fn from(answer: ProviderAnswer) -> Self {
        match answer {
            ProviderAnswer::Approval { option_id } => Self::Approval { option_id },
            ProviderAnswer::Question { answer } => Self::Question { answer },
        }
    }
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Answer {
    Approval { option_id: OfferedOptionId },
    Question { answer: QuestionAnswer },
    UnprovenCall { decision: crate::SessionCallDecision },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum QuestionAnswer {
    Answered { fields: Vec<QuestionFieldAnswer> },
    Declined {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct QuestionFieldAnswer {
    pub field_id: QuestionFieldId,
    pub value: QuestionAnswerValue,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum QuestionAnswerValue {
    SelectedOptions { selected_option_ids: Vec<OfferedOptionId> },
    Text { value: String },
    Number { value: QuestionNumber },
    Boolean { value: bool },
}
}
