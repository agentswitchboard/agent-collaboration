//! Approval outcomes shared by the person socket and the signed routes.

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    Answer, ApprovalRequestId, CentralTime, DecidedBy, GiverDecision, InvalidAnswer,
    InvalidApprovalEnding, RequestSummary, RequestView,
};

macro_rules! reply_union {
    ($name:ident as $remote:literal {
         $($(#[$variant_note:meta])* $variant:ident { $($(#[$field_annotation:meta])* $field:ident: $ty:ty $(=> $reader:literal)?),* $(,)? }),+ $(,)?
    }) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
        #[serde(tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
        pub enum $name { $($(#[$variant_note])* $variant { $($(#[$field_annotation])* $field: $ty),* }),+ }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
                #[derive(Deserialize)]
                #[serde(remote = $remote, tag = "kind", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]
                enum Arms { $($variant { $($(#[serde(deserialize_with = $reader)])? $field: $ty),* }),+ }

                Arms::deserialize(NamedReplyMap(input))
            }
        }
    };
}

// The remote enum reader and the public enum share their field and tag definitions.
struct NamedReplyMap<Input>(Input);

impl<'de, Input: Deserializer<'de>> Deserializer<'de> for NamedReplyMap<Input> {
    type Error = Input::Error;

    fn deserialize_any<Reader: serde::de::Visitor<'de>>(
        self,
        reader: Reader,
    ) -> Result<Reader::Value, Self::Error> {
        self.0.deserialize_map(reader)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ApprovalListFilter {
    Waiting,
    All,
}

fn required_cursor<'de, Input: Deserializer<'de>>(
    input: Input,
) -> Result<Option<ApprovalRequestId>, Input::Error> {
    Option::<ApprovalRequestId>::deserialize(input)
}

reply_union! {
    ApprovalListReply as "ApprovalListReply" {
        Page {
            items: Vec<RequestSummary>,
            #[serde(deserialize_with = "required_cursor")]
            #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = Option<ApprovalRequestId>))]
            next: Option<ApprovalRequestId> => "required_cursor",
        },
    }
}

reply_union! {
    ApprovalReadReply as "ApprovalReadReply" {
        /// Window and ending timestamps are RFC 3339 CentralTime strings.
        Request { view: RequestView },
        NotVisible {},
    }
}

reply_union! {
    ApprovalDecideReply as "ApprovalDecideReply" {
        SavedDecision { decision: GiverDecision<Answer> },
        /// An actual saved decision. Its time is an RFC 3339 CentralTime string.
        AlreadyDecided { by: DecidedBy, at: CentralTime },
        /// The request ended before this answer, through expiry or holder/provider closure.
        TooLate {},
        Invalid { reason: ApprovalInvalidReason },
        NotVisible {},
        OperationConflict {},
    }
}

reply_union! {
    ApprovalInvalidReason as "ApprovalInvalidReason" {
        InvalidAnswer { reason: InvalidAnswer },
        InvalidApprovalEnding { reason: InvalidApprovalEnding },
    }
}
