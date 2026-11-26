//! Central approval commands and the retained decision fingerprint.

use approval_request_protocol::{Answer, ApprovalRequestId, canonical_json, decode_named_map};
pub use approval_request_protocol::{
    ApprovalDecideReply, ApprovalInvalidReason, ApprovalListFilter, ApprovalListReply,
    ApprovalReadReply,
};
use fleet_identity_protocol::OrganizationId;
use serde::{Deserialize, Deserializer, Serialize};
use workspace_ids::RequestId;

use crate::person_proof::{PersonRequestRefusal, sha256_url};

macro_rules! named_record {
    (struct $name:ident [$($serde_rules:tt)*] {
        $($(#[$field_annotation:meta])* $vis:vis $field:ident: $ty:ty $(=> $reader:literal)?),+ $(,)?
    }) => {
        #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
        #[serde($($serde_rules)*)]
        pub struct $name { $($(#[$field_annotation])* $vis $field: $ty),+ }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<Input: Deserializer<'de>>(input: Input) -> Result<Self, Input::Error> {
                #[derive(Deserialize)]
                #[serde($($serde_rules)*)]
                struct Fields { $($(#[serde(deserialize_with = $reader)])? $field: $ty),+ }

                let Fields { $($field),+ } = decode_named_map(input)?;
                Ok(Self { $($field),+ })
            }
        }
    };
}

named_record! {
    struct ApprovalListRequest [rename_all = "camelCase", deny_unknown_fields] {
        pub organization: OrganizationId,
        pub filter: ApprovalListFilter,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub cursor: Option<ApprovalRequestId>,
    }
}

named_record! {
    struct ApprovalReadRequest [rename_all = "camelCase", deny_unknown_fields] {
        pub organization: OrganizationId,
        pub request_id: ApprovalRequestId,
    }
}

named_record! {
    struct ApprovalDecideRequest [rename_all = "camelCase", deny_unknown_fields] {
        pub organization: OrganizationId,
        pub request_id: ApprovalRequestId,
        pub operation_id: RequestId,
        pub answer: Answer,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalPersonOperation {
    List(ApprovalListRequest),
    Read(ApprovalReadRequest),
    Decide(ApprovalDecideRequest),
}

impl ApprovalPersonOperation {
    #[must_use]
    pub const fn organization(&self) -> &OrganizationId {
        match self {
            Self::List(request) => &request.organization,
            Self::Read(request) => &request.organization,
            Self::Decide(request) => &request.organization,
        }
    }
}

impl ApprovalDecideRequest {
    pub fn intent_digest(&self) -> Result<String, PersonRequestRefusal> {
        Ok(sha256_url(self.canonical_intent()?.as_bytes()))
    }

    fn canonical_intent(&self) -> Result<String, PersonRequestRefusal> {
        let intent = ApprovalIntent {
            operation: "ApprovalDecide",
            v: 1,
            organization: &self.organization,
            target: &self.request_id,
            operation_id: &self.operation_id,
            params: ApprovalDecideIntentParams {
                answer: &self.answer,
            },
        };
        let json = serde_json::to_value(intent).map_err(|_| PersonRequestRefusal::OperationBody)?;
        canonical_json(&json).map_err(|_| PersonRequestRefusal::OperationBody)
    }
}

/// The operation log stores a hash of these canonical bytes.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApprovalIntent<'request> {
    operation: &'static str,
    v: u8,
    organization: &'request OrganizationId,
    target: &'request ApprovalRequestId,
    operation_id: &'request RequestId,
    params: ApprovalDecideIntentParams<'request>,
}

#[derive(Serialize)]
struct ApprovalDecideIntentParams<'request> {
    answer: &'request Answer,
}

#[cfg(test)]
#[path = "approval_person_commands_test.rs"]
mod tests;
