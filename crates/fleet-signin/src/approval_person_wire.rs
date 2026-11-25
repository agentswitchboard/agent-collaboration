//! The private sign-in wire for a person's approval requests and replies.

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
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
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
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        pub operation_id: RequestId,
        pub answer: Answer,
    }
}

type NullableApprovalCursor = Option<ApprovalRequestId>;

/// Selected by the private HTTP route before any request body is interpreted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalPersonRoute {
    List,
    Read,
    Decide,
}

impl ApprovalPersonRoute {
    pub const ALL: [Self; 3] = [Self::List, Self::Read, Self::Decide];

    #[must_use]
    pub fn from_path(path: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|route| route.path() == path)
    }

    #[must_use]
    pub const fn path(self) -> &'static str {
        match self {
            Self::List => "/organization-admin/v1/approval-list",
            Self::Read => "/organization-admin/v1/approval-read",
            Self::Decide => "/organization-admin/v1/approval-decide",
        }
    }

    pub(crate) fn read(self, body: &[u8]) -> Result<ApprovalPersonOperation, PersonRequestRefusal> {
        let invalid = |_| PersonRequestRefusal::OperationBody;
        match self {
            Self::List => serde_json::from_slice(body)
                .map(ApprovalPersonOperation::List)
                .map_err(invalid),
            Self::Read => serde_json::from_slice(body)
                .map(ApprovalPersonOperation::Read)
                .map_err(invalid),
            Self::Decide => serde_json::from_slice(body)
                .map(ApprovalPersonOperation::Decide)
                .map_err(invalid),
        }
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

    #[must_use]
    pub fn intent(&self) -> ApprovalIntent {
        let organization = self.organization().clone();
        match self {
            Self::List(request) => ApprovalIntent::ApprovalList {
                v: ApprovalIntentVersion,
                target: organization.clone(),
                organization,
                operation_id: (),
                params: ApprovalListIntentParams {
                    filter: request.filter,
                    cursor: request.cursor.clone(),
                },
            },
            Self::Read(request) => ApprovalIntent::ApprovalRead {
                v: ApprovalIntentVersion,
                organization,
                target: request.request_id.clone(),
                operation_id: (),
                params: EmptyApprovalIntentParams {},
            },
            Self::Decide(request) => ApprovalIntent::ApprovalDecide {
                v: ApprovalIntentVersion,
                organization,
                target: request.request_id.clone(),
                operation_id: request.operation_id.clone(),
                params: ApprovalDecideIntentParams {
                    answer: request.answer.clone(),
                },
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
#[serde(tag = "operation", rename_all_fields = "camelCase")]
pub enum ApprovalIntent {
    ApprovalList {
        v: ApprovalIntentVersion,
        organization: OrganizationId,
        target: OrganizationId,
        operation_id: (),
        params: ApprovalListIntentParams,
    },
    ApprovalRead {
        v: ApprovalIntentVersion,
        organization: OrganizationId,
        target: ApprovalRequestId,
        operation_id: (),
        params: EmptyApprovalIntentParams,
    },
    ApprovalDecide {
        v: ApprovalIntentVersion,
        organization: OrganizationId,
        target: ApprovalRequestId,
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        operation_id: RequestId,
        params: ApprovalDecideIntentParams,
    },
}

impl ApprovalIntent {
    pub fn canonical(&self) -> Result<String, PersonRequestRefusal> {
        let json = serde_json::to_value(self).map_err(|_| PersonRequestRefusal::OperationBody)?;
        canonical_json(&json).map_err(|_| PersonRequestRefusal::OperationBody)
    }

    pub fn digest(&self) -> Result<String, PersonRequestRefusal> {
        Ok(sha256_url(self.canonical()?.as_bytes()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(into = "u8")]
pub struct ApprovalIntentVersion;

impl From<ApprovalIntentVersion> for u8 {
    fn from(_: ApprovalIntentVersion) -> Self {
        1
    }
}

#[cfg(all(feature = "wire-export", not(target_arch = "wasm32")))]
impl specta::Type for ApprovalIntentVersion {
    fn definition(_: &mut specta::Types) -> specta::datatype::DataType {
        specta_typescript::define("1").into()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalListIntentParams {
    pub filter: ApprovalListFilter,
    #[serde(deserialize_with = "required_cursor")]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = NullableApprovalCursor))]
    pub cursor: NullableApprovalCursor,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct EmptyApprovalIntentParams {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ApprovalDecideIntentParams {
    pub answer: Answer,
}
