//! Closed approval calls for a destination session and notices for its Router.
use std::fmt::Debug;

use serde::{Deserialize, Deserializer};
use workspace_ids::RequestId;

use crate::{
    Answer, ApprovalDecision, ApprovalRequestId, CentralSessionId, CentralTime, DecidedBy,
    InvalidAnswer, RequestSummary, RequestView,
    approval_wire_codec::{approval_wire_record, approval_wire_union},
};

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
/// The machine link supplies its validated secret type. This is a presented credential.
pub struct ApprovalSessionCall<Secret, Payload> {
    pub session_id: CentralSessionId,
    pub secret: Secret,
    pub input: Payload,
}
}

impl<Secret, Input: Debug> Debug for ApprovalSessionCall<Secret, Input> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApprovalSessionCall")
            .field("session_id", &self.session_id)
            .field("input", &self.input)
            .finish_non_exhaustive()
    }
}

type NullableApprovalCursor = Option<ApprovalRequestId>;

fn nullable_approval_cursor_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    generator.subschema_for::<NullableApprovalCursor>()
}

fn required_next<'de, Input: Deserializer<'de>>(
    deserializer: Input,
) -> Result<NullableApprovalCursor, Input::Error> {
    NullableApprovalCursor::deserialize(deserializer)
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct ListApprovalsForSessionRequest {
    pub cursor: NullableApprovalCursor,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ListApprovalsForSessionReply {
    Page {
        items: Vec<RequestSummary>,
        #[serde(deserialize_with = "required_next")]
        #[schemars(required, schema_with = "nullable_approval_cursor_schema")]
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = NullableApprovalCursor))]
        next: NullableApprovalCursor => "required_next",
    },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct ReadApprovalForSessionRequest {
    pub request_id: ApprovalRequestId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ReadApprovalForSessionReply {
    Request { view: Box<RequestView> },
    NotVisible {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct DecideApprovalAsSessionRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
    pub value: Answer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum DecideApprovalAsSessionReply {
    SavedDecision { decision: ApprovalDecision },
    AlreadyDecided { by: DecidedBy, at: CentralTime },
    TooLate {},
    PersonRequired {},
    Invalid { reason: InvalidAnswer },
    NotVisible {},
    OperationConflict {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct PassOnApprovalRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ApprovalDestination {
    Session { session_id: CentralSessionId },
    People {},
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum PassOnApprovalReply {
    PassedOn { to: ApprovalDestination },
    AlreadyDecided { by: DecidedBy, at: CentralTime },
    TooLate {},
    NotVisible {},
    OperationConflict {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
/// The destination session is part of the typed link address.
pub struct ApprovalNotice {
    pub request_id: ApprovalRequestId,
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq, schemars::JsonSchema)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ApprovalNoticeReply {
    Received {},
    NotHere {},
}
}
