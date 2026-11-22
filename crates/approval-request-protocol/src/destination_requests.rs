use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use crate::{
    ApprovalPageItems, ApprovalPageSize, PersonApprovalContinuation, SessionApprovalContinuation,
};
use workspace_ids::{ApprovalRequestId, RequestId};

use crate::{
    Answer, ApprovalDecision, EndingView, InvalidAnswer, OfferView, RequestView, WaitingRevision,
};

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ListPersonRequests {
    pub page_size: ApprovalPageSize,
    pub continuation: Option<PersonApprovalContinuation>,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ListPersonRequestsReply {
    Listed {
        requests: ApprovalPageItems,
        continuation: Option<PersonApprovalContinuation>,
    },
    Refused {
        refusal: Refusal,
    },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ReadPersonRequest {
    pub request_id: ApprovalRequestId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ReadPersonRequestReply {
    Read { request: RequestView },
    Refused { refusal: Refusal },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct DecidePersonRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
    pub answer: Answer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecidePersonReply {
    Decided { decision: ApprovalDecision },
    Refused { refusal: Refusal },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ListSessionRequests {
    pub page_size: ApprovalPageSize,
    pub continuation: Option<SessionApprovalContinuation>,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ListSessionRequestsReply {
    Listed {
        requests: ApprovalPageItems,
        continuation: Option<SessionApprovalContinuation>,
    },
    Refused {
        refusal: Refusal,
    },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ReadSessionRequest {
    pub request_id: ApprovalRequestId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ReadSessionRequestReply {
    Read { request: RequestView },
    Refused { refusal: Refusal },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct DecideSessionRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
    pub answer: Answer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecideSessionReply {
    Decided { decision: ApprovalDecision },
    Refused { refusal: Refusal },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct PassOnRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum PassOnReply {
    PassedOn {},
    Refused { refusal: Refusal },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct WaitingListAtRequest {
    pub after_revision: Option<WaitingRevision>,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum WaitingListAtReply {
    Snapshot {
        revision: WaitingRevision,
        requests: Vec<WaitingRequestView>,
    },
    Unchanged {
        revision: WaitingRevision,
    },
    Unavailable {},
    Refused {
        refusal: Refusal,
    },
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct WaitingRequestView {
    pub request_id: ApprovalRequestId,
    pub offer: OfferView,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Refusal {
    NotVisible {},
    RequesterCannotAnswer {},
    Ended { ending: EndingView },
    InvalidAnswer { reason: InvalidAnswer },
    TooLate {},
    SignInRequired {},
    SecondFactorRequired {},
    KeptChoiceNeedsAPerson {},
    OperationIdReused {},
}
}
