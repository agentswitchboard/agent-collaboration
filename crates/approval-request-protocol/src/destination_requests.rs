use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, RequestId};

use crate::{Answer, EndingView, InvalidAnswer, OfferView, RequestView, WaitingRevision};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ListPersonRequests {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ListPersonRequestsReply {
    Listed { requests: Vec<RequestView> },
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ReadPersonRequest {
    pub request_id: ApprovalRequestId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ReadPersonRequestReply {
    Read { request: RequestView },
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecidePersonReply {
    Decided { ending: EndingView },
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ListSessionRequests {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ListSessionRequestsReply {
    Listed { requests: Vec<RequestView> },
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ReadSessionRequest {
    pub request_id: ApprovalRequestId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ReadSessionRequestReply {
    Read { request: RequestView },
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecideSessionReply {
    Decided { ending: EndingView },
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct PassOnRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum PassOnReply {
    PassedOn {},
    Refused { refusal: Refusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct WaitingListAtRequest {
    pub after_revision: Option<WaitingRevision>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct WaitingRequestView {
    pub request_id: ApprovalRequestId,
    pub offer: OfferView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
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
