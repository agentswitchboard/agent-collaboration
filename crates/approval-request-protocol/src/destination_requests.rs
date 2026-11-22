use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use crate::{ApprovalPageItems, ApprovalPageSize, SessionApprovalContinuation};
use workspace_ids::{ApprovalRequestId, RequestId};

use crate::{
    Answer, ApprovalDecision, DecidedBy, EndingView, InvalidAnswer, InvalidApprovalEnding,
    OfferView, Raise, RequestView, WaitingRevision,
};

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
pub struct DecideRequest {
    pub request_id: ApprovalRequestId,
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
    pub operation_id: RequestId,
    pub value: Answer,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecideReply {
    SavedDecision { decision: ApprovalDecision },
    Standing { ending: EndingView },
    NotVisible {},
    Invalid {},
    TooLate {},
    OperationConflict {},
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
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum PassOnReply {
    Forwarded {},
    NotVisible {},
    TooLate {},
    Ended {},
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
    request_id: ApprovalRequestId,
    offer: OfferView,
}
}

impl WaitingRequestView {
    /// Central checks live destination membership before making the watch row.
    pub fn for_destination(
        raise: &Raise,
        destination: &DecidedBy,
    ) -> Result<Self, InvalidApprovalEnding> {
        crate::approval_request_views::check_reader(raise, destination)?;
        Ok(Self {
            request_id: raise.request_id().clone(),
            offer: raise.offer_view(),
        })
    }

    #[must_use]
    pub fn request_id(&self) -> &ApprovalRequestId {
        &self.request_id
    }

    #[must_use]
    pub fn offer(&self) -> &OfferView {
        &self.offer
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
