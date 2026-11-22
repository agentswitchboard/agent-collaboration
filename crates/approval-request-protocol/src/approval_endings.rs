use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, PersonId, SessionRef};

use crate::{Answer, CentralTime, OfferView};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecidedBy {
    Person {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Session {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = crate::typescript_shapes::SessionRef))]
        session: SessionRef,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Ending {
    Decided { answer: Answer, by: DecidedBy },
    Cancelled { reason: CancelReason },
    TimedOut {},
}

impl Ending {
    /// Project for a destination that central has already authorized to read this request.
    #[must_use]
    pub fn for_destination(&self, destination: &DecidedBy) -> EndingView {
        match self {
            Self::Decided { answer, by } if by == destination => EndingView::DecidedWithAnswer {
                answer: answer.clone(),
                by: by.clone(),
            },
            Self::Decided { by, .. } => EndingView::Decided { by: by.clone() },
            Self::Cancelled { reason } => EndingView::Cancelled { reason: *reason },
            Self::TimedOut {} => EndingView::TimedOut {},
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum EndingView {
    Decided { by: DecidedBy },
    DecidedWithAnswer { answer: Answer, by: DecidedBy },
    Cancelled { reason: CancelReason },
    TimedOut {},
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum HolderStopCause {
    TurnStopped,
    SessionClosed,
    ProviderConnectionLost,
    CallWithdrawn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum CancelReason {
    Stopped { cause: HolderStopCause },
    RequesterGone {},
    SessionEnded {},
    EnrollmentEnded {},
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum HolderReport {
    GoneBeforeHandover,
    ReplyWrittenEffectNotConfirmed,
    /// The holder lost custody after a possible write.
    DeliveryUnknown,
    /// The holder has evidence that the answer took effect.
    Applied,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Delivery {
    NoHandoverOwed {},
    AwaitingReport {},
    Reported {
        report: HolderReport,
    },
    /// Central retired a run with delivery still owed.
    ReceiptUnknown {},
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum WaitingStatus {
    Pending,
    PassedOn,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RequestState {
    Waiting { status: WaitingStatus },
    Ended { ending: Ending },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RequestStateView {
    Waiting {
        status: WaitingStatus,
    },
    Ended {
        ending: EndingView,
        ended_at: CentralTime,
        delivery: Delivery,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RequestView {
    pub request_id: ApprovalRequestId,
    pub offer: OfferView,
    pub state: RequestStateView,
    pub raised_at: CentralTime,
    pub deadline: CentralTime,
}
