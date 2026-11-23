use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use serde::{Deserialize, Serialize};
use workspace_ids::{CentralSessionId, PersonId};

use crate::{
    ApprovalDigestError, CallStopCause, InvalidAnswer, ProviderAnswer, SessionCallDecision,
};

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum DecidedBy {
    Person {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Session { session: CentralSessionId },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Ending {
    Decided { answer: ProviderAnswer, by: DecidedBy },
    /// An observed parked decision. The holder frame authoring API requires checked admission.
    UnprovenCallDecided {
        decision: SessionCallDecision,
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = String))]
        person_id: PersonId,
    },
    Cancelled { reason: CancelReason },
    TimedOut {},
}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum HolderStopCause {
    TurnStopped,
    AskReplaced,
    AskWithdrawn,
    SessionClosed,
    ProviderConnectionLost,
    CallWithdrawn,
    HolderRefusedLocally,
}

impl HolderStopCause {
    /// Returns the cause recorded in a holder ending for a parked call.
    #[must_use]
    pub const fn from_call(cause: CallStopCause) -> Self {
        match cause {
            CallStopCause::CallWithdrawn => Self::CallWithdrawn,
            CallStopCause::HolderRefusedLocally => Self::HolderRefusedLocally,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ProviderStopCause {
    TurnStopped,
    AskReplaced,
    /// The provider agent withdrew this waiting ask.
    AskWithdrawn,
    SessionClosed,
    ProviderConnectionLost,
    /// The holder has already given the requester a final local refusal.
    HolderRefusedLocally,
}

impl ProviderStopCause {
    /// Returns the cause recorded in a holder ending.
    #[must_use]
    pub const fn holder_cause(self) -> HolderStopCause {
        match self {
            Self::TurnStopped => HolderStopCause::TurnStopped,
            Self::AskReplaced => HolderStopCause::AskReplaced,
            Self::AskWithdrawn => HolderStopCause::AskWithdrawn,
            Self::SessionClosed => HolderStopCause::SessionClosed,
            Self::ProviderConnectionLost => HolderStopCause::ProviderConnectionLost,
            Self::HolderRefusedLocally => HolderStopCause::HolderRefusedLocally,
        }
    }
}

impl TryFrom<HolderStopCause> for ProviderStopCause {
    type Error = InvalidApprovalEnding;

    fn try_from(cause: HolderStopCause) -> Result<Self, Self::Error> {
        Ok(match cause {
            HolderStopCause::TurnStopped => Self::TurnStopped,
            HolderStopCause::AskReplaced => Self::AskReplaced,
            HolderStopCause::AskWithdrawn => Self::AskWithdrawn,
            HolderStopCause::SessionClosed => Self::SessionClosed,
            HolderStopCause::ProviderConnectionLost => Self::ProviderConnectionLost,
            HolderStopCause::HolderRefusedLocally => Self::HolderRefusedLocally,
            HolderStopCause::CallWithdrawn => return Err(InvalidApprovalEnding::BindingMismatch),
        })
    }
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum Delivery {
    Owed {},
    ReceivedByHolder {},
    ClosedWithoutReceipt {},
}
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

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RequestState {
    Waiting { status: WaitingStatus },
    Ended { ending: Ending },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum SessionDecider {
    Session { session: CentralSessionId },
}
}

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Debug, Eq, PartialEq)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    pub struct ApprovalDecision {
        pub answer: ProviderAnswer,
        pub by: SessionDecider,
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidApprovalEnding {
    #[error(transparent)]
    Digest(#[from] ApprovalDigestError),
    #[error("a requester is not a destination of its own request")]
    RequesterIsReader,
    #[error("the approval ending belongs to a different request or holder")]
    BindingMismatch,
    #[error("the answer does not fit the approval offer: {0:?}")]
    Answer(InvalidAnswer),
    #[error("this actor cannot give the selected approval decision")]
    IneligibleDecider,
    #[error("the decision requires a fresh person factor")]
    FreshFactorRequired,
    #[error("the destination is not a current member of this request")]
    DestinationNotAdmitted,
    #[error("the retained decision does not match the admitted decision operation")]
    RetainedDecisionMismatch,
    #[error("the ending time does not respect the request's approval window")]
    Chronology,
}
