use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use serde::{Deserialize, Serialize};
use workspace_ids::{PersonId, SessionRef};

use crate::{Answer, ApprovalDigestError, DeliveryEvidence, InvalidAnswer};

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
    Session {
        #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), specta(type = crate::typescript_shapes::SessionRef))]
        session: SessionRef => "crate::approval_wire_codec::decode_named_map",
    },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Ending {
    Decided { answer: Answer, by: DecidedBy },
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
    SessionClosed,
    ProviderConnectionLost,
    CallWithdrawn,
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
    SessionClosed,
    ProviderConnectionLost,
}

impl ProviderStopCause {
    pub(crate) const fn holder_cause(self) -> HolderStopCause {
        match self {
            Self::TurnStopped => HolderStopCause::TurnStopped,
            Self::AskReplaced => HolderStopCause::AskReplaced,
            Self::SessionClosed => HolderStopCause::SessionClosed,
            Self::ProviderConnectionLost => HolderStopCause::ProviderConnectionLost,
        }
    }
}

impl TryFrom<HolderStopCause> for ProviderStopCause {
    type Error = InvalidApprovalEnding;

    fn try_from(cause: HolderStopCause) -> Result<Self, Self::Error> {
        match cause {
            HolderStopCause::TurnStopped => Ok(Self::TurnStopped),
            HolderStopCause::AskReplaced => Ok(Self::AskReplaced),
            HolderStopCause::SessionClosed => Ok(Self::SessionClosed),
            HolderStopCause::ProviderConnectionLost => Ok(Self::ProviderConnectionLost),
            HolderStopCause::CallWithdrawn => Err(InvalidApprovalEnding::Cancellation),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum CallStopCause {
    CallWithdrawn,
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
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum Delivery {
    NoHandoverOwed {},
    AwaitingReport {},
    Reported {
        evidence: DeliveryEvidence,
    },
    /// Central retired a run with delivery still owed.
    ReceiptUnknown {},
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

approval_wire_record! {
    [deny_unknown_fields]
    #[derive(Clone, Debug, Eq, PartialEq)]
    #[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
    pub struct ApprovalDecision {
        pub answer: Answer,
        pub by: DecidedBy,
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
    #[error("the cancellation does not fit this kind of approval request")]
    Cancellation,
}
