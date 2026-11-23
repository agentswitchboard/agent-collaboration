use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalWindow, CancelReason, Delivery, HolderStopCause, ProviderStopCause,
    RequestContentsDigest,
};

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RegisterRunRequest {
    pub router_run: RouterRunId,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RegisterRunReply {
    Registered {},
    RunRetired {},
    RunNotRegistered {},
    EnrollmentEnded {},
}
}

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RaiseReceipt {
    pub request_id: ApprovalRequestId,
    pub window: ApprovalWindow,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
/// Outcome of a Raise on the holder link, including the current authority check.
pub enum RaiseReply {
    Receipt { receipt: RaiseReceipt },
    AlreadyStopped {},
    SessionEnded {},
    ContentsDiffer {},
    TooManyOutstanding {},
    AuthorityRefused { refusal: RunRefusal },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum StopRequest {
    Provider {
        request_id: ApprovalRequestId,
        router_run: RouterRunId,
        contents_digest: RequestContentsDigest,
        cause: ProviderStopCause,
    },
    UnprovenCall {
        request_id: ApprovalRequestId,
        router_run: RouterRunId,
        contents_digest: RequestContentsDigest,
        cause: crate::CallStopCause,
    },
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum StopReply {
    /// Identical acknowledgement for a previously unseen ID and for an existing wait.
    Stands { ending: StopReceiptEnding },
    NotVisible {},
    AuthorityRefused { refusal: RunRefusal },
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum RunRefusal {
    RunNotRegistered {},
    RunRetired {},
    EnrollmentEnded {},
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum DeliverEndingReply {
    Received {},
    StopOwed {},
    NotHeld {},
    HolderNotReady {},
    EndingDiffers {},
}
}

impl DeliverEndingReply {
    /// Apply an exact reply from the current authenticated holder to its stored ending.
    #[must_use]
    pub const fn delivery_after_reply(self, current: Delivery) -> Delivery {
        match (self, current) {
            (
                Self::Received {},
                Delivery::Owed {}
                | Delivery::ReceivedByHolder {}
                | Delivery::ClosedWithoutReceipt {},
            )
            | (
                Self::StopOwed {}
                | Self::NotHeld {}
                | Self::HolderNotReady {}
                | Self::EndingDiffers {},
                Delivery::ReceivedByHolder {},
            ) => Delivery::ReceivedByHolder {},
            (Self::NotHeld {}, Delivery::Owed {} | Delivery::ClosedWithoutReceipt {})
            | (
                Self::StopOwed {} | Self::HolderNotReady {} | Self::EndingDiffers {},
                Delivery::ClosedWithoutReceipt {},
            ) => Delivery::ClosedWithoutReceipt {},
            (
                Self::StopOwed {} | Self::HolderNotReady {} | Self::EndingDiffers {},
                Delivery::Owed {},
            ) => Delivery::Owed {},
        }
    }
}

impl StopReply {
    /// Apply the standing Stop to an admitted ending's stored delivery.
    #[must_use]
    pub const fn delivery_after_stop(&self, current: Delivery) -> Delivery {
        match (self, current) {
            (Self::Stands { .. }, Delivery::Owed {})
            | (
                Self::Stands { .. } | Self::NotVisible {} | Self::AuthorityRefused { .. },
                Delivery::ClosedWithoutReceipt {},
            ) => Delivery::ClosedWithoutReceipt {},
            (
                Self::Stands { .. } | Self::NotVisible {} | Self::AuthorityRefused { .. },
                Delivery::ReceivedByHolder {},
            ) => Delivery::ReceivedByHolder {},
            (Self::NotVisible {} | Self::AuthorityRefused { .. }, Delivery::Owed {}) => {
                Delivery::Owed {}
            }
        }
    }
}

impl StopRequest {
    #[must_use]
    pub const fn cause(&self) -> HolderStopCause {
        match self {
            Self::Provider { cause, .. } => cause.holder_cause(),
            Self::UnprovenCall {
                cause: crate::CallStopCause::CallWithdrawn,
                ..
            } => HolderStopCause::CallWithdrawn,
        }
    }
}

approval_wire_union! {
/// Which ending stands after a Stop. The receipt carries no handover or answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum StopReceiptEnding {
    Cancelled { reason: CancelReason },
    TimedOut {},
    DecidedEarlier {},
}
}
