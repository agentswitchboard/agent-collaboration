use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalWindow, CallStopCause, CancelReason, HolderReport, HolderStopCause, ProviderStopCause,
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
    RunRegistered {},
    RunRetired {},
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
/// Outcome of a Raise after the holder run is admitted.
/// The admission layer returns a typed RunRefusal before a RaiseReply exists.
pub enum RaiseReply {
    RaiseReceipt { receipt: RaiseReceipt },
    AlreadyStopped {},
    AlreadyEnded {},
    ContentsDiffer {},
    TooManyOutstanding {},
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
        cause: ProviderStopCause,
    },
    UnprovenCall {
        request_id: ApprovalRequestId,
        router_run: RouterRunId,
        cause: CallStopCause,
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
    StopReceipt {
        ending: StopReceiptEnding,
    },
    Refused {
        refusal: HolderRefusal,
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
pub struct ReportDeliveryRequest {
    pub request_id: ApprovalRequestId,
    pub router_run: RouterRunId,
    pub report: HolderReport,
}
}

approval_wire_union! {
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ReportDeliveryReply {
    DeliveryRecorded {},
    ReportDiffers {},
    /// Final after erasure, even for an equal earlier report. Central retains no report to compare.
    NoHandoverOwed {},
    Refused {
        refusal: HolderRefusal,
    },
}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RunRefusal {
    RunNotRegistered,
    RunRetired,
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum HolderRefusal {
    NotVisible {},
    Run { refusal: RunRefusal },
}
}

impl StopRequest {
    #[must_use]
    pub const fn cause(&self) -> HolderStopCause {
        match self {
            Self::Provider { cause, .. } => cause.holder_cause(),
            Self::UnprovenCall {
                cause: CallStopCause::CallWithdrawn,
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
