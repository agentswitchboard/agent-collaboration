use serde::{Deserialize, Serialize};
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{CancelReason, CentralTime, Ending, HolderReport, ParkedCallHolder, ProviderHolder};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RegisterRunRequest {
    pub router_run: RouterRunId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RegisterRunReply {
    RunRegistered {},
    RunRetired {},
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct RaiseReceipt {
    pub request_id: ApprovalRequestId,
    pub raised_at: CentralTime,
    pub deadline: CentralTime,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum RaiseReply {
    RaiseReceipt { receipt: RaiseReceipt },
    AlreadyStopped {},
    ContentsDiffer {},
    Refused { refusal: RunRefusal },
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
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
pub enum OfferEnding {
    Provider {
        request_id: ApprovalRequestId,
        holder: ProviderHolder,
        ending: Ending,
    },
    UnprovenCall {
        request_id: ApprovalRequestId,
        holder: ParkedCallHolder,
        ending: Ending,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub struct ReportDeliveryRequest {
    pub request_id: ApprovalRequestId,
    pub router_run: RouterRunId,
    pub report: HolderReport,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum ReportDeliveryReply {
    DeliveryRecorded {},
    ReportDiffers {},
    NoHandoverOwed {},
    Refused { refusal: HolderRefusal },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum HolderRefusal {
    NotVisible {},
    Run { refusal: RunRefusal },
}

impl StopRequest {
    #[must_use]
    pub const fn cause(&self) -> HolderStopCause {
        match self {
            Self::Provider { cause, .. } => match cause {
                ProviderStopCause::TurnStopped => HolderStopCause::TurnStopped,
                ProviderStopCause::SessionClosed => HolderStopCause::SessionClosed,
                ProviderStopCause::ProviderConnectionLost => {
                    HolderStopCause::ProviderConnectionLost
                }
            },
            Self::UnprovenCall {
                cause: CallStopCause::CallWithdrawn,
                ..
            } => HolderStopCause::CallWithdrawn,
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
    SessionClosed,
    ProviderConnectionLost,
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

/// Which ending stands after a Stop. The receipt carries no handover or answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(
    all(feature = "wire-export", not(target_arch = "wasm32")),
    derive(specta::Type)
)]
pub enum StopReceiptEnding {
    Cancelled { reason: CancelReason },
    TimedOut {},
    DecidedEarlier {},
}
