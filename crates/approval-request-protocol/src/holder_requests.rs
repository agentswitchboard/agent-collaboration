use crate::approval_wire_codec::approval_wire_record;
use crate::approval_wire_codec::approval_wire_union;
use workspace_ids::{ApprovalRequestId, RouterRunId};

use crate::{
    ApprovalWindow, CallStopCause, CancelReason, DeliveryEvidence, EndingDigest, HolderStopCause,
    ProviderStopCause, RequestContentsDigest,
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
    AlreadyEnded {},
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
    Stands { ending: StopReceiptEnding },
    NotVisible {},
    AuthorityRefused { refusal: RunRefusal },
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
    pub contents_digest: RequestContentsDigest,
    pub ending_digest: EndingDigest,
    pub evidence: DeliveryEvidence,
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

approval_wire_record! {
[deny_unknown_fields]
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub struct AuthorityProbeRequest {
    pub router_run: RouterRunId,
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum AuthorityProbeReply {
    Admitted {},
    AuthorityRefused { refusal: RunRefusal },
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
