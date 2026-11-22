//! Observations the holder can honestly report for an ending.
use crate::approval_wire_codec::approval_wire_union;

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum DeliveryEvidence {
    Acp { evidence: AcpDeliveryEvidence },
    Native { evidence: NativeDeliveryEvidence },
    ParkedCall { evidence: ParkedCallEvidence },
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum AcpDeliveryEvidence {
    QueueRefused {},
    /// Report the flush observation available at the time of this report.
    QueueAccepted { flush: AcpFlushObservation },
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum AcpFlushObservation {
    FlushObserved {},
    FlushNotObserved {},
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum NativeDeliveryEvidence {
    RefusedBeforeSend {},
    SendReturned {},
    PossibleWrite {},
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallEvidence {
    RanWithResult { result: ParkedCallResult },
    NotDispatched {},
    UnknownAfterDispatch {},
}
}

approval_wire_union! {
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(all(feature = "wire-export", not(target_arch = "wasm32")), derive(specta::Type))]
pub enum ParkedCallResult {
    Succeeded {},
    Failed {},
}
}
