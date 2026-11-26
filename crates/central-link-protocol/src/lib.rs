//! The closed Router–central wire vocabulary, without a runtime or storage driver.
pub mod approval_rpc;
mod central_egress;
mod correlation_id;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture_central_probe;
mod link_close;
mod link_codec;
mod link_delivery;
mod link_renewal;
mod link_request;
mod link_size;
mod link_typed_calls;
mod mac_call_address;
mod mac_call_capabilities;
mod mac_work_call;
#[cfg(test)]
mod parent_session_rpc_test;
pub mod person_link_rpc;
mod request_status;
pub mod session_lifecycle_rpc;
mod socket_direct_mail;
mod socket_mac_call_capabilities;
mod typed_call_correlations;

#[cfg(any(test, feature = "test-support"))]
pub use link_typed_calls::test_calls;
pub use link_typed_calls::{
    CentralCall, CentralCallReply, CentralLinkRpc, LinkCallRefusal, MacCall, MacCallReply,
    MacLinkRpc, TypedCallResult,
};
pub use request_status::{RequestRefusal, RequestStatusAnswer, RequestStatusQuery};
pub use typed_call_correlations::{MAX_PENDING_TYPED_CALLS, TypedCallCorrelations};

pub use central_egress::{CentralEgress, check_central_egress_length, encode_central_egress};
pub use correlation_id::{CorrelationId, FreshCorrelationId};
pub use link_close::{MachineLinkCloseCode, PersonLinkCloseCode};
pub use link_codec::{DataFrame, LinkFrame, decode_text, encode_text};
pub use link_delivery::{
    DeliveryMessage, DirectOfferBody, DirectOfferMessage, MAX_RECEIPTS, MAX_TIME_LEFT_MS, Receipt,
    ReceiptBatch, ReceiptKind, ReceiptResult, TimeLeft,
};
pub use link_renewal::{
    InvalidMachineToken, MachineTokenText, TokenRenewalRefusal, TokenRenewalResult,
};
pub use link_request::{
    LinkRequest, LinkRequestBatch, MAX_LINK_REQUEST_BATCH_SIZE, ReplyPayload, RequestPayload,
    RequestScope,
};
pub use link_size::{LINK_MAX_EGRESS_TEXT_BYTES, LINK_MAX_TEXT_BYTES, LinkSizeRefusal};
pub use mac_call_address::MacCallAddress;
pub use mac_call_capabilities::{
    DuplicateMacCallKind, HandledMacCalls, MacCallKind, MacCapabilities,
};
pub use mac_work_call::{
    MacReplyExpectation, MacReplyMismatch, MacWorkCall, MacWorkReply, MacWorkRpc, NonMacWorkCall,
};
pub use socket_direct_mail::{
    LiveOfferOutcome, NotOffered, SOCKET_DIRECT_MAIL_LIMIT, SocketDirectMailOffers,
};
pub use socket_mac_call_capabilities::{
    DuplicateMacCapabilities, MAX_SOCKET_MAC_CALL_ITEMS, MacCallDeliveryKey, MacCallSkip,
    SocketMacCallState,
};

/// The receiving end of a machine-link frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameDirection {
    ToCentral,
    ToRouter,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("invalid link JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{field} does not match the operation's request-id policy")]
    RequestIdPolicy { field: &'static str },
    #[error("batch must contain 1 to 64 writes with distinct correlation IDs")]
    InvalidBatch,
    #[error("receipts must contain 1 to 64 items")]
    InvalidReceiptBatch,
    #[error("offer does not carry the time left required by its kind")]
    InvalidOffer,
    #[error("frame is not allowed in this direction")]
    WrongDirection,
    #[error("encoded Central link frame exceeds the egress budget ({bytes} bytes)")]
    EgressTooBig { bytes: usize },
    #[error("unregistered typed link call")]
    UnknownTypedCall { correlation_id: CorrelationId },
}
