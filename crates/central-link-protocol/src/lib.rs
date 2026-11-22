//! The closed Router–central wire vocabulary, without a runtime or storage driver.
mod link_close;
mod link_codec;
mod link_delivery;
mod link_renewal;
mod link_request;
mod link_typed_calls;
mod request_status;
mod typed_call_correlations;

#[cfg(any(test, feature = "test-support"))]
pub use link_typed_calls::test_calls;
pub use link_typed_calls::{
    CentralCall, CentralCallReply, CentralLinkRpc, LinkCallRefusal, MacCall, MacCallReply,
    MacLinkRpc, TypedCallResult,
};
pub use request_status::{RequestRefusal, RequestStatusAnswer, RequestStatusQuery};
pub use typed_call_correlations::{MAX_PENDING_TYPED_CALLS, TypedCallCorrelations};

pub use link_close::MachineLinkCloseCode;
pub use link_codec::{DataFrame, FrameDirection, LinkFrame, decode_text, encode_text};
pub use link_delivery::{
    DeliveryMessage, DirectOfferBody, DirectOfferMessage, MAX_RECEIPTS, MAX_TIME_LEFT_MS, Receipt,
    ReceiptBatch, ReceiptKind, ReceiptResult, TimeLeft,
};
pub use link_renewal::{
    InvalidMachineToken, MachineTokenText, TokenRenewalRefusal, TokenRenewalResult,
};
pub use link_request::{
    CorrelationId, LinkRequest, LinkRequestBatch, MAX_LINK_REQUEST_BATCH_SIZE, ReplyPayload,
    RequestPayload, RequestScope,
};

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
    #[error("unregistered typed link call")]
    UnknownTypedCall { correlation_id: CorrelationId },
}
