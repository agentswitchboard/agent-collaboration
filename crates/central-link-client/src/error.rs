#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ClientError {
    #[error("a receipt frame is already pending on this connection")]
    ReceiptFramePending,
    #[error("receipt results must match the pending frame correlation and length")]
    InvalidReceiptResults,
    #[error("central link is unavailable")]
    CentralUnavailable,
    /// The request table was full before a frame could be produced.
    #[error("the pending request limit was reached before the request could be sent")]
    NeverReached,
    #[error("event belongs to an obsolete connection attempt")]
    StaleConnection,
    #[error("request does not follow the write request-id policy")]
    InvalidRequest,
    #[error("Welcome is unexpected or names another machine")]
    InvalidWelcome,
    #[error("unexpected frame for the Router client")]
    UnexpectedFrame,
    #[error("correlation id is already in flight")]
    DuplicateCorrelation,
    #[error("reply names no in-flight request")]
    UnknownCorrelation,
    #[error("reply payload belongs to a different operation family")]
    WrongReplyFamily,
}
