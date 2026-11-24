//! Public send/status inputs and normalized results shared by CLI, MCP and their application.
use std::{future::Future, pin::Pin};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use workspace_ids::{
    AdmittedLocalSession, MachineId, MessageId, RequestId, SessionId, SessionRef,
};
pub use workspace_model::{PostText, Timestamp, central_mail::MessageStatus};
use workspace_model::{RefusalReason, central_mail::CentralMailRefusal};

mod message_tool_text;
pub use message_tool_text::{
    ADDRESS_FORMAT, MESSAGE_CLI_HELP, MESSAGE_SEND_DESCRIPTION, MESSAGE_STATUS_DESCRIPTION,
    message_send_text, message_status_text,
};

/// A v0 caller report beside the semantic input, supplied by the MCP client or CLI.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageSendArguments {
    pub caller_session_id: Option<SessionId>,
    pub to: SessionRef,
    pub body: PostText,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<RequestId>,
}

/// A caller asks the Router for its original direct write by request ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageStatusArguments {
    pub caller_session_id: Option<SessionId>,
    pub request_id: RequestId,
}

/// The application returns one typed write outcome or one typed refusal to either transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "MessageToolResult_for_{Outcome}")]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum MessageToolResult<Outcome> {
    Returned(Outcome),
    Refused(MessageToolRefusal),
}

impl<Outcome> From<Result<Outcome, MessageToolRefusal>> for MessageToolResult<Outcome> {
    fn from(result: Result<Outcome, MessageToolRefusal>) -> Self {
        match result {
            Ok(outcome) => Self::Returned(outcome),
            Err(refusal) => Self::Refused(refusal),
        }
    }
}

/// A send outcome projected by the message unit after the Router attempts local admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MessageSendOutcome {
    InOutbox {
        request_id: RequestId,
        flush_at: Timestamp,
    },
    Answered {
        request_id: RequestId,
        message_id: MessageId,
    },
    Uncertain {
        request_id: RequestId,
    },
}

/// The message unit's sender-scoped observation of one original write.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MessageWriteStatus {
    InOutbox {
        flush_at: Timestamp,
    },
    InFlight,
    Answered {
        message_id: MessageId,
        delivery: MessageDeliveryObservation,
    },
    Refused {
        reason: MessageWriteRefusal,
    },
    NotSent,
    NotConfirmed,
    NotFound,
    StatusUnavailable,
}

/// The separate sender-scoped delivery observation the message unit obtains from Central.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum MessageDeliveryObservation {
    Checked(MessageStatus),
    Unknown,
}

/// The application prepares this exact unstamped write for the message unit.
pub struct DirectMessageWrite {
    pub request_id: RequestId,
    pub to: SessionRef,
    pub body: PostText,
}

/// The message unit resolves a projected application operation through its front.
pub type MessageOperationFuture<'call, Outcome> =
    Pin<Box<dyn Future<Output = Result<Outcome, MessageToolRefusal>> + Send + 'call>>;

/// The message unit supplies these operations without exposing its queue and link types.
pub trait MessageUnitPort: Send + Sync {
    fn check_available(&self) -> Result<(), MessageToolRefusal> {
        Ok(())
    }

    fn submit<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        write: DirectMessageWrite,
    ) -> MessageOperationFuture<'call, MessageSendOutcome>;

    fn request_status<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        request_id: RequestId,
    ) -> MessageOperationFuture<'call, MessageWriteStatus>;
}

/// A retained refusal reported by the outbox or the Central request log.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum MessageWriteRefusal {
    RequestIdConflict,
    CentralMail(CentralMailRefusal),
    CentralWorkspace(RefusalReason),
}

/// The message unit maps the executor's refusal at the generation admission gate into this vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MessageFrontRefusal {
    AwaitingFirstRead,
    NotEnrolled,
    EnrollmentEnded,
    MetadataUnreadable,
    Starting,
    Restarting,
    EnrollmentMismatch,
    ShuttingDown,
    CouldNotStart,
}

/// The message unit projects a definite custody submission refusal into this vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MessageSubmitRefusal {
    CentralUnavailable,
    OutboxUnavailable,
    RequestIdConflict,
    UnstampedMessageRequired,
    TooLargeToSend,
    InvalidSendTime,
    UnmeasurableWrite,
    InvalidRequest,
}

/// The local admission owner reports why a message call could not be attributed to a session.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum MessageCallerRefusal {
    CallerRequired,
    UnknownSession,
    AmbiguousSession,
    CouldNotTell,
}

/// A refusal produced at the Router's message door, application, or message unit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    content = "reason",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum MessageToolRefusal {
    InvalidAddress,
    InvalidBody,
    InvalidRequestId,
    InvalidCaller,
    InvalidArguments,
    Caller(MessageCallerRefusal),
    NoMessageLink,
    Front(MessageFrontRefusal),
    Submission(MessageSubmitRefusal),
    Recorded(MessageWriteRefusal),
    RequestIdUnavailable,
    UnexpectedReply,
    ReadAnswerLost,
    TransportUnavailable,
}

#[cfg(test)]
#[path = "message_tools_test.rs"]
mod tests;
