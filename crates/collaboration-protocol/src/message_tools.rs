//! Public message tool inputs and results shared by the CLI, MCP, and application.
use std::{collections::BTreeSet, future::Future, pin::Pin};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use workspace_ids::{
    AdmittedLocalSession, MachineId, MessageId, RequestId, SessionId, SessionRef,
};
pub use workspace_model::{
    PostText, Timestamp,
    central_mail::{MessageRecord, MessageStatus},
};
use workspace_model::{RefusalReason, central_mail::CentralMailRefusal};

mod message_tool_text;
pub use message_tool_text::{
    ADDRESS_FORMAT, MESSAGE_CLI_HELP, MESSAGE_INBOX_DESCRIPTION, MESSAGE_SEND_DESCRIPTION,
    MESSAGE_STATUS_DESCRIPTION, message_inbox_text, message_send_text, message_status_text,
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

/// The private MCP address already supplies the sender.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedMessageSendArguments {
    pub to: SessionRef,
    pub body: PostText,
    pub request_id: Option<RequestId>,
}

/// The private MCP address supplies the owner of the request ID.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedMessageStatusArguments {
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

    fn read_inbox<'call>(
        &'call self,
        caller: &'call AdmittedLocalSession,
        read: MessageInboxRead,
    ) -> MessageOperationFuture<'call, MessageInboxPage>;
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
    InvalidInboxLimit,
    InvalidInboxReadToken,
    InboxAcknowledgmentNotKept,
    InboxChanged,
    InboxUnavailable,
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

pub const DEFAULT_INBOX_LIMIT: u16 = 20;
pub const MAX_INBOX_LIMIT: u16 = 100;
const MAX_TOKEN_LENGTH: usize = ((MAX_INBOX_LIMIT as usize * 39 + 2) * 4).div_ceil(3);

/// A CLI or MCP caller asks for their inbox and can acknowledge a previously returned page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageInboxArguments {
    pub caller_session_id: Option<SessionId>,
    #[schemars(range(min = 1, max = 100))]
    pub limit: Option<u16>,
    pub acknowledge: Option<String>,
}

/// The private MCP address supplies the recipient, including for acknowledgments.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedMessageInboxArguments {
    #[schemars(range(min = 1, max = 100))]
    pub limit: Option<u16>,
    pub acknowledge: Option<String>,
}

/// The message unit returns the current page and any acknowledgment recorded for this call.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageInboxPage {
    pub messages: Vec<MessageRecord>,
    pub read_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledgment: Option<MessageInboxAcknowledgment>,
}

/// The message unit counts the acknowledged messages that changed or retained their state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessageInboxAcknowledgment {
    pub changed: usize,
    pub unchanged: usize,
}

/// The application decodes the ordered IDs to be validated against the admitted recipient's inbox.
pub struct MessageInboxRead {
    pub limit: u16,
    pub acknowledge: Option<Vec<MessageId>>,
}

impl MessageInboxRead {
    pub fn from_arguments(
        limit: Option<u16>,
        acknowledge: Option<&str>,
    ) -> Result<Self, MessageToolRefusal> {
        let limit = limit.unwrap_or(DEFAULT_INBOX_LIMIT);
        if !(1..=MAX_INBOX_LIMIT).contains(&limit) {
            return Err(MessageToolRefusal::InvalidInboxLimit);
        }
        Ok(Self {
            limit,
            acknowledge: acknowledge.map(decode_read_token).transpose()?,
        })
    }
}

pub fn inbox_read_token(messages: &[MessageRecord]) -> Result<String, MessageToolRefusal> {
    let ids: Vec<_> = messages.iter().map(|message| &message.message_id).collect();
    encode_read_ids(&ids)
}

fn encode_read_ids(ids: &[&MessageId]) -> Result<String, MessageToolRefusal> {
    let encoded = serde_json::to_vec(ids).map_err(|_| MessageToolRefusal::UnexpectedReply)?;
    Ok(URL_SAFE_NO_PAD.encode(encoded))
}

fn decode_read_token(token: &str) -> Result<Vec<MessageId>, MessageToolRefusal> {
    let refusal = MessageToolRefusal::InvalidInboxReadToken;
    if token.len() > MAX_TOKEN_LENGTH {
        return Err(refusal);
    }
    let encoded = URL_SAFE_NO_PAD.decode(token).map_err(|_| refusal.clone())?;
    let ids: Vec<MessageId> = serde_json::from_slice(&encoded).map_err(|_| refusal.clone())?;
    if ids.len() > usize::from(MAX_INBOX_LIMIT)
        || ids.iter().collect::<BTreeSet<_>>().len() != ids.len()
        || encode_read_ids(&ids.iter().collect::<Vec<_>>())? != token
    {
        return Err(refusal);
    }
    Ok(ids)
}

#[cfg(test)]
#[path = "message_tools_test.rs"]
mod tests;
