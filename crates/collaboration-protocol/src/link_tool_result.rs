//! The one result every Central-backed tool returns, shared by the MCP server and the CLI.
//!
//! `result` is the closed discriminant an agent branches on; `text` is the wording the
//! collaboration application writes for it. Central's own reply is carried unchanged.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::central_tool_port::{
    CentralMailResult, ReplyPayload, RequestId, Timestamp, WorkspaceReply, WorkspaceResult,
};
pub use crate::message_tools::{MessageCallerRefusal, MessageFrontRefusal};

/// The wire shape of a Central-backed tool's answer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LinkToolOutput {
    pub result: LinkToolResult,
    pub text: String,
}

/// What happened to the tool's one request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "state",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum LinkToolResult {
    /// Central's own answer, including its domain refusals, unchanged.
    Answered { reply: ReplyPayload },
    /// The write is kept in this Mac's outbox and leaves at `flush_at`.
    InOutbox {
        request_id: RequestId,
        flush_at: Timestamp,
    },
    /// An immediate write left this Mac and its answer was lost; it may have applied.
    Uncertain { request_id: RequestId },
    /// A read left this Mac and its answer was lost; reading again is safe.
    ReadAnswerLost,
    /// Nothing reached Central, or the link refused the request before it was sent.
    Refused { code: LinkRefusal },
}

impl LinkToolResult {
    /// Only Central's done answer and an outbox receipt are successes; every other answer is
    /// a tool error that still carries this typed result.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        match self {
            Self::Answered { reply } => match reply {
                ReplyPayload::Workspace(WorkspaceReply { result, .. }) => match result {
                    WorkspaceResult::Done(_) => true,
                    WorkspaceResult::Refused(_)
                    | WorkspaceResult::NotFound { .. }
                    | WorkspaceResult::Conflict
                    | WorkspaceResult::OutcomeUnknown
                    | WorkspaceResult::CentralUnavailable => false,
                },
                ReplyPayload::Mail(result) => match result {
                    CentralMailResult::Done(_) => true,
                    CentralMailResult::Refused(_)
                    | CentralMailResult::Conflict
                    | CentralMailResult::OutcomeUnknown
                    | CentralMailResult::CentralUnavailable => false,
                },
            },
            Self::InOutbox { .. } => true,
            Self::Uncertain { .. } | Self::ReadAnswerLost | Self::Refused { .. } => false,
        }
    }
}

/// Why the Router refused before Central decided anything.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum LinkRefusal {
    /// Central could not be reached and nothing was sent.
    CentralUnavailable,
    /// The outbox could not store the write, so nothing was kept.
    OutboxUnavailable,
    /// The request ID names a different stored write.
    RequestIdConflict,
    /// The link would not frame the request.
    InvalidRequest { reason: InvalidRequestReason },
    /// The input names a field this tool no longer accepts.
    RemovedInput { field: RemovedField },
    /// The input does not match the tool's schema.
    InvalidArguments,
    /// The calling session could not be admitted on this Mac.
    Caller { reason: MessageCallerRefusal },
    /// The Router's link to Central is not running for this call.
    RouterNotReady { state: MessageFrontRefusal },
}

/// The link's reason for not framing a request, as a closed public code.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum InvalidRequestReason {
    MalformedFrame,
    RequestIdPolicy { field: String },
    InvalidBatch,
    InvalidReceiptBatch,
    InvalidOffer,
    WrongDirection,
    TooLargeToSend,
    UnknownTypedCall,
    UnmeasurableWrite,
    UnstampedMessageRequired,
    InvalidSendTime,
}

/// Every input a Central-backed tool used to accept and now refuses by name.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RemovedField {
    /// `includeRedacted`: redaction deletes the text, so there is nothing to include.
    IncludeRedacted,
    /// `projectId` or `project` on a thread: a thread ID alone names the thread.
    ProjectQualifier,
    /// `protocolVersion`: the link versions itself.
    ProtocolVersion,
}

/// A tool's arguments, with the old input names it refuses as removed rather than unknown.
pub trait LinkToolArguments: DeserializeOwned + JsonSchema {
    const REMOVED_INPUTS: &'static [(&'static str, RemovedField)];
}
