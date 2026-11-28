//! Public contracts for workspace tools that read from Central.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
pub use workspace_ids::SessionId;
use workspace_model::{RefusalReason, WorkspaceResult};
pub use workspace_model::{ThreadId, WorkspaceId, WorkspaceReply};

use crate::message_tools::{MessageSubmitRefusal, MessageToolRefusal};

pub const THREAD_SHOW_DESCRIPTION: &str = "Read one thread from Central by workspaceId and threadId. Report your current callerSessionId. This reads the thread itself.";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadShowArguments {
    pub caller_session_id: SessionId,
    pub workspace_id: WorkspaceId,
    pub thread_id: ThreadId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadShowOutput {
    pub result: ThreadShowResult,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "state", rename_all = "camelCase", deny_unknown_fields)]
pub enum ThreadShowResult {
    Answered { reply: Box<WorkspaceReply> },
    ReadAnswerLost,
    Refused { code: ThreadShowRefusal },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ThreadShowRefusal {
    CentralUnavailable,
    RemovedInput { field: RemovedThreadShowField },
    InvalidArguments,
    Router { reason: MessageToolRefusal },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum RemovedThreadShowField {
    IncludeRedacted,
    ProjectQualifier,
    ProtocolVersion,
}

impl ThreadShowOutput {
    #[must_use]
    pub fn answered(reply: WorkspaceReply) -> Self {
        let text = match &reply.result {
            WorkspaceResult::Done(_) => "Thread found.".into(),
            WorkspaceResult::NotFound { .. } => {
                "Thread not found. Check workspaceId and threadId before reading again.".into()
            }
            WorkspaceResult::Refused(reason) => format!(
                "Refused by Central. {reason}. {}",
                match reason {
                    RefusalReason::MachineRevoked =>
                        "Check this Mac's enrollment before reading again.",
                    RefusalReason::WorkspaceNotAllowed =>
                        "Check that this Mac is allowed to read the workspace.",
                    RefusalReason::NotMember { .. } | RefusalReason::NotParticipant { .. } =>
                        "Use a session with access to the thread.",
                    _ => "Check the workspace and thread, then read again.",
                }
            ),
            WorkspaceResult::CentralUnavailable => {
                "Central answered that it is unavailable. Reading again is safe.".into()
            }
            WorkspaceResult::Conflict | WorkspaceResult::OutcomeUnknown => {
                "Central could not answer the read. Reading again is safe.".into()
            }
        };
        Self {
            result: ThreadShowResult::Answered {
                reply: Box::new(reply),
            },
            text,
        }
    }

    #[must_use]
    pub fn refused(code: ThreadShowRefusal) -> Self {
        let text = match &code {
            ThreadShowRefusal::CentralUnavailable => "Central unavailable. Nothing was sent. Check the link or read again.".into(),
            ThreadShowRefusal::RemovedInput { field } => match field {
                RemovedThreadShowField::IncludeRedacted => "includeRedacted is no longer accepted. Redaction deletes the text, so there is nothing to include. Omit this field.",
                RemovedThreadShowField::ProjectQualifier => "The project qualifier (projectId or project) is no longer accepted. Identify the thread with workspaceId and threadId.",
                RemovedThreadShowField::ProtocolVersion => "protocolVersion is no longer accepted. The link selects its version. Omit this field.",
            }.into(),
            ThreadShowRefusal::InvalidArguments => "Invalid thread_show arguments. Provide callerSessionId, workspaceId, and threadId as listed in the tool schema.".into(),
            ThreadShowRefusal::Router { reason } => format!("Refused by the Router. {reason} Check the current session and link, then read again."),
        };
        Self {
            result: ThreadShowResult::Refused { code },
            text,
        }
    }
}

impl From<Result<WorkspaceReply, MessageToolRefusal>> for ThreadShowOutput {
    fn from(result: Result<WorkspaceReply, MessageToolRefusal>) -> Self {
        match result {
            Ok(reply) => Self::answered(reply),
            Err(MessageToolRefusal::ReadAnswerLost) => Self {
                result: ThreadShowResult::ReadAnswerLost,
                text: "Read failed because its answer was lost. Reading again is safe.".into(),
            },
            Err(
                MessageToolRefusal::NoMessageLink
                | MessageToolRefusal::Submission(MessageSubmitRefusal::CentralUnavailable),
            ) => Self::refused(ThreadShowRefusal::CentralUnavailable),
            Err(reason) => Self::refused(ThreadShowRefusal::Router { reason }),
        }
    }
}
