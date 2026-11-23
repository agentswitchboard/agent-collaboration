//! Typed projections of the caller's own stored writes.
use crate::OutboxScope;
use central_link_protocol::ReplyPayload;
use serde::{Deserialize, Serialize};
use workspace_ids::{RequestId, SessionRef};
use workspace_model::{
    Caller, PostText, ThreadId, WorkspaceId, WorkspaceOperation, central_mail::CentralMailOperation,
};

/// Where an outbox write goes: a thread post or a direct message. It holds no
/// text, so the Mac may keep it after central answers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutboxWriteTarget {
    Thread {
        workspace_id: Option<WorkspaceId>,
        thread_id: ThreadId,
    },
    Session {
        to: SessionRef,
    },
}

impl OutboxWriteTarget {
    pub fn request_scope(&self) -> OutboxScope {
        match self {
            Self::Thread {
                workspace_id: Some(workspace_id),
                ..
            } => OutboxScope::Workspace(workspace_id.clone()),
            Self::Thread {
                workspace_id: None, ..
            }
            | Self::Session { .. } => OutboxScope::Organization,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutboxWriteContent {
    pub target: OutboxWriteTarget,
    pub body: PostText,
}

impl OutboxWriteContent {
    pub(crate) fn workspace(request: &workspace_model::WorkspaceRequest) -> Option<Self> {
        match &request.operation {
            WorkspaceOperation::ThreadPost {
                thread_id, body, ..
            } => Some(Self {
                target: OutboxWriteTarget::Thread {
                    workspace_id: request.workspace_id.clone(),
                    thread_id: thread_id.clone(),
                },
                body: body.clone(),
            }),
            _ => None,
        }
    }
}

/// Where a pending write stands. A write that went out in a batch whose answer
/// was lost may already be in central; it is resent under the same id. Its text
/// stays on the Mac only until central answers (owner, 2026-11-15: "unsent text
/// in the Mac outbox: allowed until central answers"), so only the unanswered
/// states carry the body.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum PendingWriteState {
    /// Never left the Mac.
    Unsent { body: PostText },
    /// Went out in a batch whose answer was lost; resent with the same id.
    SentAnswerLost { body: PostText },
    /// Central answered; the read layer drops it once it sees the created id.
    Answered(ReplyPayload),
    /// The Router stopped trying to send it; its text is gone.
    Cancelled { reason: crate::OutboxCancellation },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingWrite {
    pub request_id: RequestId,
    pub target: OutboxWriteTarget,
    pub flush_at_ms: i64,
    pub state: PendingWriteState,
}

/// How central answered a flushed write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutgoingOutcome {
    Answered(ReplyPayload),
    Refused(crate::OutboxRefusal),
}

/// An answered write, without its text: central holds the post or message now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutgoingWrite {
    pub request_id: RequestId,
    pub target: OutboxWriteTarget,
    pub outcome: OutgoingOutcome,
    pub answered_at_ms: i64,
}
/// A write the Router stopped trying to send because it passed its deadline;
/// its text is gone. The agent that wrote it is told.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndedWrite {
    pub caller: Caller,
    pub request_id: RequestId,
    pub target: OutboxWriteTarget,
    pub reason: crate::OutboxCancellation,
}
#[cfg(test)]
#[path = "outbox_views_test.rs"]
mod tests;
