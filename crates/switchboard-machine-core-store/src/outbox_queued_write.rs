//! The write as accepted by the Router. Only the outbox claim adds send time.
use crate::{OutboxScope, OutboxWriteContent, OutboxWriteTarget};
use serde::{Deserialize, Serialize};
use workspace_ids::RequestId;
use workspace_model::{
    DigestError, WorkspaceRequest, central_mail::SendMessageRequest, operation_content_digest,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "request",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutboxQueuedWrite {
    Workspace(WorkspaceRequest),
    SendMessage {
        request_id: RequestId,
        message: SendMessageRequest,
    },
}

impl OutboxQueuedWrite {
    pub fn request_id(&self) -> Option<&RequestId> {
        match self {
            Self::Workspace(request) => request.request_id.as_ref(),
            Self::SendMessage { request_id, .. } => Some(request_id),
        }
    }

    pub fn request_scope(&self) -> OutboxScope {
        match self {
            Self::Workspace(request) => request
                .workspace_id
                .clone()
                .map_or(OutboxScope::Organization, OutboxScope::Workspace),
            Self::SendMessage { .. } => OutboxScope::Organization,
        }
    }

    pub(crate) fn content(&self) -> Option<OutboxWriteContent> {
        match self {
            Self::Workspace(request) => OutboxWriteContent::workspace(request),
            Self::SendMessage { message, .. } => Some(OutboxWriteContent {
                target: OutboxWriteTarget::Session {
                    to: message.to.clone(),
                },
                body: message.body.clone(),
            }),
        }
    }

    /// The same operation digest that Central's request log retains.
    pub(crate) fn content_digest(&self) -> Result<String, DigestError> {
        match self {
            Self::Workspace(request) => operation_content_digest(&request.operation),
            Self::SendMessage { message, .. } => operation_content_digest(message),
        }
    }
}

#[cfg(test)]
#[path = "outbox_queued_write_test.rs"]
mod tests;
