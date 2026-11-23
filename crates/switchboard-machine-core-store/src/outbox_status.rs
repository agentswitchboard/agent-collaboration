//! The durable answer to a caller's outbox status query.
use central_link_protocol::{ReplyPayload, RequestRefusal};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalOutboxRefusal {
    RequestIdConflict,
}

/// Refusals retain their source. Central reasons serialize in their native form.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OutboxRefusal {
    Local(LocalOutboxRefusal),
    Central(RequestRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxRequestStatus {
    NotFound,
    InOutbox { flush_at_ms: i64 },
    Sent,
    Answered(ReplyPayload),
    Refused(OutboxRefusal),
    Ended { reason: OutboxCancellation },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OutboxCancellation {
    NotSentWithin15Minutes,
    NotConfirmedWithin15Minutes,
}

/// An original request's owning store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxScope {
    Organization,
    Workspace(workspace_ids::WorkspaceId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxRequestKey {
    pub request_id: workspace_ids::RequestId,
    pub scope: OutboxScope,
}
