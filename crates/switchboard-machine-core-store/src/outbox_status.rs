//! The durable answer to a caller's outbox status query.
use serde::{Deserialize, Serialize};
use workspace_ids::{MessageId, PostId};
use workspace_model::{ActorView, ChangePosition, RefusalReason, central_mail::CentralMailRefusal};

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
    Central(OutboxCentralRefusal),
}

/// A central domain refusal retained in `outcome_json`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "reason",
    rename_all = "camelCase",
    deny_unknown_fields
)]
pub enum OutboxCentralRefusal {
    Workspace(RefusalReason),
    Mail(CentralMailRefusal),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxRequestStatus {
    NotFound,
    InOutbox { flush_at_ms: i64 },
    Sent,
    Answered(OutboxAccepted),
    Refused(OutboxRefusal),
    Ended { reason: OutboxCancellation },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum OutboxCancellation {
    NotSentWithin15Minutes,
    NotConfirmedWithin15Minutes,
}

/// A successful write retained by the outbox.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum OutboxAccepted {
    ThreadPost {
        post_id: PostId,
        position: ChangePosition,
        actors: Vec<ActorView>,
    },
    SendMessage {
        message_id: MessageId,
    },
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
