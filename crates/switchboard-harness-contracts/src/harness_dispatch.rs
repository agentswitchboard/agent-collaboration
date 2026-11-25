//! Authorization and outcomes for work crossing the harness boundary.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    AuthorizationTarget, InputId, OwnerIncarnation, PendingTurnResponse, SessionGeneration,
    SettingApplication, TurnId,
};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct DispatchAuthorization {
    pub incarnation: OwnerIncarnation,
    pub run: SessionGeneration,
    pub input_id: InputId,
    pub authorization_seq: u64,
    pub target: AuthorizationTarget,
}

impl DispatchAuthorization {
    /// Check the route under the same transport lock as identity comparison
    /// and enqueue. A successful check alone does not authorize a wire send.
    #[must_use]
    pub fn matches_route(&self, run: SessionGeneration, current_turn: Option<TurnId>) -> bool {
        self.run == run
            && match self.target {
                AuthorizationTarget::ExistingTurn { turn } => current_turn == Some(turn),
                AuthorizationTarget::NewTurn | AuthorizationTarget::Session => true,
            }
    }
}

/// The local decision can finish while the peer's effect remains unknown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WireOutcome<Reply> {
    Confirmed(Reply),
    Refused,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptCompletion {
    pub stop_reason: StopReason,
    pub local_cause: Option<LocalCause>,
    pub response: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    Unknown(String),
}

#[derive(schemars::JsonSchema, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LocalCause {
    OutputOverflow,
    Other(String),
}

/// Correlation for a coded reply, with no provider-controlled message text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderWireFailure {
    pub kind: ProviderWireFailureKind,
    pub code: i64,
    pub correlation_id: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderWireFailureKind {
    AuthenticationRequired,
    SessionNotFound,
    ResourceNotFound,
    UnsupportedMethod,
    InvalidParams,
    RequestCancelled,
    Rejected,
    RestoreFailed,
    RestoreRefused,
}

impl ProviderWireFailure {
    #[must_use]
    pub const fn definitely_no_effect(self) -> bool {
        matches!(
            self.kind,
            ProviderWireFailureKind::AuthenticationRequired
                | ProviderWireFailureKind::SessionNotFound
                | ProviderWireFailureKind::ResourceNotFound
                | ProviderWireFailureKind::UnsupportedMethod
                | ProviderWireFailureKind::InvalidParams
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancelDrainReport {
    Drained,
    Expired { pending: Vec<PendingTurnResponse> },
}

impl CancelDrainReport {
    pub const EXPIRED_SUMMARY: &'static str =
        "cancel sent; some pending approval replies may reach the harness after it";
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FastReadback {
    NotOffered,
    Off,
    On {
        application: SettingApplication,
        input: InputId,
    },
}
