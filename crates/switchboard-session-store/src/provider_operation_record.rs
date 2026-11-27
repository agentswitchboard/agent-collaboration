//! Metadata retained for a provider operation.

use collaboration_protocol::{
    ConversationBindingIdentity, OperationId, ProviderOperationEffect, ProviderOperationKind,
    ProviderOperationStage, ProviderPromptStopReason, ProviderReconciliationState, SessionRef,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOperationRecord {
    pub operation_id: OperationId,
    pub operation_kind: ProviderOperationKind,
    pub binding: ConversationBindingIdentity,
    pub target: Option<SessionRef>,
    pub stage: ProviderOperationStage,
    pub effect: ProviderOperationEffect,
    pub reconciliation_state: ProviderReconciliationState,
    pub admitted_at_ms: i64,
    pub dispatched_at_ms: Option<i64>,
    pub terminal_at_ms: Option<i64>,
    pub terminal_stop_reason: Option<ProviderPromptStopReason>,
    pub updated_at_ms: i64,
}
