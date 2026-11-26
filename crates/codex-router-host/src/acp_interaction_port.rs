//! Managed ACP callback policy and its exact-turn cancellation port.

use acp_client_runtime::{InteractionFuture, InteractionPort};
use collaboration_protocol::OperationId;
use provider_session_machines::{
    SessionGeneration,
    session_owner::{
        ApprovalContext, ApprovalPortOutcome, Decision, ExactScope, ExactTurn, RequestCancellation,
        RequestCancellationSignal, RequestFuture, RequestPolicy, StopRequestsCause,
    },
    session_turn_machine::TurnId,
};
use session_event_model::managed_interaction_refusal::ManagedInteractionRefusal;
use session_event_model::{ApprovalRequest, QuestionRequest, QuestionResponse};
use tokio_util::sync::CancellationToken;

use crate::{
    ExternalProviderApprovalContext, external_provider_approval_context::OwnerApprovalScope,
};
use std::sync::Arc;

pub(crate) trait RefusedTurnCancellation: Send + Sync {
    fn cancel_refused_turn(
        &self,
        session: String,
        scope: OwnerApprovalScope,
    ) -> InteractionFuture<'_, ()>;
}

pub(crate) struct HostInteractionPort(pub(crate) Arc<dyn RefusedTurnCancellation>);

struct BindingRetirement(CancellationToken);

impl RequestCancellationSignal for BindingRetirement {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    fn cancelled(&self) -> RequestFuture<'_, ()> {
        Box::pin(self.0.cancelled())
    }
}

type HostRequestPayloads = (
    ExternalProviderApprovalContext,
    OperationId,
    ApprovalRequest,
    QuestionRequest,
    QuestionResponse,
    ManagedInteractionRefusal,
);

impl RequestPolicy<HostRequestPayloads> for HostInteractionPort {
    fn decide_approval(
        &self,
        _context: ApprovalContext<ExternalProviderApprovalContext>,
        _request: ApprovalRequest,
        _cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<ApprovalPortOutcome>> {
        Box::pin(async { Decision::Answer(ApprovalPortOutcome::Unavailable) })
    }

    fn decide_question(
        &self,
        _context: ApprovalContext<ExternalProviderApprovalContext>,
        _request: QuestionRequest,
        _cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<QuestionResponse>> {
        Box::pin(async { Decision::Answer(QuestionResponse::Cancelled) })
    }

    fn operation_id(&self, context: &ExternalProviderApprovalContext) -> OperationId {
        context.operation_id.clone()
    }

    fn binding_retirement(
        &self,
        context: &ExternalProviderApprovalContext,
    ) -> Arc<dyn RequestCancellationSignal> {
        Arc::new(BindingRetirement(context.binding_retirement.clone()))
    }

    fn bind_owner_scope(&self, context: &mut ExternalProviderApprovalContext, turn: ExactTurn) {
        context.owner_scope = Some(OwnerApprovalScope {
            run: turn.run.generation,
            turn: turn.turn,
        });
    }

    fn managed_refusal(&self) -> Option<ManagedInteractionRefusal> {
        Some(ManagedInteractionRefusal::ApprovalsUnavailableOnBuild)
    }

    fn cancel_after_managed_refusal(
        &self,
        context: ApprovalContext<ExternalProviderApprovalContext>,
    ) -> RequestFuture<'_, ()> {
        Box::pin(async move {
            if let ExactScope::Turn(turn) = context.scope {
                let session = String::from(context.context.target.session_id);
                let scope = OwnerApprovalScope {
                    run: turn.run.generation,
                    turn: turn.turn,
                };
                self.0.cancel_refused_turn(session, scope).await;
            }
        })
    }

    fn stop_all(
        &self,
        _context: ExternalProviderApprovalContext,
        _turn: ExactTurn,
        _cause: StopRequestsCause,
    ) -> RequestFuture<'_, ()> {
        Box::pin(async {})
    }
}

impl InteractionPort for HostInteractionPort {
    type Context = ExternalProviderApprovalContext;
    type OperationId = OperationId;

    fn request_policy(&self) -> Option<&dyn RequestPolicy<HostRequestPayloads>> {
        Some(self)
    }

    fn cancel_after_managed_refusal(&self, context: Self::Context) -> InteractionFuture<'_, ()> {
        Box::pin(async move {
            if let Some(scope) = context.owner_scope {
                let session = String::from(context.target.session_id);
                self.0.cancel_refused_turn(session, scope).await;
            }
        })
    }

    fn operation_id(context: &Self::Context) -> Self::OperationId {
        context.operation_id.clone()
    }

    fn binding_retirement(context: &Self::Context) -> CancellationToken {
        context.binding_retirement.clone()
    }

    fn bind_owner_scope(context: &mut Self::Context, run: SessionGeneration, turn: TurnId) {
        context.owner_scope = Some(OwnerApprovalScope { run, turn });
    }
}
