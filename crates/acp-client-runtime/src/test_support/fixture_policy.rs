//! Adapt existing ACP test doubles to the connection's neutral policy port.

use crate::{
    ApprovalPortOutcome, CanonicalRequestPayloads, InteractionPort,
    interaction_port::observe_cancellation,
};
use session_event_model::{
    ApprovalRequest, QuestionRequest, QuestionResponse,
    managed_interaction_refusal::ManagedInteractionRefusal,
};
use std::sync::Arc;
use switchboard_harness_contracts::{
    ApprovalContext, Decision, ExactTurn, RequestCancellation, RequestCancellationSignal,
    RequestFuture, RequestPolicy, StopRequestsCause,
};

pub(crate) type FixturePayloads<Port> = CanonicalRequestPayloads<
    <Port as InteractionPort>::Context,
    <Port as InteractionPort>::OperationId,
>;

pub(crate) struct FixturePolicy<Port>(pub(crate) Arc<Port>);

impl<Port: InteractionPort> RequestPolicy<FixturePayloads<Port>> for FixturePolicy<Port> {
    fn decide_approval(
        &self,
        context: ApprovalContext<Port::Context>,
        request: ApprovalRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<ApprovalPortOutcome>> {
        Box::pin(async move {
            Decision::Answer(
                self.0
                    .request_approval(
                        context.context,
                        request,
                        cancellation.turn,
                        cancellation.request,
                    )
                    .await,
            )
        })
    }

    fn decide_question(
        &self,
        context: ApprovalContext<Port::Context>,
        request: QuestionRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<QuestionResponse>> {
        Box::pin(async move {
            Decision::Answer(
                self.0
                    .request_question(
                        context.context,
                        request,
                        cancellation.turn,
                        cancellation.request,
                    )
                    .await,
            )
        })
    }

    fn operation_id(&self, context: &Port::Context) -> Port::OperationId {
        Port::operation_id(context)
    }

    fn binding_retirement(&self, context: &Port::Context) -> Arc<dyn RequestCancellationSignal> {
        observe_cancellation(Port::binding_retirement(context))
    }

    fn bind_owner_scope(&self, context: &mut Port::Context, turn: ExactTurn) {
        Port::bind_owner_scope(context, turn.run.generation, turn.turn);
    }

    fn managed_refusal(&self) -> Option<ManagedInteractionRefusal> {
        self.0.managed_refusal()
    }

    fn cancel_after_managed_refusal(
        &self,
        context: ApprovalContext<Port::Context>,
    ) -> RequestFuture<'_, ()> {
        self.0.cancel_after_managed_refusal(context.context)
    }

    fn stop_all(
        &self,
        context: Port::Context,
        _turn: ExactTurn,
        cause: StopRequestsCause,
    ) -> RequestFuture<'_, ()> {
        match cause {
            StopRequestsCause::TurnCancelled => self.0.cancel_all(context, "turnCancelled"),
        }
    }
}
