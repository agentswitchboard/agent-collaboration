//! The ACP client asks Host policy to route and settle provider interactions.

use std::{future::Future, hash::Hash, pin::Pin, sync::Arc};
use switchboard_harness_contracts::{RequestCancellationSignal, RequestFuture, RequestPolicy};
use tokio_util::sync::CancellationToken;

use crate::RefusedPermissionOption;
use session_event_model::managed_interaction_refusal::ManagedInteractionRefusal;
use session_event_model::{ApprovalRequest, ApprovalSubject, QuestionRequest, QuestionResponse};

pub type InteractionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub use switchboard_harness_contracts::ApprovalPortOutcome;

pub(crate) fn observe_cancellation(
    signal: CancellationToken,
) -> Arc<dyn RequestCancellationSignal> {
    struct TokenObservation(CancellationToken);

    impl RequestCancellationSignal for TokenObservation {
        fn is_cancelled(&self) -> bool {
            self.0.is_cancelled()
        }

        fn cancelled(&self) -> RequestFuture<'_, ()> {
            Box::pin(self.0.cancelled())
        }
    }

    Arc::new(TokenObservation(signal))
}

pub type CanonicalRequestPolicy<Context, OperationId> = dyn RequestPolicy<(
    Context,
    OperationId,
    ApprovalRequest,
    QuestionRequest,
    QuestionResponse,
    ManagedInteractionRefusal,
)>;

/// Malformed ACP offers are recorded and answered cancelled. This type never
/// appears in the public session profile or replaces a valid ApprovalRequest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefusedApprovalOffer {
    pub request_id: String,
    pub reason: &'static str,
    pub title: String,
    pub description: Option<String>,
    pub subject: Option<ApprovalSubject>,
    pub options: Vec<RefusedPermissionOption>,
}

/// The context is Host-owned. ACP code never names Router operation or generation types.
pub trait InteractionPort: Send + Sync + 'static {
    type Context: Clone + Send + Sync + 'static;
    type OperationId: Clone + Eq + Hash + Send + Sync + 'static;

    /// Track B's transition boundary while ACP's shared construction and fixture
    /// ports still receive this ACP adapter trait.
    fn request_policy(&self) -> Option<&CanonicalRequestPolicy<Self::Context, Self::OperationId>> {
        None
    }

    fn managed_refusal(
        &self,
    ) -> Option<session_event_model::managed_interaction_refusal::ManagedInteractionRefusal> {
        self.request_policy()
            .and_then(RequestPolicy::managed_refusal)
    }

    fn cancel_after_managed_refusal(&self, _context: Self::Context) -> InteractionFuture<'_, ()> {
        Box::pin(async {})
    }

    fn operation_id(context: &Self::Context) -> Self::OperationId;

    fn binding_retirement(context: &Self::Context) -> CancellationToken;

    fn bind_owner_scope(
        _context: &mut Self::Context,
        _run: provider_session_machines::SessionGeneration,
        _turn: provider_session_machines::session_turn_machine::TurnId,
    ) {
    }

    fn request_approval(
        &self,
        _context: Self::Context,
        _request: ApprovalRequest,
        _turn_cancellation: CancellationToken,
        _agent_cancellation: CancellationToken,
    ) -> InteractionFuture<'_, ApprovalPortOutcome> {
        Box::pin(async { ApprovalPortOutcome::Unavailable })
    }

    fn request_question(
        &self,
        _context: Self::Context,
        _request: QuestionRequest,
        _turn_cancellation: CancellationToken,
        _agent_cancellation: CancellationToken,
    ) -> InteractionFuture<'_, QuestionResponse> {
        Box::pin(async { QuestionResponse::Cancelled })
    }

    fn record_refusal(
        &self,
        _context: Self::Context,
        _refusal: RefusedApprovalOffer,
    ) -> InteractionFuture<'_, ()> {
        Box::pin(async {})
    }

    fn cancel_all(
        &self,
        _context: Self::Context,
        _reason: &'static str,
    ) -> InteractionFuture<'_, ()> {
        Box::pin(async {})
    }
}
