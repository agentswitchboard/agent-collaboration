//! The ACP client asks Host policy to route and settle provider interactions.

#[cfg(any(test, feature = "test-support"))]
use std::hash::Hash;
use std::{future::Future, pin::Pin, sync::Arc};
use switchboard_harness_contracts::{
    RequestCancellationSignal, RequestFuture, RequestPolicy, RequestPolicyPayloads,
};
use tokio_util::sync::CancellationToken;

use crate::RefusedPermissionOption;
use session_event_model::managed_interaction_refusal::ManagedInteractionRefusal;
use session_event_model::{ApprovalRequest, ApprovalSubject, QuestionRequest, QuestionResponse};

pub type InteractionFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
#[cfg(any(test, feature = "test-support"))]
pub type FixtureCancellation = Arc<dyn RequestCancellationSignal>;

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

pub type CanonicalRequestPayloads<Context, OperationId> = (
    Context,
    OperationId,
    ApprovalRequest,
    QuestionRequest,
    QuestionResponse,
    ManagedInteractionRefusal,
);

pub trait AcpRequestPayloads:
    RequestPolicyPayloads<
        ApprovalRequest = ApprovalRequest,
        QuestionRequest = QuestionRequest,
        QuestionResponse = QuestionResponse,
        ManagedRefusal = ManagedInteractionRefusal,
    > + 'static
{
}

impl<Payloads> AcpRequestPayloads for Payloads where
    Payloads: RequestPolicyPayloads<
            ApprovalRequest = ApprovalRequest,
            QuestionRequest = QuestionRequest,
            QuestionResponse = QuestionResponse,
            ManagedRefusal = ManagedInteractionRefusal,
        > + 'static
{
}

pub(crate) type AcpRequestPolicy<Payloads> = dyn RequestPolicy<Payloads>;

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

/// ACP fixture policy. The connection itself receives the neutral policy.
#[cfg(any(test, feature = "test-support"))]
pub trait InteractionPort: Send + Sync + 'static {
    type Context: Clone + Send + Sync + 'static;
    type OperationId: Clone + Eq + Hash + Send + Sync + 'static;

    fn managed_refusal(
        &self,
    ) -> Option<session_event_model::managed_interaction_refusal::ManagedInteractionRefusal> {
        None
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
        _turn_cancellation: FixtureCancellation,
        _agent_cancellation: FixtureCancellation,
    ) -> InteractionFuture<'_, ApprovalPortOutcome> {
        Box::pin(async { ApprovalPortOutcome::Unavailable })
    }

    fn request_question(
        &self,
        _context: Self::Context,
        _request: QuestionRequest,
        _turn_cancellation: FixtureCancellation,
        _agent_cancellation: FixtureCancellation,
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
