//! Exact request ownership, policy decisions and the turn's approval lease.

use std::{future::Future, hash::Hash, pin::Pin, sync::Arc};

use crate::{TransportRun, TurnId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactTurn {
    pub run: TransportRun,
    pub turn: TurnId,
}

/// Captured when the harness admits the request. A later turn cannot acquire it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactScope {
    Session { run: TransportRun },
    Turn(ExactTurn),
}

/// The controller supplies the opaque context and owns its interpretation.
pub struct ApprovalContext<Context> {
    pub scope: ExactScope,
    pub context: Context,
}

pub type RequestFuture<'a, Output> = Pin<Box<dyn Future<Output = Output> + Send + 'a>>;

/// Observe cancellation without giving policy ownership of the signal.
pub trait RequestCancellationSignal: Send + Sync {
    fn is_cancelled(&self) -> bool;
    fn cancelled(&self) -> RequestFuture<'_, ()>;
}

#[derive(Clone)]
pub struct RequestCancellation {
    pub turn: Arc<dyn RequestCancellationSignal>,
    pub request: Arc<dyn RequestCancellationSignal>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApprovalPortOutcome {
    Selected {
        option_id: String,
        note: Option<String>,
    },
    Cancelled,
    Unavailable,
}

/// A canonical policy answer or an explicit refusal of this request.
pub enum Decision<Answer> {
    Answer(Answer),
    Refused(RequestDecisionRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestDecisionRefusal {
    NoOpenScope,
    ScopeEnded,
    AlreadyAnswered,
    AnsweredElsewhere,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusedRequestMove {
    ReturnRefusal,
    KeepExistingAnswer,
}

impl RequestDecisionRefusal {
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NoOpenScope => "the request has no open session or turn scope",
            Self::ScopeEnded => "the request's admitted scope has ended",
            Self::AlreadyAnswered => "this request already has an answer",
            Self::AnsweredElsewhere => "another client answered this request",
        }
    }

    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedRequestMove] {
        match self {
            Self::NoOpenScope | Self::ScopeEnded => &[RefusedRequestMove::ReturnRefusal],
            Self::AlreadyAnswered | Self::AnsweredElsewhere => {
                &[RefusedRequestMove::KeepExistingAnswer]
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopRequestsCause {
    TurnCancelled,
}

/// The controller selects these canonical payloads for every harness.
pub trait RequestPolicyPayloads {
    type Context: Clone + Send + Sync + 'static;
    type OperationId: Clone + Eq + Hash + Send + Sync + 'static;
    type ApprovalRequest: Send + 'static;
    type QuestionRequest: Send + 'static;
    type QuestionResponse: Send + 'static;
    type ManagedRefusal: Send + 'static;
}

impl<Context, OperationId, ApprovalRequest, QuestionRequest, QuestionResponse, ManagedRefusal>
    RequestPolicyPayloads
    for (
        Context,
        OperationId,
        ApprovalRequest,
        QuestionRequest,
        QuestionResponse,
        ManagedRefusal,
    )
where
    Context: Clone + Send + Sync + 'static,
    OperationId: Clone + Eq + Hash + Send + Sync + 'static,
    ApprovalRequest: Send + 'static,
    QuestionRequest: Send + 'static,
    QuestionResponse: Send + 'static,
    ManagedRefusal: Send + 'static,
{
    type Context = Context;
    type OperationId = OperationId;
    type ApprovalRequest = ApprovalRequest;
    type QuestionRequest = QuestionRequest;
    type QuestionResponse = QuestionResponse;
    type ManagedRefusal = ManagedRefusal;
}

/// The harness owns the pending responder and must return at most one answer
/// in the scope in which it admitted the request. Refuse a request with no
/// open scope before invoking policy. Observe cancellation while awaiting
/// the same decision, and preserve the request ID across a harness resume.
pub trait RequestPolicy<Payloads: RequestPolicyPayloads>: Send + Sync + 'static {
    fn decide_approval(
        &self,
        context: ApprovalContext<Payloads::Context>,
        request: Payloads::ApprovalRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<ApprovalPortOutcome>>;

    fn decide_question(
        &self,
        context: ApprovalContext<Payloads::Context>,
        request: Payloads::QuestionRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<Payloads::QuestionResponse>>;

    fn operation_id(&self, context: &Payloads::Context) -> Payloads::OperationId;

    fn binding_retirement(&self, context: &Payloads::Context)
    -> Arc<dyn RequestCancellationSignal>;

    fn bind_owner_scope(&self, context: &mut Payloads::Context, turn: ExactTurn);

    /// Ask before normalizing a managed offer so the harness can select the
    /// response appropriate for the original method.
    fn managed_refusal(&self) -> Option<Payloads::ManagedRefusal>;

    /// Invoke after the harness has enqueued the refusal and released the
    /// request's response guard.
    fn cancel_after_managed_refusal(
        &self,
        context: ApprovalContext<Payloads::Context>,
    ) -> RequestFuture<'_, ()>;

    /// Run the policy's stop for the exact turn. The harness signals cancellation
    /// before this call and awaits responder drainage separately.
    fn stop_all(
        &self,
        context: Payloads::Context,
        turn: ExactTurn,
        cause: StopRequestsCause,
    ) -> RequestFuture<'_, ()>;
}

/// The guard belongs to one operation in one turn. Dropping it releases only
/// that operation's claim to the session's approval context.
pub trait ApprovalLease: Send + Sync {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefusedApprovalLeaseMove {
    /// The prompt has not reached the writer.
    SettleAsNotSent,
    /// The harness already reported this turn starting.
    KeepObservedTurn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalLeaseRefusalReason {
    ContextUnavailable,
    AlreadyActive,
}

impl ApprovalLeaseRefusalReason {
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::ContextUnavailable => "the harness approval context is unavailable",
            Self::AlreadyActive => "this session already has an active approval context",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BeforeOfferApprovalLeaseRefusal(pub ApprovalLeaseRefusalReason);

impl BeforeOfferApprovalLeaseRefusal {
    #[must_use]
    pub const fn reason(self) -> &'static str {
        self.0.reason()
    }

    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedApprovalLeaseMove] {
        &[RefusedApprovalLeaseMove::SettleAsNotSent]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservedTurnApprovalLeaseRefusal(pub ApprovalLeaseRefusalReason);

impl ObservedTurnApprovalLeaseRefusal {
    #[must_use]
    pub const fn reason(self) -> &'static str {
        self.0.reason()
    }

    #[must_use]
    pub const fn valid_moves(self) -> &'static [RefusedApprovalLeaseMove] {
        &[RefusedApprovalLeaseMove::KeepObservedTurn]
    }
}

/// The harness binding addresses the session. The turn addresses the exact
/// incarnation and generation within it.
pub trait ApprovalLeaseBinding<Context>: Send + Sync {
    /// Install before the prompt reaches an offer. Refusal settles it as not sent.
    fn install_approval_lease_before_offer(
        &self,
        turn: ExactTurn,
        context: Context,
    ) -> Result<Box<dyn ApprovalLease>, BeforeOfferApprovalLeaseRefusal>;

    /// Install for a turn the harness already started. A refusal leaves its
    /// submission and turn standing.
    fn install_approval_lease_for_observed_turn(
        &self,
        turn: ExactTurn,
        context: Context,
    ) -> Result<Box<dyn ApprovalLease>, ObservedTurnApprovalLeaseRefusal>;
}

#[cfg(test)]
#[path = "harness_request_policy_test.rs"]
mod tests;
