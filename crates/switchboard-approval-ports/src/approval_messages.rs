use approval_request_protocol::{OfferView, PrivateEnding, ProviderStopCause};
use workspace_ids::{RequestId, RouterRunId};

use crate::{ApprovalScope, WaitingRequestId};

/// A losslessly translated ask that the harness makes after identifying its exact scope.
///
/// ```compile_fail,E0451
/// use approval_request_protocol::OfferView;
/// use switchboard_approval_ports::{ApprovalAsk, ApprovalScope, WaitingRequestId};
/// fn forge(scope: ApprovalScope, waiting_request: WaitingRequestId, offer: OfferView) -> ApprovalAsk {
///     ApprovalAsk { scope, waiting_request, offer }
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct ApprovalAsk {
    scope: ApprovalScope,
    waiting_request: WaitingRequestId,
    offer: OfferView,
}

impl ApprovalAsk {
    #[must_use]
    pub const fn scope(&self) -> &ApprovalScope {
        &self.scope
    }

    #[must_use]
    pub const fn waiting_request(&self) -> &WaitingRequestId {
        &self.waiting_request
    }

    #[must_use]
    pub const fn offer(&self) -> &OfferView {
        &self.offer
    }

    /// Gives custody the admitted offer and its routing keys.
    #[must_use]
    pub fn into_parts(self) -> (ApprovalScope, WaitingRequestId, OfferView) {
        (self.scope, self.waiting_request, self.offer)
    }
}

/// The harness's typed explanation when it cannot admit an agent request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum UncarriableAsk {
    #[error("the ask cannot be tied to a session")]
    UnprovedSession,
    #[error("the ask cannot be tied to a tool")]
    UnprovedTool,
    #[error("the ask cannot be tied to a turn")]
    UnprovedTurn,
    #[error("the offer cannot be carried exactly: {0}")]
    InvalidOffer(UncarriableShape),
    #[error("an answer cannot be carried exactly: {0}")]
    InvalidAnswer(UncarriableShape),
}

/// A shape the harness identifies as unrepresentable without losing information.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum UncarriableShape {
    #[error("number field")]
    NumberField,
    #[error("constrained field")]
    ConstrainedField,
    #[error("plan approval")]
    PlanApproval,
    #[error("form outside a session")]
    NonSessionForm,
    #[error("unsupported shape")]
    UnsupportedShape,
}

/// Custody's local reason for refusing an ask or ending its wait with a refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LocalApprovalRefusal {
    #[error("session is not known to central")]
    UnknownCentralSession,
    #[error("the wait stopped: {0:?}")]
    WaitStopped(MacWaitCause),
    #[error("central refused the raise")]
    RaiseRefused,
    #[error("the approval holder has no room")]
    NoRoom,
}

/// The reply contents custody selects after claiming an ending or producing a local refusal.
#[derive(Debug, Eq, PartialEq)]
pub enum ReplyKind {
    CentralEnding(PrivateEnding),
    LocalRefusal(LocalApprovalRefusal),
}

/// The reply custody authorizes the harness to translate for the waiting agent.
///
/// ```compile_fail,E0423
/// use switchboard_approval_ports::{ApprovalReply, ReplyKind};
/// fn forge(kind: ReplyKind) -> ApprovalReply { ApprovalReply(kind) }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct ApprovalReply(ReplyKind);

impl ApprovalReply {
    /// Gives the harness the typed reply to translate at hand-over.
    #[must_use]
    pub fn into_kind(self) -> ReplyKind {
        self.0
    }
}

/// Custody's instruction to write one reply on the named waiting request.
#[derive(Debug, Eq, PartialEq)]
pub struct ReplyInstruction {
    waiting_request: WaitingRequestId,
    reply: ApprovalReply,
}

impl ReplyInstruction {
    /// Gives the harness the exact local wait and reply to use at the hand-over point.
    #[must_use]
    pub fn into_parts(self) -> (WaitingRequestId, ApprovalReply) {
        (self.waiting_request, self.reply)
    }
}

/// Custody's instruction to release one named waiting request without a reply.
#[derive(Debug, Eq, PartialEq)]
pub struct WaitingRequestRelease(WaitingRequestId);

impl WaitingRequestRelease {
    #[must_use]
    pub const fn waiting_request(&self) -> &WaitingRequestId {
        &self.0
    }
}

/// The three queue hand-over observations the harness can distinguish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyHandoverKind {
    NotHandedOver,
    HandedToConnection,
    CannotTell,
}

/// The harness's report from the hand-over point. Queue admission proves only local hand-over.
///
/// ```compile_fail,E0423
/// use switchboard_approval_ports::{ReplyHandover, ReplyHandoverKind};
/// let _ = ReplyHandover(ReplyHandoverKind::HandedToConnection);
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct ReplyHandover(ReplyHandoverKind);

impl ReplyHandover {
    #[must_use]
    pub const fn kind(&self) -> ReplyHandoverKind {
        self.0
    }
}

/// The fact a session owner or agent connection proves and the harness reports to custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MacWaitCause {
    TurnStopRequested,
    SessionClosed,
    AgentConnectionLost,
    AgentAskWithdrawn,
}

impl MacWaitCause {
    /// Maps each Mac fact to its exact provider stop cause for central.
    #[must_use]
    pub const fn provider_stop_cause(self) -> ProviderStopCause {
        match self {
            Self::TurnStopRequested => ProviderStopCause::TurnStopped,
            Self::SessionClosed => ProviderStopCause::SessionClosed,
            Self::AgentConnectionLost => ProviderStopCause::ProviderConnectionLost,
            Self::AgentAskWithdrawn => ProviderStopCause::AskWithdrawn,
        }
    }
}

/// The harness relays an ended fact from its owner for one exact waiting request.
#[derive(Debug, Eq, PartialEq)]
pub struct MacWaitEnded {
    waiting_request: WaitingRequestId,
    cause: MacWaitCause,
}

impl MacWaitEnded {
    #[must_use]
    pub fn into_parts(self) -> (WaitingRequestId, MacWaitCause) {
        (self.waiting_request, self.cause)
    }
}

/// Side-specific constructors the Host gives to the harness at composition.
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::{HarnessApprovalValues, ReplyKind, WaitingRequestId};
/// fn forge(harness: &HarnessApprovalValues, wait: WaitingRequestId, reply: ReplyKind) {
///     harness.write_reply(wait, reply);
/// }
/// ```
#[derive(Debug)]
pub struct HarnessApprovalValues {
    _private: (),
}

impl HarnessApprovalValues {
    /// Names an admitted ask using a fresh harness-allocated nonce from the current Router run.
    ///
    /// ```compile_fail,E0308
    /// use approval_request_protocol::{OfferView, RouterRunId};
    /// use switchboard_approval_ports::{ApprovalScope, HarnessApprovalValues};
    /// fn bad(values: &HarnessApprovalValues, scope: ApprovalScope, run: RouterRunId, offer: OfferView) {
    ///     values.admitted_ask(scope, run, 42_u64, offer);
    /// }
    /// ```
    #[must_use]
    pub fn admitted_ask(
        &self,
        scope: ApprovalScope,
        router_run: RouterRunId,
        fresh_local_nonce: RequestId,
        offer: OfferView,
    ) -> ApprovalAsk {
        ApprovalAsk {
            scope,
            waiting_request: WaitingRequestId::from_admission(router_run, fresh_local_nonce),
            offer,
        }
    }

    /// Records what the harness observed when it tried to hand a reply over.
    #[must_use]
    pub const fn reply_handover(&self, kind: ReplyHandoverKind) -> ReplyHandover {
        ReplyHandover(kind)
    }

    /// Reports a fact the harness can match to this exact still-named wait.
    #[must_use]
    pub const fn wait_ended(
        &self,
        waiting_request: WaitingRequestId,
        cause: MacWaitCause,
    ) -> MacWaitEnded {
        MacWaitEnded {
            waiting_request,
            cause,
        }
    }
}

/// Side-specific constructors the Host gives to the Mac custody service at composition.
///
/// ```compile_fail,E0599
/// use approval_request_protocol::{OfferView, RouterRunId};
/// use switchboard_approval_ports::{ApprovalScope, CustodyApprovalValues};
/// use workspace_ids::RequestId;
/// fn forge(custody: &CustodyApprovalValues, scope: ApprovalScope, run: RouterRunId,
///          nonce: RequestId, offer: OfferView) {
///     custody.admitted_ask(scope, run, nonce, offer);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::{CustodyApprovalValues, ReplyHandoverKind};
/// fn forge(custody: &CustodyApprovalValues) {
///     custody.reply_handover(ReplyHandoverKind::HandedToConnection);
/// }
/// ```
#[derive(Debug)]
pub struct CustodyApprovalValues {
    _private: (),
}

impl CustodyApprovalValues {
    /// Authorizes the harness to write this reply for one wait.
    #[must_use]
    pub const fn write_reply(
        &self,
        waiting_request: WaitingRequestId,
        kind: ReplyKind,
    ) -> ReplyInstruction {
        ReplyInstruction {
            waiting_request,
            reply: ApprovalReply(kind),
        }
    }

    /// Authorizes the harness to release the wait without writing a reply.
    #[must_use]
    pub const fn release_wait(&self, waiting_request: WaitingRequestId) -> WaitingRequestRelease {
        WaitingRequestRelease(waiting_request)
    }
}

/// The Host obtains both constructor sets and gives each component only its own set.
///
/// Each side can construct only its own envelopes when given its own set. Rust
/// visibility does not restrict this composition function to a particular dependent crate.
#[must_use]
pub const fn approval_value_builders() -> (HarnessApprovalValues, CustodyApprovalValues) {
    (
        HarnessApprovalValues { _private: () },
        CustodyApprovalValues { _private: () },
    )
}

/// The ACP harness implements these two requests from custody and reports its local outcome.
pub trait ApprovalHarnessPort {
    /// Applies the final check and reports the exact reply's queue hand-over outcome.
    fn write_reply(&self, instruction: ReplyInstruction) -> ReplyHandover;

    /// Releases the exact wait without a reply and reports the local hand-over outcome.
    fn release_wait(&self, instruction: WaitingRequestRelease) -> ReplyHandover;
}

/// The Mac custody service implements these immediate ingress calls from the harness.
pub trait ApprovalCustodyPort {
    /// Accepts the checked ask for custody or returns a local refusal without waiting on central.
    fn admit_ask(&self, ask: ApprovalAsk) -> Result<(), LocalApprovalRefusal>;

    /// Receives a proved terminal fact for the named waiting request.
    fn wait_ended(&self, event: MacWaitEnded);
}
