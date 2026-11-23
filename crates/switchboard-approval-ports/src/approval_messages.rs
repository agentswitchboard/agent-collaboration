use approval_request_protocol::{OfferView, PrivateEnding, ProviderStopCause};
use workspace_ids::RequestId;

use crate::{ApprovalScope, SessionOwnerApprovalValues, WaitingRequestId};

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
    /// The binding was never registered or has been retired. This is a composition fault,
    /// never an ordinary refusal or a result of the central session read.
    #[error("custody has no live registration for the session binding")]
    UnregisteredSessionBinding,
    #[error("session is not known to central")]
    UnknownCentralSession,
    #[error("the wait stopped: {0:?}")]
    WaitStopped(ReplyableStopCause),
    #[error("central refused the raise: {0:?}")]
    RaiseRefused(TerminalRaiseRefusal),
    #[error("the approval holder has no room")]
    NoRoom,
}

/// The stop custody can report in a reply while the provider's slot can still be used.
///
/// ```compile_fail,E0308
/// use switchboard_approval_ports::{LocalApprovalRefusal, MacWaitCause};
/// let _ = LocalApprovalRefusal::WaitStopped(MacWaitCause::AgentAskWithdrawn);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyableStopCause {
    TurnStopped,
    SessionClosed,
    /// Produced only by the session owner when it replaces an ask. No ACP ingress in the first cut.
    AskReplaced,
}

/// The terminal Raise outcome custody records from central's reply after excluding receipts and run authority.
///
/// Keep these cases aligned with the approval holder's RaiseRefusal.
///
/// ```compile_fail,E0308
/// use approval_request_protocol::RaiseReply;
/// use switchboard_approval_ports::LocalApprovalRefusal;
/// fn wrong_refusal(reply: RaiseReply) -> LocalApprovalRefusal {
///     LocalApprovalRefusal::RaiseRefused(reply)
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalRaiseRefusal {
    AlreadyStopped,
    SessionEnded,
    ContentsDiffer,
    TooManyOutstanding,
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

/// Custody's one-use submission made for the exact writer after the holder commits its start.
///
/// This belongs to the inline enqueue call, separate from any queued work or permit.
#[derive(Debug, Eq, PartialEq)]
pub struct StartedReplySubmission {
    waiting_request: WaitingRequestId,
    reply: ApprovalReply,
}

impl StartedReplySubmission {
    /// Gives the harness the exact local wait and reply to use at the hand-over point.
    #[must_use]
    pub fn into_parts(self) -> (WaitingRequestId, ApprovalReply) {
        (self.waiting_request, self.reply)
    }
}

/// Custody's instruction to revoke the harness's lookup authority for one wait.
///
/// It applies even after a writer has started. Revocation preserves that writer's ability to
/// report its original outcome and leaves responder disposal to the appropriate owner.
#[derive(Debug, Eq, PartialEq)]
pub struct WaitBindingRevocation(WaitingRequestId);

impl WaitBindingRevocation {
    #[must_use]
    pub const fn waiting_request(&self) -> &WaitingRequestId {
        &self.0
    }
}

/// Custody's instruction to dispose of one unstarted responder after revoking its lookup.
///
/// ```compile_fail,E0308
/// use switchboard_approval_ports::{ApprovalHarnessPort, WaitBindingRevocation};
/// fn wrong_release(port: &impl ApprovalHarnessPort, revocation: WaitBindingRevocation) {
///     port.release_unstarted_wait(revocation);
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct UnstartedWaitRelease(WaitingRequestId);

impl UnstartedWaitRelease {
    #[must_use]
    pub const fn waiting_request(&self) -> &WaitingRequestId {
        &self.0
    }
}

/// The local result the harness observes when asked to release a wait without a reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitReleaseKind {
    /// The harness released the wait and sent nothing.
    ReleasedWithoutReply,
    /// That exact waiting request was already gone.
    AlreadyGone,
    /// The harness could not dispose of the responder silently on the live connection.
    CannotReleaseSilently,
    /// The harness cannot establish what happened during release.
    CannotTell,
}

/// The harness's local disposal result for the exact unstarted wait.
///
/// Released and already gone settle the local duty. A blocked or unknown disposal remains
/// with the harness under that waiting request. It grants no new reply right and produces
/// no provider outcome or central receipt.
///
/// ```compile_fail,E0423
/// use switchboard_approval_ports::{WaitReleaseKind, WaitReleaseOutcome};
/// let _ = WaitReleaseOutcome(WaitReleaseKind::ReleasedWithoutReply);
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct WaitReleaseOutcome(WaitReleaseKind);

impl WaitReleaseOutcome {
    #[must_use]
    pub const fn kind(&self) -> WaitReleaseKind {
        self.0
    }
}

/// The three queue hand-over observations the harness can distinguish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyHandoverKind {
    NotHandedOver,
    HandedToConnection,
    CannotTell,
}

/// What the harness observed for the submission of an already-started reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplySubmissionKind {
    /// The exact slot had vanished before any submission could be attempted.
    SlotGoneBeforeSubmission,
    /// The observation made at the agent connection's hand-over point.
    Handover(ReplyHandoverKind),
}

/// The harness's report for one started writer. Queue admission proves local hand-over.
///
/// ```compile_fail,E0423
/// use switchboard_approval_ports::{ReplySubmissionKind, ReplySubmissionOutcome};
/// let _ = ReplySubmissionOutcome(ReplySubmissionKind::SlotGoneBeforeSubmission);
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct ReplySubmissionOutcome(ReplySubmissionKind);

impl ReplySubmissionOutcome {
    #[must_use]
    pub const fn kind(&self) -> ReplySubmissionKind {
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
    /// Names the cause for a provider Stop sent to central.
    ///
    /// Custody routes an exact withdrawal as the holder's ProviderWithdrew event. The other
    /// facts enter LocalStop. This wire cause alone does not select the holder input.
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

/// The harness reports that this exact reply handle became unusable before any reply started.
///
/// This observation carries only the waiting request. Custody keeps any independently owed
/// Stop. There is no wire stop cause or writer completion for the loss itself.
#[derive(Debug, Eq, PartialEq)]
pub struct ReplyHandleUnavailable(WaitingRequestId);

impl ReplyHandleUnavailable {
    #[must_use]
    pub fn into_waiting_request(self) -> WaitingRequestId {
        self.0
    }
}

/// Side-specific constructors the Host gives to the harness at composition.
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::{HarnessApprovalValues, ReplyKind, WaitingRequestId};
/// fn forge(harness: &HarnessApprovalValues, wait: WaitingRequestId, reply: ReplyKind) {
///     harness.started_reply_submission(wait, reply);
/// }
/// ```
#[derive(Debug)]
pub struct HarnessApprovalValues {
    _private: (),
}

impl HarnessApprovalValues {
    /// Names an admitted ask using a fresh harness-allocated UUIDv7.
    ///
    /// Custody associates the name with its own approval holder run.
    ///
    /// ```compile_fail,E0308
    /// use approval_request_protocol::OfferView;
    /// use switchboard_approval_ports::{ApprovalScope, HarnessApprovalValues};
    /// fn bad(values: &HarnessApprovalValues, scope: ApprovalScope, offer: OfferView) {
    ///     values.admitted_ask(scope, 42_u64, offer);
    /// }
    /// ```
    #[must_use]
    pub fn admitted_ask(
        &self,
        scope: ApprovalScope,
        fresh_local_nonce: RequestId,
        offer: OfferView,
    ) -> ApprovalAsk {
        ApprovalAsk {
            scope,
            waiting_request: WaitingRequestId::from_admission(fresh_local_nonce),
            offer,
        }
    }

    /// Records the harness's outcome for the writer whose inline submission it received.
    #[must_use]
    pub const fn reply_submission_outcome(
        &self,
        kind: ReplySubmissionKind,
    ) -> ReplySubmissionOutcome {
        ReplySubmissionOutcome(kind)
    }

    /// Records what the harness observed when it tried to release the wait silently.
    #[must_use]
    pub const fn wait_release_outcome(&self, kind: WaitReleaseKind) -> WaitReleaseOutcome {
        WaitReleaseOutcome(kind)
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

    /// Reports the exact handle loss the harness proved before a reply started.
    #[must_use]
    pub const fn reply_handle_unavailable(
        &self,
        waiting_request: WaitingRequestId,
    ) -> ReplyHandleUnavailable {
        ReplyHandleUnavailable(waiting_request)
    }
}

/// Side-specific constructors the Host gives to the Mac custody service at composition.
///
/// ```compile_fail,E0599
/// use approval_request_protocol::OfferView;
/// use switchboard_approval_ports::{ApprovalScope, CustodyApprovalValues};
/// use workspace_ids::RequestId;
/// fn forge(custody: &CustodyApprovalValues, scope: ApprovalScope, nonce: RequestId,
///          offer: OfferView) {
///     custody.admitted_ask(scope, nonce, offer);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::{CustodyApprovalValues, WaitReleaseKind};
/// fn forge(custody: &CustodyApprovalValues) {
///     custody.wait_release_outcome(WaitReleaseKind::ReleasedWithoutReply);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_approval_ports::{CustodyApprovalValues, ReplySubmissionKind};
/// fn forge(custody: &CustodyApprovalValues) {
///     custody.reply_submission_outcome(ReplySubmissionKind::SlotGoneBeforeSubmission);
/// }
/// ```
#[derive(Debug)]
pub struct CustodyApprovalValues {
    _private: (),
}

impl CustodyApprovalValues {
    /// Forms the consuming request for the exact reply the holder has already started.
    #[must_use]
    pub const fn started_reply_submission(
        &self,
        waiting_request: WaitingRequestId,
        kind: ReplyKind,
    ) -> StartedReplySubmission {
        StartedReplySubmission {
            waiting_request,
            reply: ApprovalReply(kind),
        }
    }

    /// Requests unconditional revocation of the exact waiting binding.
    #[must_use]
    pub const fn revoke_wait_binding(
        &self,
        waiting_request: WaitingRequestId,
    ) -> WaitBindingRevocation {
        WaitBindingRevocation(waiting_request)
    }

    /// Requests silent disposal of an unstarted responder whose lookup has been revoked.
    #[must_use]
    pub const fn release_unstarted_wait(
        &self,
        waiting_request: WaitingRequestId,
    ) -> UnstartedWaitRelease {
        UnstartedWaitRelease(waiting_request)
    }
}

/// The Host obtains three constructor sets and gives each component only its own set.
///
/// The sets enforce envelope shape and document the Host's composition convention. The single
/// ordered custody executor enforces live authority. Rust lets any dependent call this factory.
#[must_use]
pub const fn approval_value_builders() -> (
    HarnessApprovalValues,
    CustodyApprovalValues,
    SessionOwnerApprovalValues,
) {
    (
        HarnessApprovalValues { _private: () },
        CustodyApprovalValues { _private: () },
        SessionOwnerApprovalValues::for_composition(),
    )
}

/// The ACP harness implements custody's inline submission and local release requests.
///
/// ```compile_fail,E0308
/// use switchboard_approval_ports::{ApprovalHarnessPort, ReplySubmissionOutcome, UnstartedWaitRelease};
/// fn wrong_return(port: &impl ApprovalHarnessPort, request: UnstartedWaitRelease) -> ReplySubmissionOutcome {
///     port.release_unstarted_wait(request)
/// }
/// ```
pub trait ApprovalHarnessPort {
    /// Consumes the started reply inline under the authority that orders stops, take-backs and run fences.
    ///
    /// Custody takes the final permit and commits the start under that same authority. This synchronous
    /// call neither awaits, queues its own work nor calls back into custody. It enqueues at the agent
    /// connection's hand-over point or reports that the exact slot was gone before submission.
    /// Only this started writer's outcome can become the holder's WriteFinished.
    fn submit_started_reply(&self, submission: StartedReplySubmission) -> ReplySubmissionOutcome;

    /// Removes the exact lookup even on a fence after start. The started writer can still report its outcome.
    fn revoke_wait_binding(&self, revocation: WaitBindingRevocation);

    /// Disposes of the exact unstarted responder after revocation, keeping any unresolved duty locally.
    fn release_unstarted_wait(&self, release: UnstartedWaitRelease) -> WaitReleaseOutcome;
}

/// The Mac custody service implements these immediate ingress calls from the harness.
///
/// ```compile_fail,E0308
/// use switchboard_approval_ports::{ApprovalAsk, ApprovalCustodyPort, LocalApprovalRefusal};
/// fn wrong_result(port: &impl ApprovalCustodyPort, ask: ApprovalAsk) -> Result<(), LocalApprovalRefusal> {
///     port.admit_ask(ask)
/// }
/// ```
pub trait ApprovalCustodyPort {
    /// Takes the checked ask and its sole reply right without waiting on central.
    ///
    /// An unbound or at-capacity ask transfers that right too. Custody sends its local refusal
    /// through the same permit, start and inline submission path.
    /// An unknown or retired binding is an unregistered-binding composition fault. Custody
    /// resolves a live binding before attempting the central session read.
    fn admit_ask(&self, ask: ApprovalAsk);

    /// Receives a proved terminal fact for the named waiting request.
    fn wait_ended(&self, event: MacWaitEnded);

    /// Under the same ordering authority, routes exact pre-start handle loss to the holder's
    /// CapabilityWithdrawn for a claimed or permitted responder.
    ///
    /// This releases the responder and preserves any independently owed Stop. It produces
    /// neither a wire stop cause nor a WriteFinished.
    fn reply_handle_unavailable(&self, observation: ReplyHandleUnavailable);
}
