//! Provider discovery and the lifecycle of an owner's harness connections.

use std::sync::Arc;

use crate::{
    AcceptedRunSettings, ActivationRefusal, ActivationSetup, ApprovalLeaseBinding, CatalogueFact,
    CatalogueRequest, CloseAttempt, CloseFact, CreateAttempt, CreatedSetupFact, EventSendFuture,
    HarnessBinding, HarnessOwnerRecovery, HarnessSessionId, Held, LifecycleEventPort,
    LifecycleRefusal, LifecycleReply, OpenAttempt, OpenRequest, OpeningFact, OwnerAddress,
    ReplyAdmission, RestoreSettingRefusal, RunAttempt, RunRetirement, SessionGeneration,
    SettingAttempt, SettingFact, SetupReport, TurnId, TurnMembership,
};
#[cfg(feature = "test-support")]
use crate::{DispatchAuthorization, DispatchResult, HandOverRefusal};

#[cfg(test)]
#[path = "lifecycle_refusals_test.rs"]
mod refusal_tests;
#[cfg(test)]
#[path = "harness_lifecycle_test.rs"]
mod tests;

/// Session model types chosen by the controller. The harness supplies their values.
pub trait HarnessLifecyclePayloads: Send + Sync + 'static {
    type Event: Send + 'static;
    type SkillView: Send + 'static;
    type TurnOutcome: Send + 'static;
    type Capabilities: Send + 'static;
    type AuthStatus: Send + 'static;
    type Settings: Send + 'static;
    type SettingFailureReason: Send + 'static;
    type ApprovalContext: Send + 'static;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackAdmission {
    /// The controller enqueued the observation for its session owner.
    Admitted,
    /// The named session, generation or turn is no longer eligible.
    Ended,
    /// The controller could not enqueue the observation on its inline path.
    Unavailable,
}

/// The controller's immediate admission of lifecycle observations from harness callbacks.
///
/// Admission checks the controller's current state and uses its inline mailbox path.
/// A callback can act on the returned outcome before doing further work. Opening and
/// activation use the backpressured event port for replay and reader observations.
pub trait HarnessCallbackIngress<Payloads: HarnessLifecyclePayloads>:
    Send + Sync + 'static
{
    /// Capture the current generation when a callback starts. The controller checks it
    /// again when it receives the observation. A session without a current generation
    /// yields [`SessionGeneration::UNTRACKED`].
    fn current_generation(&self, session: &HarnessSessionId) -> SessionGeneration;

    /// Report a stream event observed for one generation. The controller determines its turn.
    fn report_for_generation(
        &self,
        session: &HarnessSessionId,
        run: SessionGeneration,
        event: Payloads::Event,
    ) -> CallbackAdmission;

    /// Report a stream event attributed by the harness to an exact run and turn.
    fn report_for_turn(
        &self,
        session: &HarnessSessionId,
        run: SessionGeneration,
        turn: TurnId,
        event: Payloads::Event,
    ) -> CallbackAdmission;

    /// Report an out-of-reader skill view using the controller's current generation.
    fn report_skill_view(
        &self,
        session: &HarnessSessionId,
        view: Payloads::SkillView,
    ) -> CallbackAdmission;
}

/// Reports about the whole owner, addressed before it has a run or a session ID.
pub trait HarnessOwnerEvents<AuthStatus>: Send + Sync + 'static {
    /// Report that the owner lost its harness. The report can await ingress capacity.
    fn report_harness_gone(&self) -> EventSendFuture<'_>;

    /// Report connection auth for the owner's current capability report on the inline path.
    fn report_connection_auth(&self, status: AuthStatus) -> CallbackAdmission;
}

pub trait HarnessProvider<Payloads: HarnessLifecyclePayloads>: HarnessOwnerRecovery {
    /// Create the owner's binding locally before any opening is offered.
    /// The caller binds `events` to this exact owner for the binding's lifetime.
    fn bind(
        &self,
        owner: OwnerAddress,
        events: Arc<dyn HarnessOwnerEvents<Payloads::AuthStatus>>,
    ) -> Box<dyn OwnerHarnessBinding<Payloads>>;

    /// Return the offered catalogue for the named scope, without inventing choices.
    fn model_catalogue(
        &self,
        request: CatalogueRequest,
        reply: LifecycleReply<CatalogueFact>,
    ) -> Result<(), LifecycleRefusal>;
}

/// A fixture's one deferred hand-over of an already offered authorization.
#[cfg(feature = "test-support")]
pub type DetachedTake = Box<dyn FnOnce() -> Result<DispatchResult, HandOverRefusal> + Send>;

/// The owner holds one binding for lifecycle and work across all of its generations.
pub trait OwnerHarnessBinding<Payloads: HarnessLifecyclePayloads>:
    HarnessBinding + ApprovalLeaseBinding<Payloads::ApprovalContext> + 'static
{
    /// Start one opening and keep its connection under the exact attempt. An immediate refusal
    /// means nothing was sent. Otherwise the opening settles through the reserved reply.
    /// The request must name the owner for which the binding was created.
    /// Before finishing a load, publish its captured replay through `events` and wait for ingress
    /// acceptance. `retired` can stop the opening or a backpressured replay. A resume skips replay.
    fn open(
        &mut self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: LifecycleEventPort<Payloads::Event, Payloads::SkillView, Payloads::TurnOutcome>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal>;

    /// Finish applying the held request after the controller has accepted the created session's
    /// ID. The harness reuses the settings it received at open, including any applied there.
    fn configure_created(
        &self,
        attempt: CreateAttempt,
        reply: LifecycleReply<
            CreatedSetupFact<
                Payloads::Capabilities,
                Payloads::Settings,
                Payloads::SettingFailureReason,
            >,
        >,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal>;

    /// Validate the setter and held restore attempt before asking for one reply reservation.
    /// A valid setter reaches the harness writer only after the reservation succeeds.
    fn put_setting_back(
        &self,
        attempt: SettingAttempt,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<SettingFact>,
    ) -> Result<(), RestoreSettingRefusal>;

    /// Ask the harness to end the session. An immediate refusal does not mark the route closing.
    fn close(
        &self,
        attempt: RunAttempt<CloseAttempt>,
        reply: LifecycleReply<CloseFact>,
    ) -> Result<(), LifecycleRefusal>;

    /// Check that the exact opening is still available before adopting its settings.
    fn opening_is_held(&self, attempt: OpenAttempt) -> Held;

    /// Accept the controller's catalogue for the held opening and report the observed setup.
    fn accept_settings(
        &self,
        attempt: OpenAttempt,
        accepted: &AcceptedRunSettings,
    ) -> Result<SetupReport<Payloads::Capabilities, Payloads::Settings>, LifecycleRefusal>;

    /// Admit callbacks and install the folder grant before publishing the held skill view or
    /// draining live events. The caller binds `events` and `retired` to this attempt. The harness
    /// owns the reader task, stops it on retirement and joins it under the harness task owner.
    /// A refusal starts no reader and leaves any already active run intact.
    fn activate(
        &self,
        setup: ActivationSetup,
        accepted: AcceptedRunSettings,
        events: LifecycleEventPort<Payloads::Event, Payloads::SkillView, Payloads::TurnOutcome>,
        retired: RunRetirement,
    ) -> Result<(), ActivationRefusal>;

    /// Publish an accepted setting snapshot for this generation only.
    fn use_settings(&self, generation: SessionGeneration, accepted: AcceptedRunSettings);

    /// Clear the closing marker only if it belongs to this exact refused close attempt.
    fn admit_again(&self, refused: RunAttempt<CloseAttempt>);

    /// Let go of a generation and its held state without reporting a remote close.
    fn retire(&self, generation: SessionGeneration);

    /// Update callback membership for the exact run and turn carried by the change.
    fn set_scope(&self, change: TurnMembership);

    /// Let a fixture attempt the exact hand-over after the owner consumes other events.
    /// Creating this handle leaves the existing offer in the harness.
    #[cfg(feature = "test-support")]
    fn detached_take_for_test(
        &self,
        _authorization: DispatchAuthorization,
    ) -> Option<DetachedTake> {
        None
    }
}
