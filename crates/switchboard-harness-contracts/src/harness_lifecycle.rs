//! Provider discovery and the lifecycle of one held harness connection.

use std::sync::Arc;

use crate::{
    AcceptedRunSettings, ActivationSetup, CatalogueFact, CatalogueRequest, CloseAttempt, CloseFact,
    CreateAttempt, CreatedSetupFact, HarnessBinding, Held, LifecycleEventPort, LifecycleRefusal,
    LifecycleReply, OpenAttempt, OpenRequest, OpeningFact, RunAttempt, RunRetirement,
    SessionGeneration, SettingAttempt, SettingFact, SetupReport, TurnMembership,
};

/// Session model types chosen by the controller. The harness supplies their values.
pub trait HarnessLifecyclePayloads: Send + Sync + 'static {
    type Event: Send + 'static;
    type SkillView: Send + 'static;
    type TurnOutcome: Send + 'static;
    type Capabilities: Send + 'static;
    type AuthStatus: Send + 'static;
    type Settings: Send + 'static;
    type SettingFailureReason: Send + 'static;
}

pub trait HarnessProvider<Payloads: HarnessLifecyclePayloads>: Send + Sync + 'static {
    /// Start one opening and keep its connection under the exact attempt. An immediate refusal
    /// means nothing was sent. Otherwise the opening settles through the reserved reply.
    /// Before finishing a load, publish its complete replay through `events` and wait for ingress
    /// acceptance. `retired` can stop the opening or a backpressured replay. A resume skips replay.
    fn open(
        &self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: LifecycleEventPort<
            Payloads::Event,
            Payloads::SkillView,
            Payloads::TurnOutcome,
            Payloads::AuthStatus,
        >,
        retired: RunRetirement,
    ) -> Result<Arc<dyn HarnessLifecycleBinding<Payloads>>, LifecycleRefusal>;

    /// Return the offered catalogue for the named scope, without inventing choices.
    fn model_catalogue(
        &self,
        request: CatalogueRequest,
        reply: LifecycleReply<CatalogueFact>,
    ) -> Result<(), LifecycleRefusal>;
}

/// The lifecycle and work surfaces refer to the same harness-owned connection.
pub trait HarnessLifecycleBinding<Payloads: HarnessLifecyclePayloads>:
    HarnessBinding + 'static
{
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

    /// Reapply one setting for the exact restore attempt and return its own observation.
    fn put_setting_back(
        &self,
        attempt: SettingAttempt,
        reply: LifecycleReply<SettingFact>,
    ) -> Result<(), LifecycleRefusal>;

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
    fn activate(
        &self,
        setup: ActivationSetup,
        accepted: AcceptedRunSettings,
        events: LifecycleEventPort<
            Payloads::Event,
            Payloads::SkillView,
            Payloads::TurnOutcome,
            Payloads::AuthStatus,
        >,
        retired: RunRetirement,
    );

    /// Publish an accepted setting snapshot for this generation only.
    fn use_settings(&self, generation: SessionGeneration, accepted: AcceptedRunSettings);

    /// Clear the closing marker only if it belongs to this exact refused close attempt.
    fn admit_again(&self, refused: RunAttempt<CloseAttempt>);

    /// Let go of a generation and its held state without reporting a remote close.
    fn retire(&self, generation: SessionGeneration);

    /// Update callback membership for the exact run and turn carried by the change.
    fn set_scope(&self, change: TurnMembership);
}
