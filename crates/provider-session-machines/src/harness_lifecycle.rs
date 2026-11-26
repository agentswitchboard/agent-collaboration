//! The controller's payloads and ports for a session harness.

use session_event_model::{
    CapabilityReport, ProviderAuthStatus, ProviderSettingFailureReason, SessionEvent,
    SessionSettings, SkillView, TurnOutcome,
};
use std::marker::PhantomData;

pub use switchboard_harness_contracts::{
    AcceptedRunSettings, ActivationSetup, CallbackAdmission, CatalogueFact, CatalogueRequest,
    CloseFact, CreateAttempt, CreatedSettingsFailure, CreatedSetupFact, CreatedSetupReport,
    CreationEffect, EventPort, EventRefusal, EventSendFuture, HarnessCallbackIngress,
    HarnessLifecyclePayloads, HarnessOwnerEvents, HarnessProvider, HarnessSessionId, Held,
    LifecycleEventPort, LifecycleFact, LifecycleRefusal, LifecycleReply, OpenAttempt, OpenRequest,
    OpenSetup, OpeningFact, OpeningFailure, OwnerHarnessBinding, RestoreLoadReadback,
    RestoredOpening, RunRetirement, SettingAttempt, SettingFact, SetupReport, StreamTurn,
};

pub struct SessionHarnessPayloads<ApprovalContext>(PhantomData<fn() -> ApprovalContext>);

impl<ApprovalContext: Send + 'static> HarnessLifecyclePayloads
    for SessionHarnessPayloads<ApprovalContext>
{
    type Event = SessionEvent;
    type SkillView = SkillView;
    type TurnOutcome = TurnOutcome;
    type Capabilities = CapabilityReport;
    type AuthStatus = ProviderAuthStatus;
    type Settings = SessionSettings;
    type SettingFailureReason = ProviderSettingFailureReason;
    type ApprovalContext = ApprovalContext;
}

pub type SessionLifecycleFact = LifecycleFact<SessionEvent, SkillView, TurnOutcome>;
pub type SessionLifecyclePort = LifecycleEventPort<SessionEvent, SkillView, TurnOutcome>;
pub type SessionCreatedSetupFact =
    CreatedSetupFact<CapabilityReport, SessionSettings, ProviderSettingFailureReason>;
