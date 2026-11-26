//! The controller's payloads and ports for a session harness.

use crate::{
    ProviderSettingsCatalog,
    session_owner::{OwnerEvent, RestorePreparation, SessionWireResult, WireOutcome},
};
use session_event_model::{
    CapabilityReport, ProviderAuthStatus, ProviderSettingFailureReason, SessionEvent,
    SessionSettings, SkillView, TurnOutcome,
};
use std::marker::PhantomData;
use std::sync::Arc;

pub use switchboard_harness_contracts::{
    AUTO_PERMISSION_MODE, AcceptedRunSettings, ActivationRefusal, ActivationSetup, ApprovalLease,
    ApprovalLeaseBinding, ApprovalLeaseRefusalReason, BeforeOfferApprovalLeaseRefusal,
    CallbackAdmission, CatalogueFact, CatalogueRequest, CloseFact, CreateAttempt,
    CreatedSettingsFailure, CreatedSetupFact, CreatedSetupReport, CreationEffect, EventPort,
    EventRefusal, EventSendFuture, ExactTurn, FastRequest, HandOverRefusal, HarnessCallbackIngress,
    HarnessContent, HarnessLifecyclePayloads, HarnessOwnerEvents, HarnessOwnerRecovery,
    HarnessProvider, HarnessSessionId, Held, HeldOpeningKind, InvalidRequestedSetting,
    LifecycleEventPort, LifecycleFact, LifecycleMove, LifecycleRefusal, LifecycleReply,
    ObservedTurnApprovalLeaseRefusal, OpenAttempt, OpenRequest, OpenSetup, OpeningFact,
    OpeningFailure, OwnerHarnessBinding, PermissionModeReport, ReplayCompleteness,
    RequestedProviderSettings, ReservedReply, RestoreLoadReadback, RestoreSettingRefusal,
    RestoredOpening, RunRetirement, SessionWorkRoots, SettingAttempt, SettingFact, SetupReport,
    SlotEvidence, StreamTurn, TransportRun, WorkReply,
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

#[must_use]
pub fn owner_event_for_close(fact: CloseFact) -> OwnerEvent {
    let (run, outcome) = match fact {
        CloseFact::Closed { run } => (run, WireOutcome::Confirmed(())),
        CloseFact::RefusedStillLive { run } => (run, WireOutcome::Refused),
        CloseFact::NotOffered { run } => {
            return OwnerEvent::Wire(SessionWireResult::CloseNotSent(run));
        }
        CloseFact::Unknown { run, .. } => (run, WireOutcome::Unknown),
    };
    OwnerEvent::Wire(SessionWireResult::Close { run, outcome })
}

#[must_use]
pub fn owner_event_for_restore_setting(fact: SettingFact) -> OwnerEvent {
    let catalogue = |record| Arc::new(ProviderSettingsCatalog::from_record(record));
    match fact {
        SettingFact::Mode {
            restore: run,
            setting,
            outcome,
            catalogue: record,
        } => OwnerEvent::Wire(SessionWireResult::RestoreMode {
            run,
            setting,
            outcome,
            catalogue: record.map(catalogue),
        }),
        SettingFact::FastOff {
            restore: run,
            setting,
            observation,
            catalogue: record,
        } => OwnerEvent::Prepare(RestorePreparation::FastOff {
            run,
            setting,
            observation,
            catalogue: record.map(catalogue),
        }),
    }
}
