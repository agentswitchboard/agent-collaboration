#![deny(clippy::wildcard_enum_match_arm)]

//! One owned step at the ACP opening boundary.

#[path = "owner_acp_opening_step.rs"]
mod reducer;

use super::policy::{OpeningEvent as Edge, OpeningPhase, OpeningPolicy};
use super::{OpenSetup, RestoreOpening};
use crate::{
    RequestedProviderSettings,
    acp_transport::{LifecycleRoute, ReadmitOutcome},
    owner_opening_instance::{OpeningInstance, OpeningOffer},
    owner_session_open::OwnerOpenedSession,
    owner_session_setting::OwnerSettingWirePlan,
    owner_wire::OwnerAcpWire,
};
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, CloseFact, LifecycleRefusal, LifecycleReply, OpenAttempt,
        SettingAttempt, SettingFact, SetupReport,
    },
    session_lifecycle_machine::{CloseAttempt, LoadAttempt},
    session_owner::{OwnerAddress, OwnerSetting, RunAttempt},
};
use session_event_model::{CapabilityReport, SessionSettings, SkillView};
use state_machine_core::StateMachine;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub(super) struct OpeningRow {
    attempt: OpenAttempt,
    instance: OpeningInstance,
    state: OpeningState,
}

enum OpeningState {
    Vacant {
        bound: bool,
    },
    OfferingCreate {
        offer: OpeningOffer,
        pending: PendingCreate,
    },
    AwaitingCreate(PendingCreate),
    HeldCreate(HeldCreate),
    ConfiguringCreate(CancellationToken),
    ConfiguredCreate(Box<OwnerOpenedSession>),
    OfferingRestore {
        offer: OpeningOffer,
        run: RunResources,
    },
    AwaitingRestore(RunResources),
    PublishingReplay(RoutedRestore),
    HeldRestore(RoutedRestore),
    PreparedRestore(RoutedRestore),
    StartingReader(RunResources),
    Active(RunResources),
    OfferingClose {
        origin: Box<OpeningState>,
        attempt: CloseAttempt,
        offer: OpeningOffer,
        deferred: Option<DeferredRestore>,
    },
    ClosingRestore {
        run: RunResources,
        attempt: CloseAttempt,
    },
    ClosingLive {
        run: RunResources,
        attempt: CloseAttempt,
    },
    CloseRefusedLive {
        run: RunResources,
        attempt: CloseAttempt,
    },
    Readmitting {
        run: RunResources,
        attempt: CloseAttempt,
    },
    RouteLost {
        run: RunResources,
        receipt: LostReceipt,
    },
    StoppedAwaitingReply(StoppedReceipt),
}

struct PendingCreate {
    pub(super) requested: Box<RequestedProviderSettings>,
    pub(super) stopped: CancellationToken,
}

struct HeldCreate {
    pub(super) session: Box<OwnerOpenedSession>,
    pub(super) requested: Box<RequestedProviderSettings>,
}

#[derive(Clone)]
pub(super) struct RunResources {
    pub(super) route: LifecycleRoute,
    pub(super) stopped: CancellationToken,
}

struct RoutedRestore {
    pub(super) run: RunResources,
    pub(super) held: Box<HeldRestore>,
}

pub(super) struct PendingSetter {
    claim: OpeningOffer,
    plan: Option<SetterPlan>,
}

struct SetterPlan {
    restore: RunAttempt<LoadAttempt>,
    setting: OwnerSetting,
    wire: OwnerSettingWirePlan,
}

pub(super) struct SetterWrite {
    pub(super) route: LifecycleRoute,
    pub(super) restore: RunAttempt<LoadAttempt>,
    pub(super) setting: OwnerSetting,
    pub(super) plan: OwnerSettingWirePlan,
    pub(super) reply: LifecycleReply<SettingFact>,
}

pub(super) struct HeldRestore {
    pub(super) session: Box<OwnerOpenedSession>,
    pub(super) accepted_mode_fallback: Option<String>,
    pub(super) skills: Option<SkillView>,
    setters: Vec<PendingSetter>,
}

pub(super) struct DeferredRestore {
    pub(super) milestone: RestoreMilestone,
    pub(super) outcome: tokio::sync::oneshot::Sender<bool>,
}

pub(super) enum RestoreMilestone {
    Loaded(Box<HeldRestore>),
    Resumed(Box<HeldRestore>),
    ReplayAdmitted,
    ReplayRejected,
    Failed,
    WorkerExited,
}

impl RestoreMilestone {
    pub(super) fn held(
        session: OwnerOpenedSession,
        accepted_mode_fallback: Option<String>,
        skills: Option<SkillView>,
        from_load: bool,
    ) -> Self {
        let held = Box::new(HeldRestore {
            session: Box::new(session),
            accepted_mode_fallback,
            skills,
            setters: Vec::new(),
        });
        if from_load {
            Self::Loaded(held)
        } else {
            Self::Resumed(held)
        }
    }

    pub(super) fn event(&self) -> Edge {
        match self {
            Self::Loaded(_) => Edge::RestoreLoadReply,
            Self::Resumed(_) => Edge::RestoreResumeReply,
            Self::ReplayAdmitted => Edge::ReplayAdmitted,
            Self::ReplayRejected => Edge::ReplayRejected,
            Self::Failed => Edge::OpenFailed,
            Self::WorkerExited => Edge::WorkerExited,
        }
    }
}
#[derive(Clone, Copy)]
enum LostReceipt {
    None,
    Open,
    Close(CloseAttempt),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ReaderEnd {
    Retired,
    Finished,
    Failed,
    IngressClosed,
    HandoffClosed,
}

pub(super) enum StoppedReceipt {
    OpenCreate(CancellationToken),
    OpenRestore(RunResources),
    Configure(CancellationToken),
    Close(RunResources, CloseAttempt),
}

impl OpeningState {
    fn live_reader(&self) -> Option<&RunResources> {
        match self {
            Self::Active(run)
            | Self::ClosingLive { run, .. }
            | Self::CloseRefusedLive { run, .. }
            | Self::Readmitting { run, .. } => Some(run),
            Self::OfferingClose { origin, .. } => origin.live_reader(),
            Self::Vacant { .. }
            | Self::OfferingCreate { .. }
            | Self::AwaitingCreate(_)
            | Self::HeldCreate(_)
            | Self::ConfiguringCreate(_)
            | Self::ConfiguredCreate(_)
            | Self::OfferingRestore { .. }
            | Self::AwaitingRestore(_)
            | Self::PublishingReplay(_)
            | Self::HeldRestore(_)
            | Self::PreparedRestore(_)
            | Self::StartingReader(_)
            | Self::ClosingRestore { .. }
            | Self::RouteLost { .. }
            | Self::StoppedAwaitingReply(_) => None,
        }
    }

    fn phase(&self) -> OpeningPhase {
        match self {
            Self::Vacant { bound: false } => OpeningPhase::UnboundIdle,
            Self::Vacant { bound: true } => OpeningPhase::BoundIdle,
            Self::OfferingCreate { .. } => OpeningPhase::QueuedCreate,
            Self::AwaitingCreate(_) => OpeningPhase::AwaitingCreate,
            Self::HeldCreate(_) => OpeningPhase::HeldCreate,
            Self::ConfiguringCreate(_) => OpeningPhase::ConfiguringCreate,
            Self::ConfiguredCreate(_) => OpeningPhase::ConfiguredCreate,
            Self::OfferingRestore { .. } => OpeningPhase::QueuedRestore,
            Self::AwaitingRestore(_) => OpeningPhase::AwaitingRestore,
            Self::PublishingReplay(_) => OpeningPhase::CapturingReplay,
            Self::HeldRestore(_) => OpeningPhase::HeldRestore,
            Self::PreparedRestore(_) => OpeningPhase::PreparedRestore,
            Self::StartingReader(_) => OpeningPhase::StartingReader,
            Self::Active(_) => OpeningPhase::Active,
            Self::OfferingClose { .. } => OpeningPhase::OfferingClose,
            Self::ClosingRestore { .. } => OpeningPhase::ClosingRestore,
            Self::ClosingLive { .. } => OpeningPhase::ClosingLive,
            Self::CloseRefusedLive { .. } => OpeningPhase::CloseRefusedLive,
            Self::RouteLost { .. } => OpeningPhase::RouteLost,
            Self::Readmitting { .. } => OpeningPhase::Readmitting,
            Self::StoppedAwaitingReply(_) => OpeningPhase::StoppedAwaitingReply,
        }
    }

    fn run(&self) -> Option<&RunResources> {
        match self {
            Self::OfferingRestore { run, .. }
            | Self::AwaitingRestore(run)
            | Self::StartingReader(run)
            | Self::Active(run)
            | Self::ClosingRestore { run, .. }
            | Self::ClosingLive { run, .. }
            | Self::CloseRefusedLive { run, .. }
            | Self::Readmitting { run, .. }
            | Self::RouteLost { run, .. } => Some(run),
            Self::PublishingReplay(restore)
            | Self::HeldRestore(restore)
            | Self::PreparedRestore(restore) => Some(&restore.run),
            Self::OfferingClose { origin, .. } => origin.run(),
            Self::Vacant { .. }
            | Self::OfferingCreate { .. }
            | Self::AwaitingCreate(_)
            | Self::HeldCreate(_)
            | Self::ConfiguringCreate(_)
            | Self::ConfiguredCreate(_)
            | Self::StoppedAwaitingReply(
                StoppedReceipt::OpenCreate(_) | StoppedReceipt::Configure(_),
            ) => None,
            Self::StoppedAwaitingReply(
                StoppedReceipt::OpenRestore(run) | StoppedReceipt::Close(run, _),
            ) => Some(run),
        }
    }

    fn offer(&self) -> Option<&OpeningOffer> {
        match self {
            Self::OfferingRestore { offer, .. }
            | Self::OfferingCreate { offer, .. }
            | Self::OfferingClose { offer, .. } => Some(offer),
            Self::Vacant { .. }
            | Self::AwaitingCreate(_)
            | Self::HeldCreate(_)
            | Self::ConfiguringCreate(_)
            | Self::ConfiguredCreate(_)
            | Self::AwaitingRestore(_)
            | Self::PublishingReplay(_)
            | Self::HeldRestore(_)
            | Self::PreparedRestore(_)
            | Self::StartingReader(_)
            | Self::Active(_)
            | Self::ClosingRestore { .. }
            | Self::ClosingLive { .. }
            | Self::CloseRefusedLive { .. }
            | Self::Readmitting { .. }
            | Self::RouteLost { .. }
            | Self::StoppedAwaitingReply(_) => None,
        }
    }

    fn held_session(&self) -> Option<&OwnerOpenedSession> {
        match self {
            Self::HeldCreate(held) => Some(&held.session),
            Self::ConfiguredCreate(session) => Some(session),
            Self::HeldRestore(restore) | Self::PreparedRestore(restore) => {
                Some(&restore.held.session)
            }
            Self::Vacant { .. }
            | Self::OfferingCreate { .. }
            | Self::AwaitingCreate(_)
            | Self::ConfiguringCreate(_)
            | Self::OfferingRestore { .. }
            | Self::AwaitingRestore(_)
            | Self::PublishingReplay(_)
            | Self::StartingReader(_)
            | Self::Active(_)
            | Self::OfferingClose { .. }
            | Self::ClosingRestore { .. }
            | Self::ClosingLive { .. }
            | Self::CloseRefusedLive { .. }
            | Self::Readmitting { .. }
            | Self::RouteLost { .. }
            | Self::StoppedAwaitingReply(_) => None,
        }
    }

    fn mark_retired(&self) {
        if let Some(offer) = self.offer() {
            let _was_taken = offer.was_taken_or_withdraw();
        }
        if let Some(run) = self.run() {
            run.route.mark_removed();
        }
        if let Self::HeldRestore(restore) = self {
            for setter in &restore.held.setters {
                let _taken = setter.claim.was_taken_or_withdraw();
            }
        }
    }
}

pub(super) enum Command {
    OpenCreate(Box<OpenCreateRequest>),
    OpenRestore(Box<OpenRestoreRequest>),
    WriterAccepted,
    WriterRefused,
    Created(Box<OwnerOpenedSession>),
    OpenFailed,
    WorkerExited,
    Configure(CancellationToken),
    Configured(Box<OwnerOpenedSession>),
    ConfigureClosed,
    AcceptSettings(AcceptedRunSettings),
    PutSettingBack {
        attempt: Box<SettingAttempt>,
        session: String,
    },
    SetterReserved(OpeningOffer, LifecycleReply<SettingFact>),
    SetterDone(OpeningOffer),
    Activate {
        run: RunResources,
        created: bool,
    },
    ActivationFailed,
    ReaderAccepted,
    ReaderStopped(ReaderEnd),
    RestoreProgress(RestoreMilestone, Option<oneshot::Sender<bool>>),
    CloseAsked(RunAttempt<CloseAttempt>),
    CloseOffered(RunAttempt<CloseAttempt>, bool),
    CloseSettled(CloseFact),
    AdmitAgain(CloseAttempt),
    ReadmitResult(CloseAttempt, ReadmitOutcome),
    Retire,
    WriterStopped,
    ConnectionRetired,
    RouteRemoved(LifecycleRoute),
}

pub(super) struct OpenCreateRequest {
    pub(super) setup: OpenSetup,
    pub(super) requested: RequestedProviderSettings,
    pub(super) stopped: CancellationToken,
}

pub(super) struct OpenRestoreRequest {
    pub(super) request: RestoreOpening,
    pub(super) owner: OwnerAddress,
    pub(super) stopped: CancellationToken,
}

pub(super) struct CreateStart {
    pub(super) setup: OpenSetup,
    pub(super) offer: OpeningOffer,
    pub(super) stopped: CancellationToken,
}

pub(super) struct RestoreStart {
    pub(super) request: RestoreOpening,
    pub(super) offer: OpeningOffer,
    pub(super) resources: RunResources,
}

pub(super) struct ReaderHandoff {
    pub(super) session: Box<OwnerOpenedSession>,
    pub(super) skills: Option<SkillView>,
    pub(super) run: RunResources,
    pub(super) install_route: bool,
}

pub(super) enum Effect {
    OfferCreate(Box<CreateStart>),
    OfferRestore(Box<RestoreStart>),
    Fault(Box<StepFault>),
    Discarded(Box<Command>),
    DropSignal(oneshot::Sender<bool>, Box<Effect>),
    ReleaseBefore(Box<Effect>, Box<Effect>),
    DiscardAfter(Box<Command>, Box<Effect>),
    Readmit(LifecycleRoute, CloseAttempt),
    Rollback(Option<DeferredRestore>),
    None,
    Configure {
        session: Box<OwnerOpenedSession>,
        requested: Box<RequestedProviderSettings>,
    },
    Settings(Box<SetupReport<CapabilityReport, SessionSettings>>),
    RefuseSetter(LifecycleRefusal),
    ReserveSetter(OpeningOffer),
    WriteSetter(Box<SetterWrite>),
    WithdrawSetters(Vec<PendingSetter>, Box<Effect>),
    StartReader(ReaderHandoff),
    CloseOffer(LifecycleRoute, OpeningOffer),
    CloseConfirmed(Release, Option<oneshot::Sender<bool>>),
    Deferred,
    ResolveRestore {
        outcome: oneshot::Sender<bool>,
        accepted: bool,
        release: Option<Release>,
    },
    Release(Release),
    ReaderEnded {
        release: Release,
        report_loss: bool,
    },
}

pub(super) struct StepFault {
    pub(super) phase: OpeningPhase,
    pub(super) edge: Edge,
    pub(super) declared: OpeningPhase,
    pub(super) actual: Option<OpeningPhase>,
    instance: OpeningInstance,
    previous_run: Option<RunResources>,
    previous_offer: Option<OpeningOffer>,
    rejected: Option<Release>,
    effect: Effect,
}

impl StepFault {
    fn fence(&self) {
        if let Some(offer) = &self.previous_offer {
            let _taken = offer.was_taken_or_withdraw();
        }
        if let Some(run) = &self.previous_run {
            run.route.mark_removed();
        }
        if let Some(rejected) = &self.rejected {
            rejected.state.mark_retired();
        }
    }

    fn release(self, wire: &OwnerAcpWire) {
        tracing::error!(phase=?self.phase, edge=?self.edge, declared=?self.declared,
            actual=?self.actual, "ACP opening step returned an undeclared outcome");
        if let Some(run) = self.previous_run {
            wire.retire_opening_route(&run.route, &self.instance);
            run.route.notify_removed();
            run.stopped.cancel();
        }
        if let Some(rejected) = self.rejected {
            rejected.run(wire);
        }
        drop(self.effect);
    }
}

pub(super) struct Release {
    instance: OpeningInstance,
    state: Box<OpeningState>,
    retire: bool,
}

impl Release {
    fn new(instance: &OpeningInstance, state: OpeningState, retire: bool) -> Self {
        Self {
            instance: instance.clone(),
            state: Box::new(state),
            retire,
        }
    }

    pub(super) fn run(self, wire: &OwnerAcpWire) {
        if self.retire {
            if let Some(run) = self.state.run() {
                wire.retire_opening_route(&run.route, &self.instance);
                run.route.notify_removed();
                run.stopped.cancel();
            }
            match &*self.state {
                OpeningState::OfferingCreate { pending, .. }
                | OpeningState::AwaitingCreate(pending) => pending.stopped.cancel(),
                OpeningState::ConfiguringCreate(stopped)
                | OpeningState::StoppedAwaitingReply(
                    StoppedReceipt::OpenCreate(stopped) | StoppedReceipt::Configure(stopped),
                ) => stopped.cancel(),
                OpeningState::Vacant { .. }
                | OpeningState::HeldCreate(_)
                | OpeningState::ConfiguredCreate(_)
                | OpeningState::OfferingRestore { .. }
                | OpeningState::AwaitingRestore(_)
                | OpeningState::PublishingReplay(_)
                | OpeningState::HeldRestore(_)
                | OpeningState::PreparedRestore(_)
                | OpeningState::StartingReader(_)
                | OpeningState::Active(_)
                | OpeningState::OfferingClose { .. }
                | OpeningState::ClosingRestore { .. }
                | OpeningState::ClosingLive { .. }
                | OpeningState::CloseRefusedLive { .. }
                | OpeningState::Readmitting { .. }
                | OpeningState::RouteLost { .. }
                | OpeningState::StoppedAwaitingReply(_) => {}
            }
        }
        if let OpeningState::OfferingClose {
            deferred: Some(deferred),
            ..
        } = *self.state
        {
            let _worker_gone = deferred.outcome.send(false);
        }
    }
}

pub(super) struct Rejected {
    pub(super) reason: LifecycleRefusal,
    // A rejected receipt may own a reader. Keep its drop outside the lock.
    pub(super) _command: Box<Command>,
}

pub(super) struct Refused {
    pub(super) row: Box<OpeningRow>,
    pub(super) rejection: Rejected,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Slot {
    Create(provider_session_machines::harness_lifecycle::CreateAttempt),
    Run(SessionGeneration),
}

impl Slot {
    pub(super) fn for_attempt(attempt: OpenAttempt) -> Self {
        match attempt {
            OpenAttempt::Create(create) => Self::Create(create),
            OpenAttempt::Restore(restore) => Self::Run(restore.generation),
        }
    }
}

impl RunResources {
    pub(super) fn new(
        owner: OwnerAddress,
        generation: SessionGeneration,
        stopped: &CancellationToken,
    ) -> Self {
        Self {
            route: LifecycleRoute::new(super::transport_run(owner, generation)),
            stopped: stopped.child_token(),
        }
    }
}

impl OpeningRow {
    pub(super) fn attempt(&self) -> OpenAttempt {
        self.attempt
    }

    pub(super) fn instance(&self) -> &OpeningInstance {
        &self.instance
    }

    pub(super) fn phase(&self) -> OpeningPhase {
        self.state.phase()
    }

    pub(super) fn held(&self) -> bool {
        self.state.held_session().is_some()
    }

    pub(super) fn removed_route(&self) -> Option<LifecycleRoute> {
        let route = &self.state.run()?.route;
        (route.is_removed() && self.phase() != OpeningPhase::RouteLost).then(|| route.clone())
    }

    pub(super) fn vacant(attempt: OpenAttempt, instance: OpeningInstance, bound: bool) -> Self {
        Self {
            attempt,
            instance,
            state: OpeningState::Vacant { bound },
        }
    }

    pub(super) fn is_vacant(&self) -> bool {
        matches!(self.state, OpeningState::Vacant { .. })
    }

    pub(super) fn apply(
        self,
        instance: &OpeningInstance,
        command: Command,
    ) -> Result<(Option<Self>, Effect), Refused> {
        let reason = self.phase().refusal();
        if !self.instance.is_same_as(instance) {
            return Err(Refused {
                row: Box::new(self),
                rejection: Rejected {
                    reason: LifecycleRefusal::AttemptMismatch,
                    _command: Box::new(command),
                },
            });
        }
        let Self {
            attempt,
            instance,
            state,
        } = self;
        let seal_offer = match &command {
            Command::WriterStopped => true,
            Command::ReaderStopped(_) => state.live_reader().is_some(),
            Command::RouteRemoved(route) => {
                state.run().is_some_and(|run| run.route.is_same_as(route))
            }
            Command::OpenCreate(_)
            | Command::OpenRestore(_)
            | Command::WriterAccepted
            | Command::WriterRefused
            | Command::Created(_)
            | Command::OpenFailed
            | Command::WorkerExited
            | Command::Configure(_)
            | Command::Configured(_)
            | Command::ConfigureClosed
            | Command::AcceptSettings(_)
            | Command::PutSettingBack { .. }
            | Command::SetterReserved(..)
            | Command::SetterDone(_)
            | Command::Activate { .. }
            | Command::ActivationFailed
            | Command::ReaderAccepted
            | Command::RestoreProgress(..)
            | Command::CloseAsked(_)
            | Command::CloseOffered(..)
            | Command::CloseSettled(_)
            | Command::AdmitAgain(_)
            | Command::ReadmitResult(..)
            | Command::Retire
            | Command::ConnectionRetired => false,
        };
        let taken = seal_offer
            && state
                .offer()
                .is_some_and(OpeningOffer::was_taken_or_withdraw);
        match reducer::step(state, command, &instance, taken) {
            Ok((next, effect)) => Ok((
                next.map(|state| Self {
                    attempt,
                    instance,
                    state,
                }),
                effect,
            )),
            Err((state, command)) => Err(Refused {
                row: Box::new(Self {
                    attempt,
                    instance,
                    state,
                }),
                rejection: Rejected {
                    reason,
                    _command: Box::new(command),
                },
            }),
        }
    }
}

impl Effect {
    pub(super) fn faulted(&self) -> bool {
        match self {
            Self::Fault(_) => true,
            Self::DropSignal(_, next) | Self::DiscardAfter(_, next) => next.faulted(),
            Self::OfferCreate(_)
            | Self::OfferRestore(_)
            | Self::Discarded(_)
            | Self::ReleaseBefore(..)
            | Self::Readmit(..)
            | Self::Rollback(_)
            | Self::None
            | Self::Configure { .. }
            | Self::Settings(_)
            | Self::RefuseSetter(_)
            | Self::ReserveSetter(_)
            | Self::WriteSetter(_)
            | Self::WithdrawSetters(..)
            | Self::StartReader(_)
            | Self::CloseOffer(..)
            | Self::CloseConfirmed(..)
            | Self::Deferred
            | Self::ResolveRestore { .. }
            | Self::Release(_)
            | Self::ReaderEnded { .. } => false,
        }
    }

    pub(super) fn fence(&self) {
        match self {
            Self::WithdrawSetters(setters, next) => {
                for setter in setters {
                    let _taken = setter.claim.was_taken_or_withdraw();
                }
                next.fence();
            }
            Self::Fault(fault) => fault.fence(),
            Self::ReaderEnded { release, .. }
            | Self::Release(release)
            | Self::ResolveRestore {
                release: Some(release),
                ..
            } if release.retire => release.state.mark_retired(),
            Self::DropSignal(_, next)
            | Self::DiscardAfter(_, next)
            | Self::ReleaseBefore(_, next) => next.fence(),
            Self::OfferCreate(_)
            | Self::OfferRestore(_)
            | Self::Discarded(_)
            | Self::Readmit(..)
            | Self::Rollback(_)
            | Self::None
            | Self::Configure { .. }
            | Self::Settings(_)
            | Self::RefuseSetter(_)
            | Self::ReserveSetter(_)
            | Self::WriteSetter(_)
            | Self::StartReader(_)
            | Self::CloseOffer(..)
            | Self::CloseConfirmed(..)
            | Self::Deferred
            | Self::ResolveRestore { .. }
            | Self::Release(_)
            | Self::ReaderEnded { .. } => {}
        }
    }

    pub(super) fn finish(self, wire: &OwnerAcpWire) -> bool {
        match self {
            Self::WithdrawSetters(setters, next) => {
                drop(setters);
                next.finish(wire)
            }
            Self::Fault(fault) => {
                fault.release(wire);
                false
            }
            Self::None | Self::Rollback(None) => true,
            Self::ReaderEnded { release, .. } | Self::Release(release) => {
                release.run(wire);
                true
            }
            Self::CloseConfirmed(release, deferred) => {
                release.run(wire);
                if let Some(deferred) = deferred {
                    let _worker_gone = deferred.send(false);
                }
                true
            }
            Self::ResolveRestore {
                outcome,
                accepted,
                release,
            } => {
                if let Some(release) = release {
                    release.run(wire);
                }
                let _worker_gone = outcome.send(accepted);
                true
            }
            Self::DropSignal(signal, next) => {
                drop(signal);
                next.finish(wire)
            }
            Self::Discarded(command) => {
                drop(command);
                false
            }
            Self::DiscardAfter(command, next) => {
                drop(command);
                next.finish(wire)
            }
            Self::ReleaseBefore(previous, next) => {
                drop(previous);
                next.finish(wire)
            }
            Self::Readmit(_, _) => false,
            Self::Deferred
            | Self::Rollback(Some(_))
            | Self::Configure { .. }
            | Self::Settings(_)
            | Self::RefuseSetter(_)
            | Self::ReserveSetter(_)
            | Self::WriteSetter(_)
            | Self::StartReader(_)
            | Self::CloseOffer(_, _)
            | Self::OfferCreate(_)
            | Self::OfferRestore(_) => false,
        }
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_row_test.rs"]
mod tests;
