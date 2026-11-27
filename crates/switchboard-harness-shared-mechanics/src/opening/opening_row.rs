#![deny(clippy::wildcard_enum_match_arm)]

//! One owned step at the opening boundary.

#[path = "opening_effect.rs"]
mod effect;
#[path = "opening_step.rs"]
mod reducer;
#[cfg(test)]
#[path = "opening_row_test.rs"]
mod tests;

use super::{
    LifecycleRoute, OpeningAdapter, OpeningEffects, OpeningEvent as Edge, OpeningInstance,
    OpeningOffer, OpeningPhase, OpeningPolicy, SendRestoreDecision, StepFaultReport,
};
use crate::run_ledger::ReadmitOutcome;
use state_machine_core::StateMachine;
use switchboard_harness_contracts::{
    AcceptedRunSettings, CloseAttempt, CloseFact, CreateAttempt, HarnessSessionId,
    LifecycleRefusal, LifecycleReply, LoadAttempt, OpenAttempt, OpenSetup, OwnerAddress,
    OwnerSetting, RequestedProviderSettings, RestoreVia, RunAttempt, SessionGeneration,
    SettingAttempt, SettingFact, SetupReport, TransportRun,
};
use tokio_util::sync::CancellationToken;

pub struct OpeningRow<A: OpeningAdapter> {
    attempt: OpenAttempt,
    instance: OpeningInstance,
    state: OpeningState<A>,
}

enum OpeningState<A: OpeningAdapter> {
    Vacant {
        bound: bool,
    },
    OfferingCreate {
        offer: OpeningOffer,
        pending: PendingCreate,
    },
    AwaitingCreate(PendingCreate),
    HeldCreate(HeldCreate<A>),
    ConfiguringCreate(CancellationToken),
    ConfiguredCreate(Box<A::OpenedSession>),
    OfferingRestore {
        offer: OpeningOffer,
        run: RunResources,
    },
    AwaitingRestore(RunResources),
    PublishingReplay(RoutedRestore<A>),
    HeldRestore(RoutedRestore<A>),
    PreparedRestore(RoutedRestore<A>),
    StartingReader(RunResources),
    Active(RunResources),
    OfferingClose {
        origin: Box<OpeningState<A>>,
        attempt: CloseAttempt,
        offer: OpeningOffer,
        deferred: Option<DeferredRestore<A>>,
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

struct HeldCreate<A: OpeningAdapter> {
    pub(super) session: Box<A::OpenedSession>,
    pub(super) requested: Box<RequestedProviderSettings>,
}

#[derive(Clone)]
pub struct RunResources {
    pub route: LifecycleRoute,
    pub stopped: CancellationToken,
}

struct RoutedRestore<A: OpeningAdapter> {
    pub(super) run: RunResources,
    pub(super) held: Box<HeldRestore<A>>,
}

pub struct PendingSetter<A: OpeningAdapter> {
    claim: OpeningOffer,
    plan: Option<SetterPlan<A>>,
}

struct SetterPlan<A: OpeningAdapter> {
    restore: RunAttempt<LoadAttempt>,
    setting: OwnerSetting,
    wire: A::SettingPlan,
}

pub struct SetterWrite<A: OpeningAdapter> {
    pub route: LifecycleRoute,
    pub restore: RunAttempt<LoadAttempt>,
    pub setting: OwnerSetting,
    pub plan: A::SettingPlan,
    pub reply: LifecycleReply<SettingFact>,
}

pub struct HeldRestore<A: OpeningAdapter> {
    pub(super) session: Box<A::OpenedSession>,
    pub(super) accepted_mode_fallback: Option<String>,
    pub(super) skills: Option<A::Skills>,
    setters: Vec<PendingSetter<A>>,
}

pub struct DeferredRestore<A: OpeningAdapter> {
    pub(super) milestone: RestoreMilestone<A>,
    pub(super) outcome: A::RestoreSignal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreDecision {
    Continue,
    Superseded,
    LocallyUnavailable(RestoreVia),
    ReceiptMissing,
}

pub enum RestoreMilestone<A: OpeningAdapter> {
    Loaded(Box<HeldRestore<A>>),
    Resumed(Box<HeldRestore<A>>),
    ReplayAdmitted,
    ReplayRejected,
    Failed,
    WorkerExited,
}

impl<A: OpeningAdapter> RestoreMilestone<A> {
    pub fn held(
        session: A::OpenedSession,
        accepted_mode_fallback: Option<String>,
        skills: Option<A::Skills>,
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

    fn after_local_loss(&self, stage: RestoreReceiptStage) -> RestoreDecision {
        match self {
            Self::Loaded(_) => RestoreDecision::LocallyUnavailable(RestoreVia::Load),
            Self::Resumed(_) => RestoreDecision::LocallyUnavailable(RestoreVia::Resume),
            Self::Failed => RestoreDecision::Continue,
            Self::ReplayAdmitted | Self::ReplayRejected | Self::WorkerExited => match stage {
                RestoreReceiptStage::AwaitingPeer => RestoreDecision::ReceiptMissing,
                RestoreReceiptStage::PublishingLoad => {
                    RestoreDecision::LocallyUnavailable(RestoreVia::Load)
                }
            },
        }
    }
}
#[derive(Clone, Copy)]
enum LostReceipt {
    None,
    Open(RestoreReceiptStage),
    Close(CloseAttempt),
}

#[derive(Clone, Copy)]
pub(super) enum RestoreReceiptStage {
    AwaitingPeer,
    PublishingLoad,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ReaderEnd {
    Retired,
    Finished,
    Failed,
    IngressClosed,
    HandoffClosed,
}

pub(super) enum StoppedReceipt {
    OpenCreate(CancellationToken),
    OpenRestore(RunResources, RestoreReceiptStage),
    Configure(CancellationToken),
    Close(RunResources, CloseAttempt),
}

impl<A: OpeningAdapter> OpeningState<A> {
    fn pending_restore(&self, taken: bool) -> Option<RestoreReceiptStage> {
        match self {
            Self::OfferingRestore { .. } if taken => Some(RestoreReceiptStage::AwaitingPeer),
            Self::AwaitingRestore(_) => Some(RestoreReceiptStage::AwaitingPeer),
            Self::PublishingReplay(_) => Some(RestoreReceiptStage::PublishingLoad),
            Self::OfferingClose { origin, .. } if !taken => origin.pending_restore(false),
            Self::RouteLost {
                receipt: LostReceipt::Open(stage),
                ..
            }
            | Self::StoppedAwaitingReply(StoppedReceipt::OpenRestore(_, stage)) => Some(*stage),
            Self::Vacant { .. }
            | Self::OfferingCreate { .. }
            | Self::AwaitingCreate(_)
            | Self::HeldCreate(_)
            | Self::ConfiguringCreate(_)
            | Self::ConfiguredCreate(_)
            | Self::OfferingRestore { .. }
            | Self::HeldRestore(_)
            | Self::PreparedRestore(_)
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
                StoppedReceipt::OpenRestore(run, _) | StoppedReceipt::Close(run, _),
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

    fn held_session(&self) -> Option<&A::OpenedSession> {
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

pub enum Command<A: OpeningAdapter> {
    OpenCreate(Box<OpenCreateRequest>),
    OpenRestore(Box<OpenRestoreRequest>),
    WriterAccepted,
    WriterRefused,
    Created(Box<A::OpenedSession>),
    OpenFailed,
    WorkerExited,
    Configure(CancellationToken),
    Configured(Box<A::OpenedSession>),
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
    RestoreProgress(RestoreMilestone<A>, Option<A::RestoreSignal>),
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

pub struct RestoreOpening {
    pub run: RunAttempt<LoadAttempt>,
    pub session: HarnessSessionId,
    pub via: RestoreVia,
    pub setup: OpenSetup,
    pub requested_mode: Option<String>,
}

pub struct OpenCreateRequest {
    pub setup: OpenSetup,
    pub requested: RequestedProviderSettings,
    pub stopped: CancellationToken,
}

pub struct OpenRestoreRequest {
    pub(super) request: RestoreOpening,
    pub(super) owner: OwnerAddress,
    pub(super) stopped: CancellationToken,
}

pub struct CreateStart {
    pub setup: OpenSetup,
    pub offer: OpeningOffer,
    pub stopped: CancellationToken,
}

pub struct RestoreStart {
    pub request: RestoreOpening,
    pub offer: OpeningOffer,
    pub resources: RunResources,
}

pub struct ReaderHandoff<A: OpeningAdapter> {
    pub session: Box<A::OpenedSession>,
    pub skills: Option<A::Skills>,
    pub run: RunResources,
    pub install_route: bool,
}

pub enum Effect<A: OpeningAdapter> {
    OfferCreate(Box<CreateStart>),
    OfferRestore(Box<RestoreStart>),
    Fault(Box<StepFault<A>>),
    Discarded(Box<Command<A>>),
    DropSignal(A::RestoreSignal, Box<Effect<A>>),
    ReleaseBefore(Box<Effect<A>>, Box<Effect<A>>),
    DiscardAfter(Box<Command<A>>, Box<Effect<A>>),
    Readmit(LifecycleRoute, CloseAttempt),
    Rollback(Option<DeferredRestore<A>>),
    None,
    Configure {
        session: Box<A::OpenedSession>,
        requested: Box<RequestedProviderSettings>,
    },
    Settings(Box<SetupReport<A::Capabilities, A::Settings>>),
    RefuseSetter(LifecycleRefusal),
    ReserveSetter(OpeningOffer),
    WriteSetter(Box<SetterWrite<A>>),
    WithdrawSetters(Vec<PendingSetter<A>>, Box<Effect<A>>),
    StartReader(ReaderHandoff<A>),
    CloseOffer(LifecycleRoute, OpeningOffer),
    CloseConfirmed(Release<A>, Option<DeferredRestore<A>>),
    Deferred,
    ResolveDeferredRestore {
        deferred: DeferredRestore<A>,
        decision: RestoreDecision,
        release: Release<A>,
    },
    ResolveRestore {
        outcome: A::RestoreSignal,
        decision: RestoreDecision,
        release: Option<Release<A>>,
    },
    Release(Release<A>),
    ReaderEnded {
        release: Release<A>,
        report_loss: bool,
    },
}

pub struct StepFault<A: OpeningAdapter> {
    pub(super) phase: OpeningPhase,
    pub(super) edge: Edge,
    pub(super) declared: OpeningPhase,
    pub(super) actual: Option<OpeningPhase>,
    instance: OpeningInstance,
    previous_run: Option<RunResources>,
    previous_offer: Option<OpeningOffer>,
    rejected: Option<Release<A>>,
    effect: Effect<A>,
}

impl<A: OpeningAdapter> StepFault<A> {
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

    fn release(self, effects: &impl OpeningEffects) {
        effects.report_step_fault(&StepFaultReport {
            phase: self.phase,
            edge: self.edge,
            declared: self.declared,
            actual: self.actual,
        });
        if let Some(run) = self.previous_run {
            effects.retire_opening_route(&run.route, &self.instance);
            run.route.notify_removed();
            run.stopped.cancel();
        }
        if let Some(rejected) = self.rejected {
            rejected.run(effects);
        }
        drop(self.effect);
    }
}

pub struct Release<A: OpeningAdapter> {
    instance: OpeningInstance,
    state: Box<OpeningState<A>>,
    retire: bool,
}

impl<A: OpeningAdapter> Release<A> {
    #[cfg(test)]
    pub(super) fn is_for_test(&self, instance: &OpeningInstance) -> bool {
        self.instance.is_same_as(instance)
    }

    fn new(instance: &OpeningInstance, state: OpeningState<A>, retire: bool) -> Self {
        Self {
            instance: instance.clone(),
            state: Box::new(state),
            retire,
        }
    }

    fn run(self, effects: &impl OpeningEffects) {
        if self.retire {
            if let Some(run) = self.state.run() {
                effects.retire_opening_route(&run.route, &self.instance);
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
            deferred.outcome.send(RestoreDecision::Superseded);
        }
    }
}

pub struct Rejected<A: OpeningAdapter> {
    pub reason: LifecycleRefusal,
    // Keep receipt payloads outside the lock. begin_reader settles its fresh
    // activation resources inside the guard, before any reader exists.
    pub(super) _command: Box<Command<A>>,
}

pub(super) struct Refused<A: OpeningAdapter> {
    pub(super) row: Box<OpeningRow<A>>,
    pub(super) rejection: Rejected<A>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Slot {
    Create(CreateAttempt),
    Run(SessionGeneration),
}

impl Slot {
    pub fn for_attempt(attempt: OpenAttempt) -> Self {
        match attempt {
            OpenAttempt::Create(create) => Self::Create(create),
            OpenAttempt::Restore(restore) => Self::Run(restore.generation),
        }
    }
}

impl RunResources {
    pub fn new(
        owner: OwnerAddress,
        generation: SessionGeneration,
        stopped: &CancellationToken,
    ) -> Self {
        Self {
            route: LifecycleRoute::new(TransportRun {
                incarnation: owner.incarnation,
                generation,
            }),
            stopped: stopped.child_token(),
        }
    }
}

impl<A: OpeningAdapter> OpeningRow<A> {
    pub fn attempt(&self) -> OpenAttempt {
        self.attempt
    }

    pub fn instance(&self) -> &OpeningInstance {
        &self.instance
    }

    pub fn phase(&self) -> OpeningPhase {
        self.state.phase()
    }

    pub fn held(&self) -> bool {
        self.state.held_session().is_some()
    }

    pub fn removed_route(&self) -> Option<LifecycleRoute> {
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
        command: Command<A>,
    ) -> Result<(Option<Self>, Effect<A>), Refused<A>> {
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
            Command::WriterStopped | Command::ConnectionRetired => true,
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
            | Command::Retire => false,
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
