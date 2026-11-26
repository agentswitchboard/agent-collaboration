//! One owned step at the ACP opening boundary.

#[path = "owner_acp_opening_step.rs"]
mod reducer;

use super::policy::{OpeningEvent as Edge, OpeningPhase, OpeningPolicy};
use crate::{
    RequestedProviderSettings,
    acp_transport::LifecycleRoute,
    owner_opening_instance::{OpeningInstance, OpeningOffer},
    owner_session_open::OwnerOpenedSession,
    owner_wire::OwnerAcpWire,
};
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, CloseFact, LifecycleRefusal, OpenAttempt, SetupReport,
    },
    session_lifecycle_machine::CloseAttempt,
    session_owner::{OwnerAddress, RunAttempt},
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
        close: Option<CloseAttempt>,
    },
    StoppedAwaitingReply(StoppedReceipt),
}

struct PendingCreate {
    pub(super) requested: Box<RequestedProviderSettings>,
    pub(super) stopped: CancellationToken,
}

pub(super) struct HeldCreate {
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

pub(super) struct HeldRestore {
    pub(super) session: Box<OwnerOpenedSession>,
    pub(super) accepted_mode_fallback: Option<String>,
    pub(super) skills: Option<SkillView>,
}

pub(super) struct DeferredRestore {
    pub(super) milestone: RestoreMilestone,
    pub(super) outcome: tokio::sync::oneshot::Sender<bool>,
}

pub(super) enum RestoreMilestone {
    Loaded(Box<HeldRestore>),
    Resumed(Box<HeldRestore>),
    ReplayAdmitted,
    Failed,
    WorkerExited,
}

impl RestoreMilestone {
    pub(super) fn event(&self) -> Edge {
        match self {
            Self::Loaded(_) => Edge::RestoreLoadReply,
            Self::Resumed(_) => Edge::RestoreResumeReply,
            Self::ReplayAdmitted => Edge::ReplayAdmitted,
            Self::Failed => Edge::OpenFailed,
            Self::WorkerExited => Edge::WorkerExited,
        }
    }
}
pub(super) enum StoppedReceipt {
    OpenCreate(CancellationToken),
    OpenRestore(RunResources),
    Configure(CancellationToken),
    Close(RunResources, CloseAttempt),
}

impl OpeningState {
    fn phase(&self) -> OpeningPhase {
        match self {
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
            Self::OfferingCreate { .. }
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
            _ => None,
        }
    }

    fn mark_retired(&self) {
        if let Some(offer) = self.offer() {
            let _was_taken = offer.was_taken_or_withdraw();
        }
        if let Some(run) = self.run() {
            run.route.mark_removed();
        }
    }
}

pub(super) enum Command {
    WriterAccepted,
    WriterRefused,
    Created(Box<OwnerOpenedSession>),
    OpenFailed,
    WorkerExited,
    Configure(CancellationToken),
    Configured(Box<OwnerOpenedSession>),
    ConfigureClosed,
    AcceptSettings(AcceptedRunSettings),
    Activate { run: RunResources, created: bool },
    ActivationFailed,
    ReaderAccepted,
    RestoreProgress(RestoreMilestone, Option<oneshot::Sender<bool>>),
    CloseAsked(RunAttempt<CloseAttempt>),
    CloseOffered(RunAttempt<CloseAttempt>, bool),
    CloseSettled(CloseFact),
    AdmitAgain(CloseAttempt),
    ReadmitResult(CloseAttempt, bool),
    Retire,
    WriterStopped,
    ConnectionRetired,
    RouteRemoved(LifecycleRoute),
}

pub(super) struct ReaderHandoff {
    pub(super) session: Box<OwnerOpenedSession>,
    pub(super) skills: Option<SkillView>,
    pub(super) run: RunResources,
    pub(super) install_route: bool,
}

pub(super) enum Effect {
    Discarded(Box<Command>),
    DropSignal(oneshot::Sender<bool>, Box<Effect>),
    ReleaseBefore(Box<Effect>, Box<Effect>),
    DiscardAfter(Box<Command>, Box<Effect>),
    Readmit(LifecycleRoute, CloseAttempt),
    Rollback(Option<DeferredRestore>),
    None,
    Configure(HeldCreate),
    Settings(Box<SetupReport<CapabilityReport, SessionSettings>>),
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
                _ => {}
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
        matches!(
            self.state,
            OpeningState::HeldCreate(_)
                | OpeningState::ConfiguredCreate(_)
                | OpeningState::HeldRestore(_)
                | OpeningState::PreparedRestore(_)
        )
    }

    pub(super) fn removed_route(&self) -> Option<LifecycleRoute> {
        let route = &self.state.run()?.route;
        (route.is_removed() && self.phase() != OpeningPhase::RouteLost).then(|| route.clone())
    }

    pub(super) fn held_restore_offer_data(&self) -> Option<(LifecycleRoute, Option<String>)> {
        let OpeningState::HeldRestore(restore) = &self.state else {
            return None;
        };
        Some((
            restore.run.route.clone(),
            restore.held.accepted_mode_fallback.clone(),
        ))
    }

    pub(super) fn create(
        attempt: OpenAttempt,
        instance: OpeningInstance,
        offer: OpeningOffer,
        requested: RequestedProviderSettings,
        stopped: CancellationToken,
    ) -> Self {
        Self {
            attempt,
            instance,
            state: OpeningState::OfferingCreate {
                offer,
                pending: PendingCreate {
                    requested: Box::new(requested),
                    stopped,
                },
            },
        }
    }

    pub(super) fn restore(
        attempt: OpenAttempt,
        instance: OpeningInstance,
        offer: OpeningOffer,
        run: RunResources,
    ) -> Self {
        Self {
            attempt,
            instance,
            state: OpeningState::OfferingRestore { offer, run },
        }
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
            Command::RouteRemoved(route) => {
                state.run().is_some_and(|run| run.route.is_same_as(route))
            }
            _ => false,
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
    pub(super) fn fence(&self) {
        match self {
            Self::Release(release)
            | Self::ResolveRestore {
                release: Some(release),
                ..
            } if release.retire => release.state.mark_retired(),
            Self::DropSignal(_, next)
            | Self::DiscardAfter(_, next)
            | Self::ReleaseBefore(_, next) => next.fence(),
            _ => {}
        }
    }

    pub(super) fn finish(self, wire: &OwnerAcpWire) -> bool {
        match self {
            Self::None | Self::Rollback(None) => true,
            Self::Release(release) => {
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
            Self::Readmit(route, close) => wire.reopen_opening_route(&route, close),
            Self::Deferred
            | Self::Rollback(Some(_))
            | Self::Configure(_)
            | Self::Settings(_)
            | Self::StartReader(_)
            | Self::CloseOffer(_, _) => false,
        }
    }
}

#[cfg(test)]
#[path = "owner_acp_opening_row_test.rs"]
mod tests;
