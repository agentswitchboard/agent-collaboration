//! The binding's exact opening row owns every resource needed by its state.

use super::policy::{OpeningEvent, OpeningPhase, OpeningPolicy};
use crate::{
    RequestedProviderSettings,
    acp_transport::LifecycleRoute,
    owner_opening_instance::{OpeningInstance, OpeningOffer},
    owner_session_open::OwnerOpenedSession,
};
use provider_session_machines::{
    harness_lifecycle::{LifecycleRefusal, OpenAttempt},
    session_lifecycle_machine::CloseAttempt,
};
use session_event_model::SkillView;
use state_machine_core::StateMachine;
use tokio_util::sync::CancellationToken;

pub(super) struct OpeningRow {
    pub(super) attempt: OpenAttempt,
    pub(super) instance: OpeningInstance,
    pub(super) state: OpeningState,
}

pub(super) enum OpeningState {
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
    RouteLost {
        run: RunResources,
        close: Option<CloseAttempt>,
    },
    StoppedAwaitingReply(StoppedReceipt),
}

pub(super) struct PendingCreate {
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

pub(super) struct RoutedRestore {
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
    pub(super) fn event(&self) -> OpeningEvent {
        match self {
            Self::Loaded(_) => OpeningEvent::RestoreLoadReply,
            Self::Resumed(_) => OpeningEvent::RestoreResumeReply,
            Self::ReplayAdmitted => OpeningEvent::ReplayAdmitted,
            Self::Failed => OpeningEvent::OpenFailed,
            Self::WorkerExited => OpeningEvent::WorkerExited,
        }
    }
}
pub(super) enum StoppedReceipt {
    OpenCreate(CancellationToken),
    OpenRestore(RunResources),
    Configure(CancellationToken),
    Close(RunResources, CloseAttempt),
}

pub(super) enum Transition<Effects> {
    Store(OpeningState, Effects),
    Remove(Effects),
}

pub(super) struct Refused<Payload> {
    pub(super) row: OpeningRow,
    pub(super) reason: LifecycleRefusal,
    pub(super) payload: Payload,
}

impl OpeningRow {
    pub(super) fn phase(&self) -> OpeningPhase {
        self.state.phase()
    }

    pub(super) fn apply<Payload, Effects>(
        self,
        event: OpeningEvent,
        payload: Payload,
        apply: impl FnOnce(
            OpeningState,
            Payload,
        ) -> Result<Transition<Effects>, (OpeningState, Payload)>,
    ) -> Result<(Option<Self>, Effects), Refused<Payload>> {
        let phase = self.phase();
        let Ok((next, _)) = OpeningPolicy::transition(&phase, event) else {
            return Err(Refused {
                row: self,
                reason: phase.refusal(),
                payload,
            });
        };
        let Self {
            attempt,
            instance,
            state,
        } = self;
        match apply(state, payload) {
            Ok(Transition::Store(state, effects)) => {
                debug_assert_eq!(state.phase(), next);
                Ok((
                    Some(Self {
                        attempt,
                        instance,
                        state,
                    }),
                    effects,
                ))
            }
            Ok(Transition::Remove(effects)) => {
                debug_assert!(matches!(
                    next,
                    OpeningPhase::Terminal | OpeningPhase::RetiredAwaitingReply
                ));
                Ok((None, effects))
            }
            Err((state, payload)) => Err(Refused {
                row: Self {
                    attempt,
                    instance,
                    state,
                },
                reason: phase.refusal(),
                payload,
            }),
        }
    }
}

impl OpeningState {
    pub(super) fn phase(&self) -> OpeningPhase {
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
            Self::StoppedAwaitingReply(_) => OpeningPhase::StoppedAwaitingReply,
        }
    }

    pub(super) fn run(&self) -> Option<&RunResources> {
        match self {
            Self::OfferingRestore { run, .. }
            | Self::AwaitingRestore(run)
            | Self::StartingReader(run)
            | Self::Active(run)
            | Self::ClosingRestore { run, .. }
            | Self::ClosingLive { run, .. }
            | Self::CloseRefusedLive { run, .. }
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

    pub(super) fn offer(&self) -> Option<&OpeningOffer> {
        match self {
            Self::OfferingRestore { offer, .. }
            | Self::OfferingCreate { offer, .. }
            | Self::OfferingClose { offer, .. } => Some(offer),
            _ => None,
        }
    }

    pub(super) fn mark_retired(&self) {
        if let Some(offer) = self.offer() {
            let _was_taken = offer.was_taken_or_withdraw();
        }
        if let Some(run) = self.run() {
            run.route.mark_removed();
        }
    }
}
