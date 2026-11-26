//! A second harness used to drive the production session owner in tests.

use crate::{
    external_provider_approval_context::ExternalProviderApprovalContext,
    session_owner_lifecycle_reports::{OwnerHarnessPayloads, SessionHarnessBinding},
};
use provider_session_machines::{
    ProviderSettingKind, SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, ActivationSetup, ApprovalLease, ApprovalLeaseBinding,
        ApprovalLeaseRefusalReason, BeforeOfferApprovalLeaseRefusal, CatalogueFact,
        CatalogueRequest, CloseFact, CreateAttempt, CreatedSetupFact, CreatedSetupReport,
        ExactTurn, HandOverRefusal, HarnessContent, HarnessOwnerEvents, HarnessProvider,
        HarnessSessionId, Held, LifecycleRefusal, LifecycleReply, ObservedTurnApprovalLeaseRefusal,
        OpenAttempt, OpenRequest, OpeningFact, OwnerHarnessBinding, PermissionModeReport,
        RestoreLoadReadback, RestoreSettingRefusal, RestoredOpening, RunRetirement,
        SessionCreatedSetupFact, SessionLifecycleFact, SessionLifecyclePort, SettingAttempt,
        SettingFact, SetupReport, StreamTurn, WorkReply,
    },
    session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia},
    session_owner::{
        CancelDrainReport, CancelDrainStart, CancelDrainWait, DispatchAuthorization,
        DispatchResult, HarnessBinding, OwnerAddress, PromptCompletion, ReplyAdmission,
        ReservedWorkReply, RestoredMode, RunAttempt, SettingOffer, SteerIdlePolicy,
        TakeBackRefusal, TurnMembership, WireOutcome, WorkOfferRefusal,
    },
    session_setting_machine::SettingValueId,
    session_turn_machine::TurnId,
};
use session_event_model::{
    CapabilityReport, ProviderAuthStatus, SessionEvent, SessionItem, SessionItemKind,
    SessionSettings, SkillView, StopReason,
};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

pub(super) const SCRIPTED_SESSION: &str = "scripted-session";
pub(super) const REPLAY_TEXTS: [&str; 2] = ["first saved item", "second saved item"];
pub(super) const LIVE_TEXT: &str = "first live item";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ScriptObservation {
    Bound(OwnerAddress),
    CreateOpened(CreateAttempt),
    Configured(CreateAttempt),
    Replayed(RunAttempt<LoadAttempt>),
    Activated(ActivationSetup),
    SkillsReported,
    LiveReported,
    PromptTaken(DispatchAuthorization),
    Closed(RunAttempt<CloseAttempt>),
    Retired(SessionGeneration),
}

pub(super) struct ScriptedHarness {
    tasks: TaskTracker,
    journal: mpsc::UnboundedSender<ScriptObservation>,
}

impl ScriptedHarness {
    pub(super) fn new(
        tasks: TaskTracker,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<ScriptObservation>) {
        let (journal, observations) = mpsc::unbounded_channel();
        (Arc::new(Self { tasks, journal }), observations)
    }
}

impl HarnessProvider<OwnerHarnessPayloads> for ScriptedHarness {
    fn bind(
        &self,
        address: OwnerAddress,
        owner_events: Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>,
    ) -> SessionHarnessBinding {
        record(&self.journal, ScriptObservation::Bound(address));
        Box::new(ScriptedBinding {
            address,
            tasks: self.tasks.clone(),
            journal: self.journal.clone(),
            state: Arc::default(),
            _owner_events: owner_events,
        })
    }

    fn model_catalogue(
        &self,
        _request: CatalogueRequest,
        reply: LifecycleReply<CatalogueFact>,
    ) -> Result<(), LifecycleRefusal> {
        reply.send(CatalogueFact::Reported(Arc::default()));
        Ok(())
    }
}

struct ScriptedBinding {
    address: OwnerAddress,
    tasks: TaskTracker,
    journal: mpsc::UnboundedSender<ScriptObservation>,
    state: Arc<Mutex<BindingState>>,
    _owner_events: Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>,
}

#[derive(Default)]
struct BindingState {
    held: Option<OpenAttempt>,
    active: Option<SessionGeneration>,
    offered: Option<(DispatchAuthorization, ReservedWorkReply)>,
    taken: Option<DispatchAuthorization>,
}

impl OwnerHarnessBinding<OwnerHarnessPayloads> for ScriptedBinding {
    fn open(
        &mut self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        match request {
            OpenRequest::Create { owner, attempt, .. } if owner == self.address => {
                lock(&self.state).held = Some(OpenAttempt::Create(attempt));
                record(&self.journal, ScriptObservation::CreateOpened(attempt));
                reply.send(OpeningFact::Created {
                    attempt,
                    session: HarnessSessionId(SCRIPTED_SESSION.into()),
                });
            }
            OpenRequest::Restore {
                owner,
                run,
                session,
                via,
                ..
            } if owner == self.address && session.0 == SCRIPTED_SESSION => {
                let state = self.state.clone();
                let journal = self.journal.clone();
                self.tasks.spawn(async move {
                    let open = async {
                        let readback = RestoreLoadReadback {
                            catalogue: Arc::default(),
                            router_auto_requested: false,
                            mode: RestoredMode::Observed,
                            fast_on: None,
                        };
                        let opening = match via {
                            RestoreVia::Load => {
                                let replay = REPLAY_TEXTS.map(item_event).to_vec();
                                if events
                                    .send(SessionLifecycleFact::Replay {
                                        run,
                                        events: replay,
                                    })
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                record(&journal, ScriptObservation::Replayed(run));
                                RestoredOpening::Loaded {
                                    readback,
                                    accepted_mode_fallback: None,
                                }
                            }
                            RestoreVia::Resume => RestoredOpening::Resumed {
                                readback,
                                accepted_mode_fallback: None,
                            },
                        };
                        lock(&state).held = Some(OpenAttempt::Restore(run));
                        reply.send(OpeningFact::Restored { run, opening });
                    };
                    tokio::select! {
                        biased;
                        () = retired => {}
                        () = open => {}
                    }
                });
            }
            _ => return Err(LifecycleRefusal::AttemptMismatch),
        }
        Ok(())
    }

    fn configure_created(
        &self,
        attempt: CreateAttempt,
        reply: LifecycleReply<SessionCreatedSetupFact>,
        _retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        require_held(&self.state, OpenAttempt::Create(attempt))?;
        record(&self.journal, ScriptObservation::Configured(attempt));
        reply.send(CreatedSetupFact::Configured {
            attempt,
            report: Box::new(CreatedSetupReport {
                setup: setup_report(),
                catalogue: Arc::default(),
                effective_settings: Default::default(),
                permission_mode: PermissionModeReport::default(),
                router_auto_requested: false,
                keep_fast_off: false,
                failed_setting: None,
            }),
        });
        Ok(())
    }

    fn put_setting_back(
        &self,
        _attempt: SettingAttempt,
        _reserve: &mut dyn FnMut() -> ReplyAdmission<SettingFact>,
    ) -> Result<(), RestoreSettingRefusal> {
        Err(RestoreSettingRefusal::Harness(
            LifecycleRefusal::OperationNotOffered,
        ))
    }

    fn close(
        &self,
        run: RunAttempt<CloseAttempt>,
        reply: LifecycleReply<CloseFact>,
    ) -> Result<(), LifecycleRefusal> {
        if lock(&self.state).active != Some(run.generation) {
            return Err(LifecycleRefusal::RouteUnavailable);
        }
        record(&self.journal, ScriptObservation::Closed(run));
        reply.send(CloseFact::Closed { run });
        Ok(())
    }

    fn opening_is_held(&self, attempt: OpenAttempt) -> Held {
        match require_held(&self.state, attempt) {
            Ok(()) => Held::Held,
            Err(refusal) => Held::NotHeld(refusal),
        }
    }

    fn accept_settings(
        &self,
        attempt: OpenAttempt,
        _accepted: &AcceptedRunSettings,
    ) -> Result<SetupReport<CapabilityReport, SessionSettings>, LifecycleRefusal> {
        require_held(&self.state, attempt)?;
        Ok(setup_report())
    }

    fn activate(
        &self,
        setup: ActivationSetup,
        _accepted: AcceptedRunSettings,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) {
        let (attempt, run) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => (OpenAttempt::Create(attempt), generation),
            ActivationSetup::Restored(run) => (OpenAttempt::Restore(run), run.generation),
        };
        {
            let mut state = lock(&self.state);
            assert_eq!(state.held.take(), Some(attempt));
            state.active = Some(run);
        }
        record(&self.journal, ScriptObservation::Activated(setup));
        let journal = self.journal.clone();
        self.tasks.spawn(async move {
            let stream = async {
                let view = SkillView::Available {
                    skills: vec![],
                    errors: vec![],
                };
                if events
                    .send(SessionLifecycleFact::SkillView { run, view })
                    .await
                    .is_err()
                {
                    return;
                }
                record(&journal, ScriptObservation::SkillsReported);
                if events
                    .send(SessionLifecycleFact::Stream {
                        run,
                        turn: StreamTurn::Observed(None),
                        event: item_event(LIVE_TEXT),
                    })
                    .await
                    .is_ok()
                {
                    record(&journal, ScriptObservation::LiveReported);
                }
            };
            tokio::select! {
                biased;
                () = retired => {}
                () = stream => {}
            }
        });
    }

    fn use_settings(&self, _generation: SessionGeneration, _accepted: AcceptedRunSettings) {}

    fn admit_again(&self, _run: RunAttempt<CloseAttempt>) {}

    fn retire(&self, generation: SessionGeneration) {
        let mut state = lock(&self.state);
        if state.active == Some(generation) {
            state.active = None;
        }
        if matches!(state.held, Some(OpenAttempt::Restore(run)) if run.generation == generation) {
            state.held = None;
        }
        record(&self.journal, ScriptObservation::Retired(generation));
    }

    fn set_scope(&self, _change: TurnMembership) {}
}

impl HarnessBinding for ScriptedBinding {
    fn offer_prompt(
        &self,
        authorization: DispatchAuthorization,
        _turn: TurnId,
        _content: HarnessContent<'_>,
        reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        let mut state = lock(&self.state);
        if authorization.incarnation != self.address.incarnation
            || state.active != Some(authorization.run)
        {
            return Err(WorkOfferRefusal::RouteUnavailable);
        }
        if state.offered.is_some() {
            return Err(WorkOfferRefusal::OtherAuthorizationOffered);
        }
        state.offered = Some((authorization, reply));
        Ok(())
    }

    fn offer_steer(
        &self,
        _authorization: DispatchAuthorization,
        _content: HarnessContent<'_>,
        _idle: SteerIdlePolicy,
        _reply: ReservedWorkReply,
    ) -> Result<(), WorkOfferRefusal> {
        Err(WorkOfferRefusal::RouteUnavailable)
    }

    fn offer_setting(
        &self,
        _authorization: DispatchAuthorization,
        _kind: ProviderSettingKind,
        _value: SettingValueId,
        _reply: ReservedWorkReply,
    ) -> Result<SettingOffer, WorkOfferRefusal> {
        Err(WorkOfferRefusal::SettingSetterUnavailable)
    }

    fn hand_over(
        &self,
        authorization: &DispatchAuthorization,
        still_requested: &mut dyn FnMut() -> bool,
    ) -> Result<DispatchResult, HandOverRefusal> {
        let mut state = lock(&self.state);
        let Some((authorization, reply)) = state.offered.take_if(|offer| &offer.0 == authorization)
        else {
            return Err(HandOverRefusal::NoSuchOffer);
        };
        if !still_requested() {
            return Ok(DispatchResult::DefinitelyUnsent(authorization));
        }
        state.taken = Some(authorization.clone());
        record(
            &self.journal,
            ScriptObservation::PromptTaken(authorization.clone()),
        );
        reply.send(WorkReply::Dispatch(DispatchResult::PromptFinished {
            authorization: authorization.clone(),
            outcome: WireOutcome::Confirmed(PromptCompletion {
                stop_reason: StopReason::EndTurn,
                local_cause: None,
                response: "scripted answer".into(),
            }),
        }));
        Ok(DispatchResult::Taken(authorization))
    }

    fn take_back(
        &mut self,
        authorization: &DispatchAuthorization,
    ) -> Result<DispatchResult, TakeBackRefusal> {
        let mut state = lock(&self.state);
        if state.taken.as_ref() == Some(authorization) {
            return Ok(DispatchResult::AlreadyTaken(authorization.clone()));
        }
        state
            .offered
            .take_if(|offer| &offer.0 == authorization)
            .map(|(authorization, _)| DispatchResult::Revoked(authorization))
            .ok_or(TakeBackRefusal::NoRecord)
    }

    fn begin_cancel(
        &mut self,
        _authorization: DispatchAuthorization,
        _reserve: &mut dyn FnMut() -> ReplyAdmission<WorkReply>,
        _retired: RunRetirement,
    ) -> CancelDrainStart {
        CancelDrainStart::AlreadyDrained
    }

    fn send_cancel(
        &mut self,
        authorization: DispatchAuthorization,
        _wait: CancelDrainWait,
    ) -> DispatchResult {
        DispatchResult::CancelTaken {
            authorization,
            drain: CancelDrainReport::Drained,
        }
    }
}

struct ScriptedApprovalLease;
impl ApprovalLease for ScriptedApprovalLease {}

impl ApprovalLeaseBinding<ExternalProviderApprovalContext> for ScriptedBinding {
    fn install_approval_lease_before_offer(
        &self,
        turn: ExactTurn,
        _context: ExternalProviderApprovalContext,
    ) -> Result<Box<dyn ApprovalLease>, BeforeOfferApprovalLeaseRefusal> {
        self.install_lease(turn)
            .map_err(BeforeOfferApprovalLeaseRefusal)
    }

    fn install_approval_lease_for_observed_turn(
        &self,
        turn: ExactTurn,
        _context: ExternalProviderApprovalContext,
    ) -> Result<Box<dyn ApprovalLease>, ObservedTurnApprovalLeaseRefusal> {
        self.install_lease(turn)
            .map_err(ObservedTurnApprovalLeaseRefusal)
    }
}

impl ScriptedBinding {
    fn install_lease(
        &self,
        turn: ExactTurn,
    ) -> Result<Box<dyn ApprovalLease>, ApprovalLeaseRefusalReason> {
        if turn.run.incarnation == self.address.incarnation
            && lock(&self.state).active == Some(turn.run.generation)
        {
            Ok(Box::new(ScriptedApprovalLease))
        } else {
            Err(ApprovalLeaseRefusalReason::ContextUnavailable)
        }
    }
}

fn setup_report() -> SetupReport<CapabilityReport, SessionSettings> {
    SetupReport {
        capabilities: CapabilityReport {
            load: true,
            resume: true,
            close: true,
            ..Default::default()
        },
        settings: SessionSettings::default(),
        observed: Default::default(),
    }
}

fn item_event(text: &str) -> SessionEvent {
    SessionEvent::ItemStarted {
        item: SessionItem {
            item_id: Uuid::now_v7().to_string(),
            kind: SessionItemKind::AgentMessage,
            text: Some(text.into()),
        },
    }
}

fn require_held(state: &Mutex<BindingState>, attempt: OpenAttempt) -> Result<(), LifecycleRefusal> {
    if lock(state).held == Some(attempt) {
        Ok(())
    } else {
        Err(LifecycleRefusal::OpeningNotHeld)
    }
}

fn record(journal: &mpsc::UnboundedSender<ScriptObservation>, fact: ScriptObservation) {
    let _test_ended = journal.send(fact);
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().expect("scripted harness state")
}
