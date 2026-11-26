//! A second harness used to drive the production session owner in tests.

#[path = "session_owner_scripted_harness_loss_test_support.rs"]
mod loss;
#[path = "session_owner_scripted_harness_work_test_support.rs"]
mod work;
pub(super) use loss::PolicySignal;

use crate::{
    acp_interaction_port::HostRequestPayloads,
    external_provider_approval_context::ExternalProviderApprovalContext,
    session_owner_lifecycle_reports::{OwnerHarnessPayloads, SessionHarnessBinding},
};
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, ActivationSetup, ApprovalLease, ApprovalLeaseBinding,
        ApprovalLeaseRefusalReason, BeforeOfferApprovalLeaseRefusal, CatalogueFact,
        CatalogueRequest, CloseFact, CreateAttempt, CreatedSetupFact, CreatedSetupReport,
        ExactTurn, HarnessOwnerEvents, HarnessOwnerRecovery, HarnessProvider, HarnessSessionId,
        Held, LifecycleRefusal, LifecycleReply, ObservedTurnApprovalLeaseRefusal, OpenAttempt,
        OpenRequest, OpeningFact, OpeningFailure, OwnerHarnessBinding, PermissionModeReport,
        RestoreLoadReadback, RestoreSettingRefusal, RestoredOpening, RunRetirement,
        SessionCreatedSetupFact, SessionLifecycleFact, SessionLifecyclePort, SettingAttempt,
        SettingFact, SetupReport, SlotEvidence, StreamTurn,
    },
    session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia},
    session_owner::{
        DispatchAuthorization, OwnerAddress, OwnerEvidence, OwnerIncarnation, OwnerInputEvidence,
        ReplyAdmission, ReservedWorkReply, RestoredMode, RunAttempt, TurnMembership,
    },
};
use session_event_model::{
    CapabilityReport, InputId, ProviderAuthStatus, SessionEvent, SessionItem, SessionItemKind,
    SessionSettings, SkillView,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard},
};
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
    ReplayLost(RunAttempt<LoadAttempt>),
    Activated(ActivationSetup),
    SkillsReported,
    LiveReported,
    PromptOffered(DispatchAuthorization),
    PromptHeld(DispatchAuthorization),
    PromptTaken(DispatchAuthorization),
    SteerTaken(
        DispatchAuthorization,
        provider_session_machines::session_turn_machine::TurnId,
    ),
    ObservedLeaseRefused {
        turn: ExactTurn,
        operation: collaboration_protocol::OperationId,
    },
    OwnerGivenUp {
        incarnation: OwnerIncarnation,
        asked: BTreeSet<InputId>,
        evidence: OwnerEvidence,
    },
    Closed(RunAttempt<CloseAttempt>),
    Retired(SessionGeneration),
}

pub(super) struct ScriptedHarness {
    tasks: TaskTracker,
    journal: mpsc::UnboundedSender<ScriptObservation>,
    replay: ReplayCapture,
    owners: Mutex<BTreeMap<OwnerIncarnation, Arc<Mutex<BindingState>>>>,
    controls: ScriptControls,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum ScriptedWork {
    #[default]
    Complete,
    UnavailableRecovery,
    HoldPromptReply,
    RefuseObservedLease,
}

#[derive(Clone, Default)]
pub(super) struct ScriptControls {
    pub(super) work: ScriptedWork,
    pub(super) policy: Option<
        Arc<dyn provider_session_machines::session_owner::RequestPolicy<HostRequestPayloads>>,
    >,
}

#[derive(Clone, Copy)]
pub(super) enum ReplayCapture {
    Complete,
    Interrupted,
}

impl ReplayCapture {
    fn capture(self) -> Result<Vec<SessionEvent>, OpeningFailure> {
        REPLAY_TEXTS
            .into_iter()
            .enumerate()
            .map(|(index, text)| {
                if index == 1 && matches!(self, Self::Interrupted) {
                    Err(OpeningFailure::ReplayNotKnown)
                } else {
                    Ok(item_event(text))
                }
            })
            .collect()
    }
}

impl ScriptedHarness {
    pub(super) fn new(
        tasks: TaskTracker,
        replay: ReplayCapture,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<ScriptObservation>) {
        Self::with_controls(tasks, replay, ScriptControls::default())
    }

    pub(super) fn with_controls(
        tasks: TaskTracker,
        replay: ReplayCapture,
        controls: ScriptControls,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<ScriptObservation>) {
        let (journal, observations) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                tasks,
                journal,
                replay,
                owners: Mutex::default(),
                controls,
            }),
            observations,
        )
    }

    pub(super) fn owner_events(
        &self,
        owner: OwnerIncarnation,
    ) -> Option<Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>> {
        let state = lock(&self.owners).get(&owner)?.clone();
        let events = lock(&state).owner_events.clone();
        events
    }

    pub(super) fn take_prompt_reply(
        &self,
        owner: OwnerIncarnation,
    ) -> Option<(DispatchAuthorization, ReservedWorkReply)> {
        let state = lock(&self.owners).get(&owner)?.clone();
        let reply = lock(&state).prompt_reply.take();
        reply
    }

    pub(super) fn finish_prompt(&self, owner: OwnerIncarnation) -> Option<DispatchAuthorization> {
        let (authorization, reply) = self.take_prompt_reply(owner)?;
        work::finish_prompt(authorization.clone(), reply);
        Some(authorization)
    }

    pub(super) fn run_events(&self, owner: OwnerIncarnation) -> Option<SessionLifecyclePort> {
        let state = lock(&self.owners).get(&owner)?.clone();
        let events = lock(&state).run_events.clone();
        events
    }
}

impl HarnessOwnerRecovery for ScriptedHarness {
    fn give_up_owner(
        &self,
        incarnation: OwnerIncarnation,
        inputs: &BTreeSet<InputId>,
    ) -> OwnerEvidence {
        let owner = lock(&self.owners).remove(&incarnation);
        let Some(owner) = owner else {
            return inputs
                .iter()
                .map(|input| {
                    (
                        input.clone(),
                        OwnerInputEvidence::NoEvidence { slots: vec![] },
                    )
                })
                .collect();
        };
        let mut state = lock(&owner);
        state.held = None;
        state.active = None;
        state.offered = None;
        for (_, evidence) in &mut state.dispatches {
            if *evidence == SlotEvidence::Offered {
                *evidence = SlotEvidence::Revoked;
            }
        }
        let evidence: OwnerEvidence = inputs
            .iter()
            .map(|input| {
                let slots: Vec<_> = state
                    .dispatches
                    .iter()
                    .filter(|(authorization, _)| authorization.input_id == *input)
                    .cloned()
                    .collect();
                let evidence = if self.controls.work == ScriptedWork::UnavailableRecovery {
                    OwnerInputEvidence::NoEvidence { slots: Vec::new() }
                } else if slots.iter().any(|(_, slot)| *slot == SlotEvidence::Taken) {
                    OwnerInputEvidence::Taken { slots }
                } else if slots.is_empty() {
                    OwnerInputEvidence::NeverOffered
                } else {
                    OwnerInputEvidence::Withdrawn { slots }
                };
                (input.clone(), evidence)
            })
            .collect();
        if self.controls.work != ScriptedWork::Complete {
            record(
                &self.journal,
                ScriptObservation::OwnerGivenUp {
                    incarnation,
                    asked: inputs.clone(),
                    evidence: evidence.clone(),
                },
            );
        }
        evidence
    }
}

impl HarnessProvider<OwnerHarnessPayloads> for ScriptedHarness {
    fn bind(
        &self,
        address: OwnerAddress,
        owner_events: Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>,
    ) -> SessionHarnessBinding {
        record(&self.journal, ScriptObservation::Bound(address));
        let state = Arc::new(Mutex::new(BindingState {
            owner_events: Some(owner_events),
            ..BindingState::default()
        }));
        lock(&self.owners).insert(address.incarnation, state.clone());
        Box::new(ScriptedBinding {
            address,
            tasks: self.tasks.clone(),
            journal: self.journal.clone(),
            state,
            replay: self.replay,
            controls: self.controls.clone(),
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
    replay: ReplayCapture,
    controls: ScriptControls,
}

#[derive(Default)]
struct BindingState {
    held: Option<OpenAttempt>,
    active: Option<SessionGeneration>,
    offered: Option<(DispatchAuthorization, ReservedWorkReply)>,
    dispatches: Vec<(DispatchAuthorization, SlotEvidence)>,
    owner_events: Option<Arc<dyn HarnessOwnerEvents<ProviderAuthStatus>>>,
    run_events: Option<SessionLifecyclePort>,
    prompt_reply: Option<(DispatchAuthorization, ReservedWorkReply)>,
    approval: Option<(ExactTurn, ExternalProviderApprovalContext)>,
    pending_request: Option<loss::PendingRequest>,
}

impl BindingState {
    fn record_slot(&mut self, authorization: &DispatchAuthorization, slot: SlotEvidence) {
        if let Some((_, current)) = self
            .dispatches
            .iter_mut()
            .find(|(recorded, _)| recorded == authorization)
        {
            *current = slot;
        } else {
            self.dispatches.push((authorization.clone(), slot));
        }
    }
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
                let capture = self.replay;
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
                                let replay = match capture.capture() {
                                    Ok(replay) => replay,
                                    Err(failure) => {
                                        record(&journal, ScriptObservation::ReplayLost(run));
                                        reply.send(OpeningFact::RestoreFailed { run, failure });
                                        return;
                                    }
                                };
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
            state.run_events = Some(events.clone());
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

struct ScriptedApprovalLease;
impl ApprovalLease for ScriptedApprovalLease {}

impl ApprovalLeaseBinding<ExternalProviderApprovalContext> for ScriptedBinding {
    fn install_approval_lease_before_offer(
        &self,
        turn: ExactTurn,
        mut context: ExternalProviderApprovalContext,
    ) -> Result<Box<dyn ApprovalLease>, BeforeOfferApprovalLeaseRefusal> {
        let lease = self
            .install_lease(turn)
            .map_err(BeforeOfferApprovalLeaseRefusal)?;
        if let Some(policy) = &self.controls.policy {
            policy.bind_owner_scope(&mut context, turn);
            lock(&self.state).approval = Some((turn, context));
        }
        Ok(lease)
    }

    fn install_approval_lease_for_observed_turn(
        &self,
        turn: ExactTurn,
        context: ExternalProviderApprovalContext,
    ) -> Result<Box<dyn ApprovalLease>, ObservedTurnApprovalLeaseRefusal> {
        if self.controls.work == ScriptedWork::RefuseObservedLease {
            record(
                &self.journal,
                ScriptObservation::ObservedLeaseRefused {
                    turn,
                    operation: context.operation_id,
                },
            );
            return Err(ObservedTurnApprovalLeaseRefusal(
                ApprovalLeaseRefusalReason::AlreadyActive,
            ));
        }
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
