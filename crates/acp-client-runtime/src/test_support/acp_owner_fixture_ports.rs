//! Execute fixture effects with the production ACP writer and stream reader.

use super::{FixtureState, lock};
use crate::{
    AcpRequestPayloads, OwnerApprovalBindings, OwnerApprovalLease,
    ProviderPromptDispatchObservation, SessionEventSink,
    acp_transport::TransportRun,
    owner_acp_binding::OwnerAcpBinding,
    owner_session_open::{OwnerOpenedSession, OwnerSessionOpenSetup},
    owner_session_setting::OwnerSettingWirePlan,
    owner_session_stream::{OwnerStreamTarget, ReplayedSessionFacts},
    owner_wire::OwnerAcpWire,
    session_owner_task::{OwnerCompletionPort, OwnerTaskError, SessionOwnerPorts},
};
use futures_util::FutureExt as _;
use provider_session_machines::session_owner::{
    ActivationRefusal, AuthorizedWork, CreateProgress, CreatedSessionFacts, DispatchAuthorization,
    DispatchResult, EffectOutcome, ObservationCause, ObservedSetting, OwnerAddress, OwnerEffect,
    OwnerEvent, OwnerInput, OwnerSetting, PreparationStage, PublicationBatch, PublicationFact,
    PublicationResult, RemoteEffect, RestoreFailure, RestoreLoadReadback, RestorePreparation,
    RunAttempt, SessionOwnerReply, SessionPublication, SessionStoreRequest, SessionStoreResult,
    SessionWireRequest, SessionWireResult, SettingEvidence, SteerOrigin, TurnMembership,
    WireOutcome,
};
use provider_session_machines::{
    ProviderSettingsCatalog, SessionGeneration,
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_turn_machine::TurnId,
};
use session_event_model::{SessionEvent, SessionItem, SessionItemKind, SessionState, SkillView};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};
use switchboard_harness_contracts::{
    CancelDrainStart, CancelDrainWait, CancelReservationRefusal, HarnessBinding, RefusedWorkMove,
    ReplyAdmission, ReservedWorkReply, SettingOffer, SteerIdlePolicy, WriterState,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

struct OpenedRestore {
    opened: OwnerOpenedSession,
    generation: SessionGeneration,
    replay: ReplayedSessionFacts,
    fallback: Option<String>,
}

pub(super) struct FixturePorts<P: AcpRequestPayloads> {
    id: String,
    shared: Arc<Mutex<FixtureState<P>>>,
    wire: Arc<OwnerAcpWire>,
    approvals: OwnerApprovalBindings<P>,
    work_binding: OwnerAcpBinding<P>,
    sink: Arc<dyn SessionEventSink>,
    stop: CancellationToken,
    created: Option<(OwnerOpenedSession, CreatedSessionFacts)>,
    restores: Arc<Mutex<HashMap<LoadAttempt, OpenedRestore>>>,
    restored_skills: Option<(RunAttempt<LoadAttempt>, SkillView)>,
    readers: HashMap<SessionGeneration, CancellationToken>,
    active_approval: Option<(TurnId, OwnerApprovalLease<P>)>,
}

impl<P: AcpRequestPayloads> FixturePorts<P> {
    pub(super) fn new(
        id: String,
        shared: Arc<Mutex<FixtureState<P>>>,
        wire: Arc<OwnerAcpWire>,
        approvals: OwnerApprovalBindings<P>,
        sink: Arc<dyn SessionEventSink>,
        stop: CancellationToken,
        created: Option<(OwnerOpenedSession, CreatedSessionFacts)>,
    ) -> Self {
        let tasks = TaskTracker::new();
        let work_binding = OwnerAcpBinding::new(wire.clone(), id.clone(), approvals.clone(), tasks);
        Self {
            id,
            shared,
            wire,
            approvals,
            work_binding,
            sink,
            stop,
            created,
            restores: Arc::default(),
            restored_skills: None,
            readers: HashMap::new(),
            active_approval: None,
        }
    }

    fn transport_run(address: OwnerAddress, generation: SessionGeneration) -> TransportRun {
        TransportRun {
            incarnation: address.incarnation,
            generation,
        }
    }

    fn attach_reader(
        &mut self,
        address: OwnerAddress,
        generation: SessionGeneration,
        mut opened: OwnerOpenedSession,
        completions: &OwnerCompletionPort,
    ) -> Result<(), ActivationRefusal> {
        let run = Self::transport_run(address, generation);
        if !self.wire.activate_route(run) {
            return Err(ActivationRefusal::RouteUnavailable);
        }
        opened.register_in_folder_scope(&self.wire, run);
        {
            let mut shared = lock(&self.shared);
            shared.capabilities = opened.capabilities().clone();
        }
        let retired = self
            .readers
            .entry(generation)
            .or_insert_with(|| self.stop.child_token())
            .clone();
        let target = OwnerStreamTarget::new(
            address,
            run,
            self.wire.clone(),
            completions.clone(),
            retired.clone(),
        );
        let completions = completions.clone();
        tokio::spawn(async move {
            if opened.read_stream(target).await.is_err() {
                let _retired = completions
                    .send_stream(address, OwnerEvent::RunLost(generation), &retired)
                    .await;
            }
        });
        Ok(())
    }

    fn restore(
        &mut self,
        address: OwnerAddress,
        run: RunAttempt<LoadAttempt>,
        via: RestoreVia,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let completion = completions.reserve(address)?;
        let Some((cwd, setup)) = lock(&self.shared).restores.remove(&run.attempt) else {
            return Ok(Some(OwnerEvent::Wire(SessionWireResult::RestoreFailed {
                run,
                failure: RestoreFailure::LocalPrerequisite {
                    stage: PreparationStage::SavedSetup,
                },
            })));
        };
        let route = Self::transport_run(address, run.generation);
        self.wire.open_route(route, self.id.clone());
        let Ok(receipt) = self.wire.restore(
            route,
            run.attempt,
            self.id.clone(),
            via,
            OwnerSessionOpenSetup {
                cwd,
                work_roots: setup.work_roots,
                scratch_folder: setup.scratch_folder,
            },
        ) else {
            return Ok(Some(OwnerEvent::Wire(SessionWireResult::RestoreFailed {
                run,
                failure: RestoreFailure::Disconnected,
            })));
        };
        let restores = self.restores.clone();
        let retired = self.stop.child_token();
        self.readers.insert(run.generation, retired.clone());
        tokio::spawn(async move {
            let receipt = tokio::select! {
                biased;
                () = retired.cancelled() => return,
                receipt = receipt => receipt,
            };
            let event = match receipt {
                Ok(Ok(mut opened)) => match opened.prepare_replay() {
                    Ok(replay) => {
                        let planned = opened.restore_setting_plan(setup.requested_mode.as_deref());
                        let catalogue = Arc::new(opened.catalog().clone());
                        lock(&restores).insert(
                            run.attempt,
                            OpenedRestore {
                                opened,
                                generation: run.generation,
                                replay,
                                fallback: planned.accepted_mode_fallback,
                            },
                        );
                        SessionWireResult::Restore {
                            run,
                            outcome: WireOutcome::Confirmed(RestoreLoadReadback {
                                catalogue,
                                router_auto_requested: setup.requested_mode.as_deref()
                                    == Some(crate::AUTO_PERMISSION_MODE),
                                mode: planned.mode,
                                fast_on: planned.fast_off,
                            }),
                        }
                    }
                    Err(_) => SessionWireResult::RestoreFailed {
                        run,
                        failure: RestoreFailure::LocalPrerequisite {
                            stage: PreparationStage::Route,
                        },
                    },
                },
                Ok(Err(error)) => SessionWireResult::RestoreFailed {
                    run,
                    failure: error
                        .coded_wire_failure()
                        .map_or(RestoreFailure::Disconnected, RestoreFailure::from_coded),
                },
                _ => SessionWireResult::RestoreFailed {
                    run,
                    failure: RestoreFailure::Disconnected,
                },
            };
            completion.send(OwnerEvent::Wire(event));
        });
        Ok(None)
    }

    fn authorize(
        &mut self,
        address: OwnerAddress,
        authorization: DispatchAuthorization,
        work: AuthorizedWork,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let offered = match work {
            AuthorizedWork::Prompt(input) => {
                let pair = lock(&self.shared)
                    .inputs
                    .get(&input.input)
                    .map(|stored| (stored.content.clone(), stored.context.clone()));
                if let Some((content, context)) = pair {
                    if let Some(context) = context {
                        let installed = self.approvals.install(
                            self.id.clone(),
                            Self::transport_run(address, input.generation),
                            input.turn,
                            context.clone(),
                        );
                        let Ok(lease) = installed else {
                            return Ok(Some(OwnerEvent::Dispatch(
                                DispatchResult::DefinitelyUnsent(authorization),
                            )));
                        };
                        self.active_approval = Some((input.turn, lease));
                        lock(&self.shared).active_context = Some((input.turn, context));
                    }
                    self.work_binding.offer_prompt(
                        authorization.clone(),
                        input.turn,
                        content.as_harness_content(),
                        reserve_fixture_work_reply(completions, address)?,
                    )
                } else {
                    return Ok(Some(OwnerEvent::Dispatch(
                        DispatchResult::DefinitelyUnsent(authorization),
                    )));
                }
            }
            AuthorizedWork::Steer { input, origin } => {
                let content = lock(&self.shared)
                    .inputs
                    .get(&input.input)
                    .map(|stored| stored.content.clone());
                if let Some(content) = content {
                    self.work_binding.offer_steer(
                        authorization.clone(),
                        content.as_harness_content(),
                        match origin {
                            SteerOrigin::Auto => SteerIdlePolicy::AllowOwnerPrompt,
                            SteerOrigin::Named | SteerOrigin::Notice => SteerIdlePolicy::Refuse,
                        },
                        reserve_fixture_work_reply(completions, address)?,
                    )
                } else {
                    return Ok(Some(OwnerEvent::Dispatch(
                        DispatchResult::DefinitelyUnsent(authorization),
                    )));
                }
            }
            AuthorizedWork::Cancel(_) => return Err(OwnerTaskError::ExecutorStopped),
            AuthorizedWork::Setting { setting, value, .. } => {
                match self.work_binding.offer_setting(
                    authorization.clone(),
                    setting.kind,
                    value,
                    reserve_fixture_work_reply(completions, address)?,
                ) {
                    Ok(SettingOffer::Settled(outcome)) => {
                        return Ok(Some(OwnerEvent::Dispatch(outcome)));
                    }
                    Ok(SettingOffer::Offered) => Ok(()),
                    Err(refusal) => Err(refusal),
                }
            }
        };
        if let Err(refusal) = offered {
            let result = if refusal.valid_moves().contains(&RefusedWorkMove::TakeBack) {
                self.revoke_work(authorization)
            } else {
                DispatchResult::DefinitelyUnsent(authorization)
            };
            return Ok(Some(OwnerEvent::Dispatch(result)));
        }
        Ok(self
            .work_binding
            .hand_over(&authorization, &mut || true)
            .ok()
            .map(OwnerEvent::Dispatch))
    }

    fn revoke_work(&mut self, authorization: DispatchAuthorization) -> DispatchResult {
        self.work_binding
            .take_back(&authorization)
            .unwrap_or(DispatchResult::Revoked(authorization))
    }

    fn restore_setting(
        &self,
        address: OwnerAddress,
        run: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        value: String,
        catalogue: &ProviderSettingsCatalog,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let pending = lock(&self.restores);
        let opened = pending
            .get(&run.attempt)
            .ok_or(OwnerTaskError::ExecutorStopped)?;
        let plan = OwnerSettingWirePlan::from_catalog(
            self.id.clone(),
            catalogue,
            setting.kind,
            value,
            opened.fallback.clone(),
        );
        let completion = completions.reserve(address)?;
        match plan {
            Ok(plan) => {
                let _sent =
                    self.wire
                        .reapply_setting(address.incarnation, run, setting, plan, completion);
                Ok(None)
            }
            Err(_) => {
                let event = if setting.kind == provider_session_machines::ProviderSettingKind::Fast
                {
                    OwnerEvent::Prepare(RestorePreparation::FastOff {
                        run,
                        setting,
                        catalogue: None,
                        observation: SettingEvidence {
                            value: ObservedSetting::Unknown,
                            cause: Some(ObservationCause::NotOffered),
                        },
                    })
                } else {
                    OwnerEvent::Wire(SessionWireResult::RestoreMode {
                        run, setting, outcome: provider_session_machines::session_owner::RestoreModeOutcome::Unconfirmed, catalogue: None,
                    })
                };
                Ok(Some(event))
            }
        }
    }

    fn publish_live(&self, fact: SessionPublication) -> Result<(), crate::EventSinkClosed> {
        let (run, event) = match fact {
            SessionPublication::Created(run) => (
                run,
                SessionEvent::StateChanged {
                    state: SessionState::Idle,
                },
            ),
            SessionPublication::Activated { run, .. } => (
                run.generation,
                SessionEvent::StateChanged {
                    state: SessionState::Idle,
                },
            ),
            SessionPublication::Closed(run) => (
                run,
                SessionEvent::StateChanged {
                    state: SessionState::Closed,
                },
            ),
            SessionPublication::Unloaded(run) => (
                run,
                SessionEvent::StateChanged {
                    state: SessionState::Unloaded,
                },
            ),
            SessionPublication::TurnStarted {
                generation,
                turn,
                input,
            } => (
                generation,
                SessionEvent::TurnStarted {
                    turn_id: turn.0.to_string(),
                    input_id: input,
                },
            ),
            SessionPublication::TurnEnded {
                generation,
                turn,
                outcome,
                ..
            }
            | SessionPublication::PreviousTurnEnded {
                generation,
                turn,
                outcome,
            } => (
                generation,
                SessionEvent::TurnEnded {
                    turn_id: turn.0.to_string(),
                    outcome,
                },
            ),
            SessionPublication::SettingObserved { .. } => return Ok(()),
        };
        self.sink.publish_in(&self.id, run, event).map(|_| ())
    }

    fn publish(&mut self, address: OwnerAddress, batch: PublicationBatch) -> OwnerEvent {
        let (id, facts) = match batch {
            PublicationBatch::Commit { id, facts } => (id, facts),
            PublicationBatch::Rebase { id, .. } => {
                return OwnerEvent::Publication(PublicationResult::Applied(id));
            }
        };
        for fact in &facts {
            if let PublicationFact::Settings {
                run,
                accepted: Some(accepted),
                ..
            } = fact
            {
                self.wire
                    .publish_run_settings(Self::transport_run(address, *run), accepted.clone());
            }
        }
        let applied = facts.into_iter().try_for_each(|fact| match fact {
            PublicationFact::ReplayBoundary(_) => self
                .sink
                .begin_history_replay(&self.id)
                .now_or_never()
                .unwrap_or(Err(crate::HistoryReplayUnavailable))
                .map_err(|_| crate::EventSinkClosed),
            PublicationFact::HistoryUnavailable(_) => Ok(()),
            PublicationFact::Replay { run, event } => self
                .sink
                .publish_in(&self.id, run.generation, event)
                .map(|_| ()),
            PublicationFact::Stream { run, event } => {
                self.sink.publish_in(&self.id, run, event).map(|_| ())
            }
            PublicationFact::Capabilities { run, report } => self
                .sink
                .publish_in(
                    &self.id,
                    run,
                    SessionEvent::CapabilitiesChanged {
                        capabilities: report,
                    },
                )
                .map(|_| ()),
            PublicationFact::Settings { run, settings, .. } => self
                .sink
                .publish_in(&self.id, run, SessionEvent::SettingsChanged { settings })
                .map(|_| ()),
            PublicationFact::Skills { view, .. } => self.sink.publish_skill_view(&self.id, view),
            PublicationFact::Live(fact) => {
                if let SessionPublication::Activated { run, .. } = &fact
                    && self
                        .restored_skills
                        .as_ref()
                        .is_some_and(|(attempt, _)| attempt == run)
                    && let Some((_, skills)) = self.restored_skills.take()
                {
                    self.sink.publish_skill_view(&self.id, skills)?;
                }
                self.publish_live(fact)
            }
        });
        OwnerEvent::Publication(if applied.is_ok() {
            PublicationResult::Applied(id)
        } else {
            PublicationResult::Refused(id)
        })
    }

    fn submit_item(&self, input: OwnerInput, accepted: bool) -> Option<OwnerEvent> {
        let mut state = lock(&self.shared);
        let payload = state.inputs.get_mut(&input.input)?;
        if accepted && let Some(notice) = payload.dispatch.take() {
            let _receiver_gone = notice.send(ProviderPromptDispatchObservation::Submitted);
        }
        let text = payload.content.submitted_text()?.to_owned();
        Some(OwnerEvent::SubmittedInput {
            input,
            item: SessionItem {
                item_id: Uuid::now_v7().to_string(),
                kind: SessionItemKind::UserMessage,
                text: Some(text),
            },
        })
    }
}

impl<P: AcpRequestPayloads> SessionOwnerPorts for FixturePorts<P> {
    fn execute(
        &mut self,
        address: OwnerAddress,
        effect: OwnerEffect,
        completions: &OwnerCompletionPort,
        _caller_wakes: &mut crate::session_owner_task::OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        match effect {
            OwnerEffect::RecordDeliveryEvidence(_) => Err(OwnerTaskError::ExecutorStopped),
            OwnerEffect::Wire(SessionWireRequest::Create { operation }) => {
                Ok(Some(OwnerEvent::Create(CreateProgress::ProviderId {
                    operation,
                    provider_id: self.id.clone(),
                })))
            }
            OwnerEffect::AdoptProviderId { operation, .. } => {
                let (_, facts) = self
                    .created
                    .as_ref()
                    .ok_or(OwnerTaskError::ExecutorStopped)?;
                Ok(Some(OwnerEvent::Create(CreateProgress::Configured {
                    operation,
                    facts: Box::new(facts.clone()),
                })))
            }
            OwnerEffect::ActivateCreated {
                operation,
                generation,
                facts,
            } => {
                let (opened, _) = self.created.take().ok_or(OwnerTaskError::ExecutorStopped)?;
                self.wire
                    .open_route(Self::transport_run(address, generation), self.id.clone());
                let progress = match self.attach_reader(address, generation, opened, completions) {
                    Ok(()) => CreateProgress::Activated { operation, facts },
                    Err(_) => CreateProgress::Failed {
                        operation,
                        remote: RemoteEffect::Applied,
                    },
                };
                Ok(Some(OwnerEvent::Create(progress)))
            }
            OwnerEffect::Wire(SessionWireRequest::Restore { run, via }) => {
                self.restore(address, run, via, completions)
            }
            OwnerEffect::Wire(SessionWireRequest::RestoreMode {
                run,
                setting,
                value,
                catalogue,
            }) => self.restore_setting(address, run, setting, value.0, &catalogue, completions),
            OwnerEffect::Wire(SessionWireRequest::RestoreFastOff {
                run,
                setting,
                catalogue,
            }) => {
                self.restore_setting(address, run, setting, "off".into(), &catalogue, completions)
            }
            OwnerEffect::Wire(SessionWireRequest::Close(run)) => {
                if self
                    .wire
                    .enqueue_close(
                        address.incarnation,
                        run,
                        self.id.clone(),
                        completions.reserve(address)?,
                    )
                    .is_ok()
                {
                    Ok(None)
                } else {
                    Ok(Some(OwnerEvent::Wire(SessionWireResult::CloseNotSent(run))))
                }
            }
            OwnerEffect::Store(request) => Ok(Some(OwnerEvent::Store(match request {
                SessionStoreRequest::ReadRestoreSetup(run) => SessionStoreResult::RestoreSetup {
                    run,
                    outcome: EffectOutcome::Applied,
                },
                SessionStoreRequest::MarkDispatch(input)
                | SessionStoreRequest::MarkQueuedDispatch(input) => {
                    let completion = completions.reserve(address)?;
                    tokio::spawn(async move {
                        crate::sim_points::reach(
                            crate::sim_points::ClientSimPoint::DispatchMarkOffered,
                        )
                        .await;
                        completion.send(OwnerEvent::Store(SessionStoreResult::Dispatch {
                            input,
                            outcome: EffectOutcome::Applied,
                        }));
                    });
                    return Ok(None);
                }
                SessionStoreRequest::MarkSteerDispatch(input) => {
                    SessionStoreResult::SteerDispatch {
                        input,
                        outcome: EffectOutcome::Applied,
                    }
                }
                SessionStoreRequest::UndoDispatch(input) => SessionStoreResult::UndoDispatch {
                    input,
                    outcome: EffectOutcome::Applied,
                },
                SessionStoreRequest::ReconcileLateCancelledPrompt(input) => {
                    SessionStoreResult::LateCancelledPromptReconciled {
                        input,
                        outcome: EffectOutcome::Applied,
                    }
                }
                SessionStoreRequest::RecordConfirmedSetting { setting, .. } => {
                    SessionStoreResult::ConfirmedSetting {
                        setting,
                        outcome: EffectOutcome::Applied,
                    }
                }
                SessionStoreRequest::RecordTerminal { write, .. } => SessionStoreResult::Terminal {
                    write,
                    outcome: EffectOutcome::Applied,
                },
            }))),
            OwnerEffect::PrepareRoute(run) => {
                let pending = lock(&self.restores);
                let opened = pending
                    .get(&run.attempt)
                    .ok_or(OwnerTaskError::ExecutorStopped)?;
                let preparation = completions.reserve(address)?;
                let events = opened.replay.events.clone();
                preparation.send(OwnerEvent::Prepare(RestorePreparation::RouteReady(run)));
                Ok(Some(OwnerEvent::Replay { run, events }))
            }
            OwnerEffect::PrepareHost { run, catalogue } => {
                let mut pending = lock(&self.restores);
                let opened = pending
                    .get_mut(&run.attempt)
                    .ok_or(OwnerTaskError::ExecutorStopped)?;
                opened.opened.accept_restore_catalogue(&catalogue);
                Ok(Some(OwnerEvent::Prepare(RestorePreparation::HostReady {
                    run,
                    capabilities: opened.opened.capabilities().to_session_model(),
                    settings: opened.opened.settings(),
                    observed: BTreeMap::new(),
                })))
            }
            OwnerEffect::ActivateRestored { run, .. } => {
                let opened = lock(&self.restores).remove(&run.attempt);
                let result = match opened {
                    Some(opened) => self
                        .attach_reader(address, run.generation, opened.opened, completions)
                        .map(|()| {
                            self.restored_skills = opened.replay.skills.map(|skills| (run, skills))
                        }),
                    None => Err(ActivationRefusal::OpeningNotHeld),
                };
                Ok(Some(OwnerEvent::Prepare(RestorePreparation::Activation {
                    run,
                    result,
                })))
            }
            OwnerEffect::Authorize {
                authorization,
                work,
            } => self.authorize(address, authorization, work, completions),
            OwnerEffect::DrainCancel(authorization) => {
                match self.work_binding.begin_cancel(
                    authorization.clone(),
                    &mut || completions.reserve_work_reply(address),
                    Box::pin(self.stop.clone().cancelled_owned()),
                ) {
                    CancelDrainStart::AlreadyDrained => Ok(Some(OwnerEvent::Dispatch(
                        DispatchResult::CancelDrainFinished(authorization),
                    ))),
                    CancelDrainStart::Waiting => Ok(None),
                    CancelDrainStart::Refused(CancelReservationRefusal::Full) => {
                        Err(OwnerTaskError::CompletionCapacity)
                    }
                    CancelDrainStart::Refused(CancelReservationRefusal::Stopped) => {
                        Err(OwnerTaskError::ExecutorStopped)
                    }
                }
            }
            OwnerEffect::TakeCancel {
                authorization,
                expired,
            } => {
                let wait = if expired {
                    CancelDrainWait::Expired
                } else {
                    CancelDrainWait::Drained
                };
                Ok(Some(OwnerEvent::Dispatch(
                    self.work_binding.send_cancel(authorization, wait),
                )))
            }
            OwnerEffect::Revoke { authorization, .. } => {
                Ok(Some(OwnerEvent::Dispatch(self.revoke_work(authorization))))
            }
            OwnerEffect::CallbackMembership(change) => {
                self.wire.apply_membership(address.incarnation, change);
                if let TurnMembership::End { turn, .. }
                | TurnMembership::RemoveOnRevoke { turn, .. } = change
                    && self
                        .active_approval
                        .as_ref()
                        .is_some_and(|(held, _)| *held == turn)
                {
                    self.active_approval = None;
                    lock(&self.shared).active_context = None;
                }
                Ok(None)
            }
            OwnerEffect::RetireRoute(generation) => {
                if let Some(stop) = self.readers.remove(&generation) {
                    stop.cancel();
                }
                lock(&self.restores).retain(|_, opened| opened.generation != generation);
                Ok(None)
            }
            OwnerEffect::ReopenRoute(run) => {
                self.wire
                    .reopen_route(Self::transport_run(address, run.generation), run.attempt);
                Ok(None)
            }
            OwnerEffect::Publish(batch) => Ok(Some(self.publish(address, batch))),
            OwnerEffect::Reply(reply) | OwnerEffect::ReplyUnsaved(reply) => {
                let operation = match &reply {
                    SessionOwnerReply::RestoreFinished(result) => result.attempt.attempt.0,
                    SessionOwnerReply::CloseFinished { run, .. } => run.attempt.0,
                    SessionOwnerReply::SettingFinished { setting, .. }
                    | SessionOwnerReply::SettingKept { setting }
                    | SessionOwnerReply::SettingNotSent { setting, .. }
                    | SessionOwnerReply::ModelFastFinished { setting, .. } => {
                        setting.change.attempt.0
                    }
                    SessionOwnerReply::PromptCompleted { input, .. }
                    | SessionOwnerReply::PromptFailed { input, .. }
                    | SessionOwnerReply::PromptCancelUnconfirmed { input }
                    | SessionOwnerReply::SteerInvalidReply { input }
                    | SessionOwnerReply::InputFinished { input, .. }
                    | SessionOwnerReply::InputUnsent { input, .. }
                    | SessionOwnerReply::SteerStarted { input, .. } => {
                        if let Some(stored) = lock(&self.shared).inputs.remove(&input.input)
                            && let Some(notice) = stored.dispatch
                        {
                            let _gone =
                                notice.send(ProviderPromptDispatchObservation::NotSubmitted);
                        }
                        input.operation
                    }
                };
                if let Some(settle) = lock(&self.shared).replies.remove(&operation) {
                    let _gone = settle.send(reply);
                }
                Ok(None)
            }
            OwnerEffect::PromptTaken(input) => Ok(self.submit_item(input, true)),
            OwnerEffect::SteerSubmitted(input) => Ok(self.submit_item(input, false)),
            OwnerEffect::CancelSent { drain, .. } => {
                lock(&self.shared)
                    .cancel_report
                    .send_replace(Some(Ok(drain)));
                Ok(None)
            }
            OwnerEffect::CancelNotSent { reason, .. } => {
                lock(&self.shared)
                    .cancel_report
                    .send_replace(Some(Err(reason)));
                Ok(None)
            }
            OwnerEffect::ProviderStartedTurn(input) => Ok(self.submit_item(input, false)),
            OwnerEffect::Arm { .. }
            | OwnerEffect::CreateSettled { .. }
            | OwnerEffect::RestoreSettled { .. }
            | OwnerEffect::InputQueued(_)
            | OwnerEffect::SettingQueued { .. }
            | OwnerEffect::ForwardTool { .. } => Ok(None),
        }
    }
}

fn reserve_fixture_work_reply(
    completions: &OwnerCompletionPort,
    address: OwnerAddress,
) -> Result<ReservedWorkReply, OwnerTaskError> {
    match completions.reserve_work_reply(address) {
        ReplyAdmission::Reserved(reply) => Ok(reply),
        ReplyAdmission::Refused(WriterState::Full) => Err(OwnerTaskError::CompletionCapacity),
        ReplyAdmission::Refused(WriterState::Stopped) => Err(OwnerTaskError::ExecutorStopped),
    }
}
