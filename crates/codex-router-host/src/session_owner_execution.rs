//! Execute the effects of one adopted session owner.

#[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
use crate::session_owner_work_observation::{WorkObservationKind, WorkObservationSink};
use crate::{
    session_owner_calls::{InputAcceptance, SessionOwnerCalls},
    session_owner_lifecycle_reports::{SessionHarnessBinding, SessionOwnerLifecycleReports},
    session_owner_publication::SessionOwnerPublisher,
    session_owner_storage::SessionOwnerStorage,
};
use acp_client_runtime::session_owner_task::{
    OwnerCallerWakes, OwnerCompletionPort, OwnerTaskError, SessionOwnerPorts,
};
use collaboration_protocol::{ProviderIdentity, SessionRef};
use lifecycle::lifecycle_settings;
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        ActivationSetup, ApprovalLease, ApprovalLeaseRefusalReason, CreateAttempt, ExactTurn, Held,
        OpenAttempt, TransportRun, owner_event_for_close,
    },
    session_owner::{
        AuthorizedWork, CancelDrainStart, CancelDrainWait, CancelReservationRefusal,
        DispatchAuthorization, DispatchResult, OwnerAddress, OwnerEffect, OwnerEvent,
        PreparationStage, PublicationBatch, PublicationFact, RefusedWorkMove, ReplyAdmission,
        ReservedWorkReply, RestorePreparation, SessionOwnerReply, SessionPublication,
        SessionWireRequest, SessionWireResult, SettingOffer, SteerIdlePolicy, SteerOrigin,
        TurnMembership, WorkOfferRefusal, WriterState,
    },
    session_setting_machine::SettingValueId,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[path = "session_owner_execution_lifecycle.rs"]
mod lifecycle;

#[cfg(test)]
#[path = "session_owner_work_refusal_test.rs"]
mod work_refusal_tests;

pub struct SessionOwnerExecution {
    target: SessionRef,
    binding: SessionHarnessBinding,
    observations: Arc<SessionOwnerLifecycleReports>,
    storage: SessionOwnerStorage,
    publisher: SessionOwnerPublisher,
    calls: Arc<SessionOwnerCalls>,
    tasks: TaskTracker,
    routes: BTreeMap<SessionGeneration, OwnerRun>,
    created: Option<CreateAttempt>,
    work_roots: crate::provider_work_roots::ProviderWorkRoots,
    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
    work_observer: Option<Arc<dyn WorkObservationSink>>,
    #[cfg(any(test, feature = "test-support"))]
    preparation_fixture: Arc<crate::session_owner_preparation_fixture::PreparationFixture>,
}

struct OwnerRun {
    cancel: CancellationToken,
    router_auto_requested: bool,
    approval: Option<ActiveTurnApproval>,
}

struct ActiveTurnApproval {
    turn: provider_session_machines::session_turn_machine::TurnId,
    approver: ProviderIdentity,
    _lease: Box<dyn ApprovalLease>,
}

enum ApprovalLeaseMoment {
    BeforeOffer,
    ObservedTurn,
}

impl SessionOwnerExecution {
    pub(crate) fn new(
        target: SessionRef,
        binding: SessionHarnessBinding,
        observations: Arc<SessionOwnerLifecycleReports>,
        storage: SessionOwnerStorage,
        publisher: SessionOwnerPublisher,
        calls: Arc<SessionOwnerCalls>,
        work_roots: crate::provider_work_roots::ProviderWorkRoots,
    ) -> Self {
        let tasks = storage.task_tracker();
        Self {
            target,
            binding,
            observations,
            storage,
            publisher,
            calls,
            tasks,
            work_roots,
            routes: BTreeMap::new(),
            created: None,
            #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
            work_observer: None,
            #[cfg(any(test, feature = "test-support"))]
            preparation_fixture: Arc::default(),
        }
    }

    #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
    pub(crate) fn with_work_observer(
        mut self,
        observer: Option<Arc<dyn WorkObservationSink>>,
    ) -> Self {
        self.work_observer = observer;
        self
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn with_preparation_fixture(
        mut self,
        fixture: Arc<crate::session_owner_preparation_fixture::PreparationFixture>,
    ) -> Self {
        self.preparation_fixture = fixture;
        self
    }

    fn session_id(&self) -> String {
        String::from(self.target.session_id.clone())
    }

    fn submitted_input(
        &self,
        input: provider_session_machines::session_owner::OwnerInput,
    ) -> Option<OwnerEvent> {
        let content = self.calls.content(&input.input)?;
        let text = content.submitted_text()?.to_owned();
        Some(OwnerEvent::SubmittedInput {
            input,
            item: session_event_model::SessionItem {
                item_id: uuid::Uuid::now_v7().to_string(),
                kind: session_event_model::SessionItemKind::UserMessage,
                text: Some(text),
            },
        })
    }

    fn install_approval_for(
        &self,
        owner: OwnerAddress,
        input: &provider_session_machines::session_owner::OwnerInput,
        moment: ApprovalLeaseMoment,
    ) -> Result<Option<ActiveTurnApproval>, ApprovalLeaseRefusalReason> {
        let Some(context) = self.calls.take_approval(&input.input) else {
            return Ok(None);
        };
        let approver = context.approver.clone();
        let turn = ExactTurn {
            run: TransportRun {
                incarnation: owner.incarnation,
                generation: input.generation,
            },
            turn: input.turn,
        };
        let lease = match moment {
            ApprovalLeaseMoment::BeforeOffer => self
                .binding
                .install_approval_lease_before_offer(turn, context)
                .map_err(|refusal| refusal.0),
            ApprovalLeaseMoment::ObservedTurn => self
                .binding
                .install_approval_lease_for_observed_turn(turn, context)
                .map_err(|refusal| refusal.0),
        }?;
        Ok(Some(ActiveTurnApproval {
            turn: input.turn,
            approver,
            _lease: lease,
        }))
    }

    fn authorize(
        &mut self,
        owner: OwnerAddress,
        authorization: DispatchAuthorization,
        work: AuthorizedWork,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        #[cfg(test)]
        let named_steer = matches!(
            &work,
            AuthorizedWork::Steer {
                origin: SteerOrigin::Named,
                ..
            }
        );
        let notice = match &work {
            AuthorizedWork::Prompt(input) | AuthorizedWork::Steer { input, .. } => {
                self.calls.notice(&input.input)
            }
            AuthorizedWork::Cancel(_) | AuthorizedWork::Setting { .. } => None,
        };
        #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
        let witness_kind = if notice.is_some() {
            Some(WorkObservationKind::ManagedNotice)
        } else if matches!(&work, AuthorizedWork::Prompt(_)) {
            Some(WorkObservationKind::Prompt)
        } else {
            None
        };
        let setting_operation = match &work {
            AuthorizedWork::Setting { setting, .. } => Some(setting.change.attempt.0),
            _ => None,
        };
        let offered = match work {
            AuthorizedWork::Cancel(_) => return Err(OwnerTaskError::ExecutorStopped),
            AuthorizedWork::Prompt(input) => {
                let Some(content) = self.calls.content(&input.input) else {
                    return Ok(Some(definitely_unsent(authorization)));
                };
                let completion = reserve_work_reply(completions, owner)?;
                let Ok(approval) =
                    self.install_approval_for(owner, &input, ApprovalLeaseMoment::BeforeOffer)
                else {
                    return Ok(Some(definitely_unsent(authorization)));
                };
                let offered = self.binding.offer_prompt(
                    authorization.clone(),
                    input.turn,
                    content.as_harness_content(),
                    completion,
                );
                if offered.is_ok()
                    && let Some(run) = self.routes.get_mut(&input.generation)
                {
                    run.approval = approval;
                }
                offered
            }
            AuthorizedWork::Steer { input, origin } => {
                let approver = self
                    .routes
                    .get(&input.generation)
                    .and_then(|run| run.approval.as_ref())
                    .filter(|approval| approval.turn == input.turn)
                    .map(|approval| &approval.approver);
                self.calls.capture_steer_target(&input, approver);
                let Some(content) = self.calls.content(&input.input) else {
                    return Ok(Some(definitely_unsent(authorization)));
                };
                let completion = reserve_work_reply(completions, owner)?;
                let idle = match origin {
                    SteerOrigin::Auto => SteerIdlePolicy::AllowOwnerPrompt,
                    SteerOrigin::Named | SteerOrigin::Notice => SteerIdlePolicy::Refuse,
                };
                self.binding.offer_steer(
                    authorization.clone(),
                    content.as_harness_content(),
                    idle,
                    completion,
                )
            }
            AuthorizedWork::Setting { setting, value, .. } => {
                if !self.calls.settlement_requested(setting.change.attempt.0) {
                    return Ok(Some(definitely_unsent(authorization)));
                }
                match self.binding.offer_setting(
                    authorization.clone(),
                    setting.kind,
                    value,
                    reserve_work_reply(completions, owner)?,
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
            return Ok(Some(settle_refused_offer(
                authorization,
                refusal,
                |authorization| self.binding.take_back(authorization),
            )));
        }
        #[cfg(test)]
        if named_steer && let Some(release) = self.preparation_fixture.hold_named_steer_take() {
            // Leave the offer on this binding while the owner consumes the foreground ending.
            let completion = completions.reserve(owner)?;
            let take = self
                .binding
                .detached_take_for_test(authorization)
                .ok_or(OwnerTaskError::ExecutorStopped)?;
            self.tasks.spawn(async move {
                let _released = release.await;
                if let Ok(result) = take() {
                    completion.send(OwnerEvent::Dispatch(result));
                }
            });
            return Ok(None);
        }
        #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
        if let (Some(work_kind), Some(observer)) = (witness_kind, &self.work_observer) {
            observer.offered(&authorization, work_kind);
        }
        let handed_over = if let Some(notice) = notice {
            notice.take(&authorization, self.binding.as_ref())
        } else {
            self.binding
                .hand_over(&authorization, &mut || {
                    setting_operation
                        .is_none_or(|operation| self.calls.settlement_requested(operation))
                })
                .ok()
        };
        #[cfg(all(debug_assertions, unix, feature = "harness-work-observation"))]
        if let (Some(work_kind), Some(observer)) = (witness_kind, &self.work_observer)
            && matches!(&handed_over, Some(DispatchResult::Taken(_)))
        {
            observer.newly_taken(&authorization, work_kind);
        }
        Ok(handed_over.map(OwnerEvent::Dispatch))
    }
}

impl SessionOwnerPorts for SessionOwnerExecution {
    fn execute(
        &mut self,
        owner: OwnerAddress,
        effect: OwnerEffect,
        completions: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        match effect {
            OwnerEffect::RecordDeliveryEvidence(placement) => {
                self.calls.offer_delivery_evidence(
                    placement,
                    completions.reserve(owner)?,
                    &self.tasks,
                );
                Ok(None)
            }
            OwnerEffect::Store(request) => self.storage.execute(owner, request, completions),
            OwnerEffect::Wire(SessionWireRequest::Restore { run, via }) => {
                self.restore(owner, run, via, completions)
            }
            OwnerEffect::Wire(SessionWireRequest::RestoreMode {
                run,
                setting,
                value,
                catalogue,
            }) => self.restore_setting(owner, run, setting, value, &catalogue, completions),
            OwnerEffect::Wire(SessionWireRequest::RestoreFastOff {
                run,
                setting,
                catalogue,
            }) => self.restore_setting(
                owner,
                run,
                setting,
                SettingValueId("off".into()),
                &catalogue,
                completions,
            ),
            OwnerEffect::Wire(SessionWireRequest::Close(run)) => {
                let completion = completions.reserve(owner)?.map(owner_event_for_close);
                let unavailable = self.binding.close(run, completion).is_err();
                Ok(unavailable.then_some(OwnerEvent::Wire(SessionWireResult::CloseNotSent(run))))
            }
            OwnerEffect::Wire(SessionWireRequest::Create { .. }) => {
                Err(OwnerTaskError::ExecutorStopped)
            }
            OwnerEffect::PrepareRoute(run) => Ok(Some(OwnerEvent::Prepare(
                if self.binding.opening_is_held(OpenAttempt::Restore(run)) == Held::Held {
                    RestorePreparation::RouteReady(run)
                } else {
                    RestorePreparation::Failed {
                        run,
                        stage: PreparationStage::Route,
                    }
                },
            ))),
            OwnerEffect::PrepareHost { run, catalogue } => {
                self.prepare_harness_settings(owner, run, catalogue, completions)
            }
            OwnerEffect::Authorize {
                authorization,
                work,
            } => self.authorize(owner, authorization, work, completions),
            OwnerEffect::DrainCancel(authorization) => {
                let stop = self
                    .routes
                    .get(&authorization.run)
                    .map(|route| route.cancel.clone())
                    .unwrap_or_default();
                match self.binding.begin_cancel(
                    authorization.clone(),
                    &mut || completions.reserve_work_reply(owner),
                    Box::pin(stop.cancelled_owned()),
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
                let result = self.binding.send_cancel(authorization, wait);
                Ok(Some(OwnerEvent::Dispatch(result)))
            }
            OwnerEffect::Revoke { authorization, .. } => Ok(Some(OwnerEvent::Dispatch(
                take_back_work(authorization, |authorization| {
                    self.binding.take_back(authorization)
                }),
            ))),
            OwnerEffect::CallbackMembership(change) => {
                self.binding.set_scope(change);
                if let TurnMembership::End { turn, .. } = change {
                    self.calls.end_turn(turn);
                }
                if let TurnMembership::End { run, turn }
                | TurnMembership::RemoveOnRevoke { run, turn } = change
                    && let Some(runtime) = self.routes.get_mut(&run)
                    && runtime
                        .approval
                        .as_ref()
                        .is_some_and(|active| active.turn == turn)
                {
                    runtime.approval = None;
                }
                Ok(None)
            }
            OwnerEffect::Publish(batch) => {
                if let PublicationBatch::Commit { facts, .. } = &batch {
                    for fact in facts {
                        let setup = match fact {
                            PublicationFact::Live(SessionPublication::Activated {
                                run, ..
                            }) => ActivationSetup::Restored(*run),
                            PublicationFact::Live(SessionPublication::Created(generation)) => {
                                let Some(attempt) = self.created.take() else {
                                    continue;
                                };
                                ActivationSetup::Created {
                                    attempt,
                                    generation: *generation,
                                }
                            }
                            _ => continue,
                        };
                        let generation = match setup {
                            ActivationSetup::Created { generation, .. } => generation,
                            ActivationSetup::Restored(run) => run.generation,
                        };
                        let accepted = facts
                            .iter()
                            .find_map(|fact| match fact {
                                PublicationFact::Settings { run, accepted, .. }
                                    if *run == generation =>
                                {
                                    accepted.as_ref()
                                }
                                _ => None,
                            })
                            .ok_or(OwnerTaskError::ExecutorStopped)?;
                        self.activate(setup, lifecycle_settings(accepted));
                    }
                    for fact in facts {
                        if let PublicationFact::Settings {
                            run,
                            accepted: Some(accepted),
                            ..
                        } = fact
                        {
                            self.binding
                                .use_settings(*run, lifecycle_settings(accepted));
                        }
                    }
                }
                self.publisher.publish(owner, batch, completions)
            }
            OwnerEffect::Reply(reply) => {
                self.storage.release_dispatch_wait(&reply);
                if let SessionOwnerReply::SettingFinished { setting, .. }
                | SessionOwnerReply::SettingNotSent { setting, .. }
                | SessionOwnerReply::ModelFastFinished { setting, .. } = &reply
                {
                    self.storage.forget_setting(*setting);
                }
                self.calls.settle(reply, true, Some(caller_wakes));
                Ok(None)
            }
            OwnerEffect::ReplyUnsaved(reply) => {
                self.storage.release_dispatch_wait(&reply);
                self.calls.settle(reply, false, Some(caller_wakes));
                Ok(None)
            }
            OwnerEffect::RestoreSettled { operation, result } => {
                self.calls.deliver_restore(operation, result, caller_wakes);
                Ok(None)
            }
            OwnerEffect::RetireRoute(run) => {
                if let Some(route) = self.routes.remove(&run) {
                    route.cancel.cancel();
                }
                self.binding.retire(run);
                Ok(None)
            }
            OwnerEffect::ReopenRoute(run) => {
                self.binding.admit_again(run);
                Ok(None)
            }
            OwnerEffect::InputQueued(input) => {
                self.calls
                    .accepted(input.operation, InputAcceptance::Queued, Some(caller_wakes));
                Ok(None)
            }
            OwnerEffect::SettingQueued { .. } => {
                #[cfg(any(test, feature = "test-support"))]
                self.storage.notice_queued_setting();
                Ok(None)
            }
            OwnerEffect::PromptTaken(input) => {
                self.calls.prompt_taken(input.clone(), caller_wakes);
                Ok(self.submitted_input(input))
            }
            OwnerEffect::SteerSubmitted(input) => Ok(self.submitted_input(input)),
            OwnerEffect::CancelSent {
                authorization,
                operation,
                drain,
            } => {
                use provider_session_machines::session_owner::CancelDrainReport;
                if let CancelDrainReport::Expired { pending } = &drain {
                    tracing::warn!(
                        session = %self.session_id(),
                        %authorization.authorization_seq,
                        ?pending,
                        "{}", CancelDrainReport::EXPIRED_SUMMARY
                    );
                }
                self.calls
                    .settle_cancel(&authorization, operation, Ok(drain), caller_wakes);
                Ok(None)
            }
            OwnerEffect::CancelNotSent {
                authorization,
                operation,
                reason,
            } => {
                self.calls
                    .settle_cancel(&authorization, operation, Err(reason), caller_wakes);
                Ok(None)
            }
            OwnerEffect::ProviderStartedTurn(input) => {
                self.calls.record_active_turn(&input);
                if let Some(approval) = self
                    .install_approval_for(owner, &input, ApprovalLeaseMoment::ObservedTurn)
                    .ok()
                    .flatten()
                    && let Some(run) = self.routes.get_mut(&input.generation)
                {
                    run.approval = Some(approval);
                }
                tracing::debug!(turn = %input.turn.0, "started by a steer; its end isn't reported by the harness until it finishes");
                Ok(self.submitted_input(input))
            }
            OwnerEffect::Arm { .. }
            | OwnerEffect::AdoptProviderId { .. }
            | OwnerEffect::CreateSettled { .. }
            | OwnerEffect::ForwardTool { .. } => Ok(None),
        }
    }
}

impl Drop for SessionOwnerExecution {
    fn drop(&mut self) {
        self.storage.stop_dispatch_waits();
        for route in self.routes.values() {
            route.cancel.cancel();
        }
    }
}

fn reserve_work_reply(
    completions: &OwnerCompletionPort,
    owner: OwnerAddress,
) -> Result<ReservedWorkReply, OwnerTaskError> {
    match completions.reserve_work_reply(owner) {
        ReplyAdmission::Reserved(reply) => Ok(reply),
        ReplyAdmission::Refused(WriterState::Full) => Err(OwnerTaskError::CompletionCapacity),
        ReplyAdmission::Refused(WriterState::Stopped) => Err(OwnerTaskError::ExecutorStopped),
    }
}

fn definitely_unsent(authorization: DispatchAuthorization) -> OwnerEvent {
    OwnerEvent::Dispatch(DispatchResult::DefinitelyUnsent(authorization))
}

fn settle_refused_offer(
    authorization: DispatchAuthorization,
    refusal: WorkOfferRefusal,
    take_back: impl FnOnce(
        &DispatchAuthorization,
    ) -> Result<
        DispatchResult,
        provider_session_machines::session_owner::TakeBackRefusal,
    >,
) -> OwnerEvent {
    if refusal.valid_moves().contains(&RefusedWorkMove::TakeBack) {
        OwnerEvent::Dispatch(take_back_work(authorization, take_back))
    } else {
        definitely_unsent(authorization)
    }
}

fn take_back_work(
    authorization: DispatchAuthorization,
    take_back: impl FnOnce(
        &DispatchAuthorization,
    ) -> Result<
        DispatchResult,
        provider_session_machines::session_owner::TakeBackRefusal,
    >,
) -> DispatchResult {
    take_back(&authorization).unwrap_or(DispatchResult::Revoked(authorization))
}
