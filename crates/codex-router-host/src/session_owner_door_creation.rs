//! Adopt the provider ID before releasing a provisional create.

use super::SessionOwnerDoor;
use crate::{
    session_owner_execution::SessionOwnerExecution,
    session_owner_lifecycle_reports::{BoundSessionHarness, LifecycleObservationScope},
};
use acp_client_runtime::session_owner_task::{
    OwnerCallerWakes, OwnerCompletion, OwnerCompletionPort, OwnerTaskError, SessionOwnerPorts,
};
use collaboration_protocol::{EndpointRef, OperationId, SessionId, SessionRef};
use provider_session_machines::session_refusal_answers::effect_in_progress_answer;
use provider_session_machines::{
    EffectiveProviderSettings, ProviderSettingsCatalog,
    harness_lifecycle::{
        AcceptedRunSettings, ActivationRefusal, ActivationSetup, CreateAttempt,
        CreatedSettingsFailure, CreatedSetupFact, CreationEffect, HarnessSessionId, Held,
        InvalidRequestedSetting, OpenAttempt, OpenRequest, OpenSetup, OpeningFact, OpeningFailure,
        PermissionModeReport, RequestedProviderSettings, ReservedReply,
    },
    session_owner::{
        CreateOutcome, CreateProgress, CreatedSessionFacts, OwnerAddress, OwnerEffect, OwnerEvent,
        PublicationBatch, RemoteEffect, SessionOwnerCommand, SessionOwnerRefusal,
        SessionOwnerRejection, SessionWireRequest,
    },
};
use session_event_model::ProviderSettingFailureReason;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

type PreparedCreate = Result<CreatePreparation, OwnerCreateFailure>;

enum CreatePreparation {
    Opened,
    Configured(Box<OwnerCreateReport>),
}

pub(crate) struct OwnerCreateReport {
    pub(crate) session: HarnessSessionId,
    pub(crate) effective_settings: EffectiveProviderSettings,
    pub(crate) permission_mode: PermissionModeReport,
    pub(crate) settings_failure: Option<CreatedSettingsFailure<ProviderSettingFailureReason>>,
}

pub(crate) enum OwnerCreateFailure {
    TransportUnavailable,
    Operation(String),
    Opening(OpeningFailure),
    Activation {
        reason: ActivationRefusal,
        session: HarnessSessionId,
        permission_mode: PermissionModeReport,
    },
    ClosedAfterInvalidSetting(InvalidRequestedSetting),
}

struct ProvisionalCreate {
    door: std::sync::Weak<SessionOwnerDoor>,
    endpoint: EndpointRef,
    operation: OperationId,
    input: Option<(OpenSetup, RequestedProviderSettings)>,
    work_roots: crate::provider_work_roots::ProviderWorkRoots,
    bound: Option<BoundSessionHarness>,
    prepared: Arc<Mutex<Option<PreparedCreate>>>,
    rebase: Option<PublicationBatch>,
    settings: Option<OwnerCreateReport>,
    adopted: Option<SessionOwnerExecution>,
    response: Option<oneshot::Sender<Result<OwnerCreateReport, OwnerCreateFailure>>>,
    stop: CancellationToken,
    configuring: Option<CancellationToken>,
}

impl SessionOwnerDoor {
    #[cfg(test)]
    pub(crate) fn provisional_snapshots_for_test(
        &self,
        operation: &OperationId,
    ) -> Option<
        tokio::sync::watch::Receiver<provider_session_machines::session_owner::SessionSnapshot>,
    > {
        self.registry
            .provisional_owner(operation)
            .map(|owner| owner.handle.snapshots())
    }

    pub(crate) async fn create_with_harness(
        self: &Arc<Self>,
        endpoint: EndpointRef,
        operation: OperationId,
        setup: OpenSetup,
        requested: RequestedProviderSettings,
        work_roots: crate::provider_work_roots::ProviderWorkRoots,
    ) -> Result<OwnerCreateReport, OwnerCreateFailure> {
        if self.retired.is_cancelled() {
            return Err(OwnerCreateFailure::TransportUnavailable);
        }
        let correlation = Uuid::parse_str(operation.as_str())
            .map_err(|_| OwnerCreateFailure::Operation("invalid create identity".to_owned()))?;
        let launch = self
            .registry
            .begin_create(operation.clone(), self.connection)
            .map_err(|error| OwnerCreateFailure::Operation(error.to_string()))?;
        let admitted = launch
            .owner
            .handle
            .try_submit(
                launch.owner.address,
                SessionOwnerCommand::Create {
                    operation: correlation,
                },
            )
            .map_err(|_| OwnerCreateFailure::TransportUnavailable)?;
        let (response, result) = oneshot::channel();
        let stop = self.shutdown.child_token();
        let recover_roots = work_roots.clone();
        let provisional_operation = operation.clone();
        let ports = ProvisionalCreate {
            door: Arc::downgrade(self),
            endpoint,
            operation,
            input: Some((setup, requested)),
            work_roots,
            bound: Some(self.bind(&launch.owner, stop.clone())),
            prepared: Arc::default(),
            rebase: None,
            settings: None,
            adopted: None,
            response: Some(response),
            stop: stop.clone(),
            configuring: None,
        };
        let door = Arc::downgrade(self);
        let incarnation = launch.owner.address.incarnation;
        self.tasks.spawn(async move {
            let outcome = launch.task.run(ports, stop).await;
            if let Err(error) = &outcome {
                tracing::debug!(%error, "ACP create owner stopped");
            }
            if let Some(door) = door.upgrade() {
                let owner = door
                    .sessions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .find(|(_, entry)| entry.owner.address.incarnation == incarnation)
                    .map(|(target, entry)| (target.clone(), entry.clone()));
                if let Some((target, owner)) = owner {
                    if outcome.is_err()
                        && let Some((task, ports, replacement)) =
                            door.recover_session(&target, &owner, recover_roots.clone())
                    {
                        door.run_registered(target, task, ports, replacement, recover_roots);
                    } else {
                        owner.calls.stop();
                    }
                } else {
                    let _retired = door
                        .registry
                        .retire_create(&provisional_operation, incarnation);
                }
            }
        });
        admitted
            .await
            .map_err(|_| OwnerCreateFailure::TransportUnavailable)?
            .map_err(|refusal| {
                let answer = match refusal.rejection {
                    SessionOwnerRejection::Refused(SessionOwnerRefusal::EffectInProgress(
                        effect,
                    )) => effect_in_progress_answer(effect, &refusal.snapshot),
                    other => format!("the session owner refused create: {other:?}"),
                };
                OwnerCreateFailure::Operation(answer)
            })?;
        result
            .await
            .map_err(|_| OwnerCreateFailure::TransportUnavailable)?
    }
}

impl ProvisionalCreate {
    fn start(
        &mut self,
        address: OwnerAddress,
        operation: Uuid,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let door = self.door.upgrade().ok_or(OwnerTaskError::ExecutorStopped)?;
        let (setup, requested) = self.input.take().ok_or(OwnerTaskError::ExecutorStopped)?;
        let completion = completions.reserve(address)?;
        let bound = self.bound.as_mut().ok_or(OwnerTaskError::ExecutorStopped)?;
        let reply = Box::new(CreateOpeningReply {
            operation,
            completion,
            prepared: self.prepared.clone(),
            #[cfg(test)]
            stop: self.stop.clone(),
            #[cfg(test)]
            tasks: door.tasks.clone(),
            #[cfg(test)]
            preparation_fixture: door.preparation_fixture.clone(),
        });
        #[cfg(not(test))]
        let _door = door;
        let events = bound
            .observations
            .observed_at(LifecycleObservationScope::Creating, self.stop.clone());
        let request = OpenRequest::Create {
            owner: address,
            attempt: CreateAttempt(operation),
            setup,
            requested,
        };
        if bound
            .binding
            .open(
                request,
                reply,
                events,
                Box::pin(self.stop.clone().cancelled_owned()),
            )
            .is_err()
        {
            return Ok(Some(OwnerEvent::Create(CreateProgress::Failed {
                operation,
                remote: RemoteEffect::None,
            })));
        }
        Ok(None)
    }

    fn adopt(
        &mut self,
        address: OwnerAddress,
        operation: Uuid,
        provider_id: String,
        completions: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let door = self.door.upgrade().ok_or(OwnerTaskError::ExecutorStopped)?;
        let id = match SessionId::try_from(provider_id) {
            Ok(id) => id,
            Err(_) => {
                return Ok(Some(OwnerEvent::Create(CreateProgress::Failed {
                    operation,
                    remote: RemoteEffect::Applied,
                })));
            }
        };
        let target = SessionRef {
            endpoint: self.endpoint.clone(),
            session_id: id,
        };
        let Some(Ok(CreatePreparation::Opened)) = take(&self.prepared) else {
            return Err(OwnerTaskError::ExecutorStopped);
        };
        if self.bound.as_ref().is_none_or(|bound| {
            bound
                .binding
                .opening_is_held(OpenAttempt::Create(CreateAttempt(operation)))
                != Held::Held
        }) {
            return Err(OwnerTaskError::ExecutorStopped);
        }
        let mut sessions = door
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (registered, lane) =
            match door
                .registry
                .adopt(&self.operation, address.incarnation, target.clone())
            {
                Ok(adopted) => adopted,
                Err(crate::session_owner_registry::SessionOwnerRegistryError::AlreadyOwned) => {
                    return Ok(Some(OwnerEvent::Create(CreateProgress::AlreadyOwned {
                        operation,
                    })));
                }
                Err(_) => return Err(OwnerTaskError::ExecutorStopped),
            };
        let (owner, mut ports) = door
            .prepare_adopted_with_binding(
                target.clone(),
                registered,
                Some(lane),
                None,
                self.work_roots.clone(),
                self.bound.take().ok_or(OwnerTaskError::ExecutorStopped)?,
            )
            .map_err(|_| OwnerTaskError::ExecutorStopped)?;
        if let Some(batch) = self.rebase.take()
            && let Some(result) = ports.execute(
                address,
                OwnerEffect::Publish(batch),
                completions,
                caller_wakes,
            )?
        {
            completions.reserve(address)?.send(result);
        }
        let session = HarnessSessionId(String::from(target.session_id.clone()));
        sessions.insert(target, owner);
        self.adopted = Some(ports);
        drop(sessions);
        self.configure(address, operation, session, completions)?;
        Ok(None)
    }

    fn configure(
        &mut self,
        address: OwnerAddress,
        operation: Uuid,
        session: HarnessSessionId,
        completions: &OwnerCompletionPort,
    ) -> Result<(), OwnerTaskError> {
        let prepared = self.prepared.clone();
        let completion = completions.reserve(address)?.map_if(move |fact| {
            let (report, settings_failure) = match fact {
                CreatedSetupFact::Configured { attempt, report } if attempt.0 == operation => {
                    (report, None)
                }
                CreatedSetupFact::RemainsCreated {
                    attempt,
                    report,
                    failure,
                } if attempt.0 == operation => (report, Some(failure)),
                CreatedSetupFact::ClosedAfterInvalidSetting { attempt, failure }
                    if attempt.0 == operation =>
                {
                    save(
                        &prepared,
                        Some(Err(OwnerCreateFailure::ClosedAfterInvalidSetting(failure))),
                    );
                    return Some(OwnerEvent::Create(CreateProgress::Failed {
                        operation,
                        remote: RemoteEffect::Applied,
                    }));
                }
                _ => return None,
            };
            let facts = Box::new(CreatedSessionFacts {
                capabilities: report.setup.capabilities,
                settings: report.setup.settings,
                observed: report.setup.observed,
                catalogue: Arc::new(ProviderSettingsCatalog::from_record(report.catalogue)),
                router_auto_requested: report.router_auto_requested,
                mode_reason: report.permission_mode.reason,
                keep_fast_off: report.keep_fast_off,
                failed_setting: report.failed_setting,
            });
            save(
                &prepared,
                Some(Ok(CreatePreparation::Configured(Box::new(
                    OwnerCreateReport {
                        session,
                        effective_settings: report.effective_settings,
                        permission_mode: report.permission_mode,
                        settings_failure,
                    },
                )))),
            );
            Some(OwnerEvent::Create(CreateProgress::Configured {
                operation,
                facts,
            }))
        });
        let stop = self.stop.child_token();
        self.configuring = Some(stop.clone());
        self.adopted
            .as_ref()
            .ok_or(OwnerTaskError::ExecutorStopped)?
            .configure_created(
                CreateAttempt(operation),
                completion,
                Box::pin(stop.cancelled_owned()),
            )
            .map_err(|_| OwnerTaskError::ExecutorStopped)
    }

    fn reply(
        &mut self,
        address: OwnerAddress,
        outcome: CreateOutcome,
        caller_wakes: &mut OwnerCallerWakes,
    ) {
        if let Some(configuring) = self.configuring.take() {
            configuring.cancel();
        }
        let result = match outcome {
            CreateOutcome::Adopted(_) => self
                .settings
                .take()
                .ok_or(OwnerCreateFailure::TransportUnavailable),
            CreateOutcome::SessionIdInUse => Err(OwnerCreateFailure::Operation(
                "this session id is already in use".to_owned(),
            )),
            CreateOutcome::Failed(_) => match take(&self.prepared) {
                Some(Err(error)) => Err(error),
                _ => Err(OwnerCreateFailure::TransportUnavailable),
            },
        };
        if let Some(response) = self.response.take() {
            caller_wakes.send(response, result);
        }
        if self.adopted.is_none() {
            if let Some(door) = self.door.upgrade() {
                let _retired = door
                    .registry
                    .retire_create(&self.operation, address.incarnation);
            }
            self.stop.cancel();
        }
    }
}

impl SessionOwnerPorts for ProvisionalCreate {
    fn execute_dispatch_mark(
        &mut self,
        address: OwnerAddress,
        mark: provider_session_machines::session_owner::TurnDispatchMark,
        deadline: router_clock::ClockInstant,
        completions: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        self.adopted
            .as_mut()
            .ok_or(OwnerTaskError::ExecutorStopped)?
            .execute_dispatch_mark(address, mark, deadline, completions, caller_wakes)
    }

    fn execute(
        &mut self,
        address: OwnerAddress,
        effect: OwnerEffect,
        completions: &OwnerCompletionPort,
        caller_wakes: &mut OwnerCallerWakes,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        match effect {
            OwnerEffect::Wire(SessionWireRequest::Create { operation }) => {
                self.start(address, operation, completions)
            }
            OwnerEffect::AdoptProviderId {
                operation,
                provider_id,
            } => self.adopt(address, operation, provider_id, completions, caller_wakes),
            OwnerEffect::CreateSettled { outcome, .. } => {
                self.reply(address, outcome, caller_wakes);
                Ok(None)
            }
            OwnerEffect::Publish(batch) if self.adopted.is_none() => {
                self.rebase = Some(batch);
                Ok(None)
            }
            OwnerEffect::ActivateCreated {
                operation,
                generation,
                facts,
            } => {
                let ports = self
                    .adopted
                    .as_mut()
                    .ok_or(OwnerTaskError::ExecutorStopped)?;
                let Some(Ok(CreatePreparation::Configured(settings))) = take(&self.prepared) else {
                    return Err(OwnerTaskError::ExecutorStopped);
                };
                let setup = ActivationSetup::Created {
                    attempt: CreateAttempt(operation),
                    generation,
                };
                let accepted = AcceptedRunSettings {
                    catalogue: facts.catalogue.as_ref().clone().into_record(),
                    router_auto_requested: facts.router_auto_requested,
                };
                let progress = match ports.activate(setup, accepted) {
                    Ok(()) => {
                        self.settings = Some(*settings);
                        CreateProgress::Activated { operation, facts }
                    }
                    Err(reason) => {
                        save(
                            &self.prepared,
                            Some(Err(OwnerCreateFailure::Activation {
                                reason,
                                session: settings.session,
                                permission_mode: settings.permission_mode,
                            })),
                        );
                        CreateProgress::Failed {
                            operation,
                            remote: RemoteEffect::Applied,
                        }
                    }
                };
                Ok(Some(OwnerEvent::Create(progress)))
            }
            other => self
                .adopted
                .as_mut()
                .ok_or(OwnerTaskError::ExecutorStopped)?
                .execute(address, other, completions, caller_wakes),
        }
    }
}

struct CreateOpeningReply {
    operation: Uuid,
    completion: OwnerCompletion,
    prepared: Arc<Mutex<Option<PreparedCreate>>>,
    #[cfg(test)]
    stop: CancellationToken,
    #[cfg(test)]
    tasks: tokio_util::task::TaskTracker,
    #[cfg(test)]
    preparation_fixture: Arc<crate::session_owner_preparation_fixture::PreparationFixture>,
}

impl ReservedReply<OpeningFact> for CreateOpeningReply {
    fn send(self: Box<Self>, reply: OpeningFact) {
        let operation = self.operation;
        let (prepared, progress) = match reply {
            OpeningFact::Created { attempt, session } if attempt.0 == operation => (
                Ok(CreatePreparation::Opened),
                CreateProgress::ProviderId {
                    operation,
                    provider_id: session.0,
                },
            ),
            OpeningFact::CreateFailed {
                attempt,
                remote,
                failure,
            } if attempt.0 == operation => (
                Err(OwnerCreateFailure::Opening(failure)),
                CreateProgress::Failed {
                    operation,
                    remote: match remote {
                        CreationEffect::None => RemoteEffect::None,
                        CreationEffect::Applied => RemoteEffect::Applied,
                        CreationEffect::Unknown => RemoteEffect::Unknown,
                    },
                },
            ),
            _ => return,
        };
        #[cfg(test)]
        if prepared.is_ok()
            && let Some(release) = self.preparation_fixture.hold_create()
        {
            let Self {
                stop,
                tasks,
                completion,
                prepared: slot,
                ..
            } = *self;
            tasks.spawn(async move {
                tokio::select! {
                    _released = release => {}
                    () = stop.cancelled() => return,
                }
                save(&slot, Some(prepared));
                completion.send(OwnerEvent::Create(progress));
            });
            return;
        }
        save(&self.prepared, Some(prepared));
        self.completion.send(OwnerEvent::Create(progress));
    }
}

fn take<T>(value: &Mutex<Option<T>>) -> Option<T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

fn save<T>(value: &Mutex<Option<T>>, result: Option<T>) {
    *value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = result;
}
