//! Open and activate a session through its owner binding.

use super::{OwnerRun, SessionOwnerExecution};
use crate::session_owner_lifecycle_reports::LifecycleObservationScope;
use acp_client_runtime::session_owner_task::{OwnerCompletionPort, OwnerTaskError};
use provider_session_machines::{
    ProviderSettingsCatalog,
    harness_lifecycle::{
        AUTO_PERMISSION_MODE, AcceptedRunSettings, ActivationRefusal, ActivationSetup,
        CreateAttempt, HarnessSessionId, LifecycleRefusal, LifecycleReply, OpenAttempt,
        OpenRequest, OpeningFact, OpeningFailure, RestoreSettingRefusal, RestoredOpening,
        RunRetirement, SessionCreatedSetupFact, SettingAttempt, owner_event_for_restore_setting,
        owner_event_for_unsent_restore_setting,
    },
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_owner::{
        OwnerAddress, OwnerEvent, OwnerSetting, PreparationStage, ReplyAdmission, RestoreFailure,
        RestorePreparation, RunAttempt, SessionWireResult, WireOutcome, WriterState,
    },
    session_setting_machine::SettingValueId,
};
use std::{path::PathBuf, sync::Arc};
use tokio_util::sync::CancellationToken;

impl SessionOwnerExecution {
    pub(super) fn restore(
        &mut self,
        owner: OwnerAddress,
        run: RunAttempt<LoadAttempt>,
        via: RestoreVia,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let Some(saved) = self.storage.take_setup(run.attempt) else {
            return Ok(Some(restore_failed(
                run,
                RestoreFailure::LocalPrerequisite {
                    stage: PreparationStage::SavedSetup,
                },
            )));
        };
        let completion = completions
            .reserve(owner)?
            .map_if(move |fact| restore_reply(run, via, fact));
        let cwd = self
            .storage
            .explicit_working_directory(run.attempt)
            .unwrap_or_else(|| PathBuf::from(String::from(saved.working_directory.clone())));
        let setup =
            crate::provider_session_setup::restore_setup(&self.work_roots, cwd, &saved.resume);
        let requested_mode = saved.resume.requested_mode;
        let router_auto_requested = requested_mode.as_deref() == Some(AUTO_PERMISSION_MODE);
        let cancel = CancellationToken::new();
        let events = self
            .observations
            .observed_at(LifecycleObservationScope::Restoring(run), cancel.clone());
        if self
            .binding
            .open(
                OpenRequest::Restore {
                    owner,
                    run,
                    session: HarnessSessionId(self.session_id()),
                    via,
                    setup,
                    requested_mode,
                },
                completion,
                events,
                Box::pin(cancel.clone().cancelled_owned()),
            )
            .is_err()
        {
            return Ok(Some(restore_failed(run, RestoreFailure::NotEnqueued)));
        }
        self.routes.insert(
            run.generation,
            OwnerRun {
                cancel,
                router_auto_requested,
                approval: None,
            },
        );
        Ok(None)
    }

    pub(super) fn restore_setting(
        &self,
        address: OwnerAddress,
        run: RunAttempt<LoadAttempt>,
        setting: OwnerSetting,
        value: SettingValueId,
        catalogue: &ProviderSettingsCatalog,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        let attempt = SettingAttempt {
            restore: run,
            setting,
            value,
            catalogue: catalogue.clone().into_record(),
        };
        match self
            .binding
            .put_setting_back(attempt, &mut || match completions.reserve(address) {
                Ok(completion) => {
                    ReplyAdmission::Reserved(completion.map(owner_event_for_restore_setting))
                }
                Err(OwnerTaskError::CompletionCapacity) => {
                    ReplyAdmission::Refused(WriterState::Full)
                }
                Err(_) => ReplyAdmission::Refused(WriterState::Stopped),
            }) {
            Ok(()) => Ok(None),
            Err(RestoreSettingRefusal::Harness(_)) => {
                Ok(Some(owner_event_for_unsent_restore_setting(run, setting)))
            }
            Err(RestoreSettingRefusal::ReplyReservation(WriterState::Full)) => {
                Err(OwnerTaskError::CompletionCapacity)
            }
            Err(RestoreSettingRefusal::ReplyReservation(WriterState::Stopped)) => {
                Err(OwnerTaskError::ExecutorStopped)
            }
        }
    }

    pub(crate) fn activate(
        &mut self,
        setup: ActivationSetup,
        accepted: AcceptedRunSettings,
    ) -> Result<(), ActivationRefusal> {
        let (generation, cancel) = match setup {
            ActivationSetup::Created { generation, .. } => (generation, CancellationToken::new()),
            ActivationSetup::Restored(run) => (
                run.generation,
                self.routes
                    .get(&run.generation)
                    .ok_or(ActivationRefusal::OpeningNotHeld)?
                    .cancel
                    .clone(),
            ),
        };
        let router_auto_requested = accepted.router_auto_requested;
        let events = self.observations.observed_at(
            LifecycleObservationScope::Activated(generation),
            cancel.clone(),
        );
        self.binding.activate(
            setup,
            accepted,
            events,
            Box::pin(cancel.clone().cancelled_owned()),
        )?;
        if matches!(setup, ActivationSetup::Created { .. }) {
            self.routes.insert(
                generation,
                OwnerRun {
                    cancel,
                    router_auto_requested,
                    approval: None,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn configure_created(
        &self,
        attempt: CreateAttempt,
        reply: LifecycleReply<SessionCreatedSetupFact>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        self.binding.configure_created(attempt, reply, retired)
    }

    pub(super) fn prepare_harness_settings(
        &self,
        owner: OwnerAddress,
        run: RunAttempt<LoadAttempt>,
        catalogue: Arc<ProviderSettingsCatalog>,
        completions: &OwnerCompletionPort,
    ) -> Result<Option<OwnerEvent>, OwnerTaskError> {
        #[cfg(not(any(test, feature = "test-support")))]
        let _ = (owner, completions);
        let accepted = AcceptedRunSettings {
            catalogue: catalogue.as_ref().clone().into_record(),
            router_auto_requested: self
                .routes
                .get(&run.generation)
                .is_some_and(|route| route.router_auto_requested),
        };
        let preparation = match self
            .binding
            .accept_settings(OpenAttempt::Restore(run), &accepted)
        {
            Ok(report) => RestorePreparation::HostReady {
                run,
                capabilities: report.capabilities,
                settings: report.settings,
                observed: report.observed,
            },
            Err(_) => RestorePreparation::Failed {
                run,
                stage: PreparationStage::HostSetup,
            },
        };
        #[cfg(any(test, feature = "test-support"))]
        {
            let failed = move || RestorePreparation::Failed {
                run,
                stage: PreparationStage::HostSetup,
            };
            let fixture = self.preparation_fixture.clone();
            if let Some(release) = fixture.hold_restore() {
                let completion = completions.reserve(owner)?;
                self.tasks.spawn(async move {
                    let _released = release.await;
                    let result = if fixture.take_restore_fault() {
                        failed()
                    } else {
                        preparation
                    };
                    completion.send(OwnerEvent::Prepare(result));
                    #[cfg(test)]
                    fixture.witness_restore_completion_queued();
                });
                return Ok(None);
            }
            if fixture.take_restore_fault() {
                return Ok(Some(OwnerEvent::Prepare(failed())));
            }
        }
        Ok(Some(OwnerEvent::Prepare(preparation)))
    }
}

pub(super) fn lifecycle_settings(
    settings: &provider_session_machines::session_owner::AcceptedRunSettings,
) -> AcceptedRunSettings {
    AcceptedRunSettings {
        catalogue: settings.catalogue.as_ref().clone().into_record(),
        router_auto_requested: settings.router_auto_requested,
    }
}

fn restore_reply(
    expected: RunAttempt<LoadAttempt>,
    via: RestoreVia,
    fact: OpeningFact,
) -> Option<OwnerEvent> {
    match fact {
        OpeningFact::Restored { run, opening } if expected == run => {
            let (readback, replay) = match (via, opening) {
                (
                    RestoreVia::Load,
                    RestoredOpening::Loaded {
                        readback, replay, ..
                    },
                ) => (readback, Some(replay)),
                (RestoreVia::Resume, RestoredOpening::Resumed { readback, .. }) => (readback, None),
                _ => return None,
            };
            Some(OwnerEvent::Wire(SessionWireResult::Restore {
                run,
                replay,
                outcome: WireOutcome::Confirmed(
                    provider_session_machines::session_owner::RestoreLoadReadback {
                        catalogue: Arc::new(ProviderSettingsCatalog::from_record(
                            readback.catalogue,
                        )),
                        router_auto_requested: readback.router_auto_requested,
                        mode: readback.mode,
                        fast_on: readback.fast_on,
                    },
                ),
            }))
        }
        OpeningFact::RestoreFailed { run, failure } if expected == run => Some(restore_failed(
            run,
            match failure {
                OpeningFailure::Lost { reported } => RestoreFailure::Lost { reported },
                OpeningFailure::Refused(failure) => RestoreFailure::Refused(failure),
                OpeningFailure::ProviderError(failure) => RestoreFailure::ProviderError(failure),
                OpeningFailure::NoAnswer => RestoreFailure::NoAnswer,
                OpeningFailure::Closed => RestoreFailure::Closed,
                OpeningFailure::ReplayNotKnown => RestoreFailure::LocalPrerequisite {
                    stage: PreparationStage::Route,
                },
                OpeningFailure::Disconnected => RestoreFailure::Disconnected,
                OpeningFailure::OutcomeUnknown | OpeningFailure::Unavailable { .. } => {
                    RestoreFailure::OutcomeUnknown
                }
            },
        )),
        _ => None,
    }
}

fn restore_failed(run: RunAttempt<LoadAttempt>, failure: RestoreFailure) -> OwnerEvent {
    OwnerEvent::Wire(SessionWireResult::RestoreFailed { run, failure })
}
