//! Held ACP openings remain with their owner through adoption.

#[path = "owner_acp_lifecycle_activation.rs"]
mod activation;

use super::{lifecycle_transport_refusal, transport_run};
use crate::{
    ExternalProviderRuntimeError, RequestedProviderSettings,
    owner_session_creation::catalogue_evidence,
    owner_session_open::OwnerOpenedSession,
    owner_session_setting::{OwnerSettingWirePlan, RestoreSettingPlan},
    owner_wire::OwnerAcpWire,
};
use provider_session_machines::{
    ProviderSettingsCatalog, SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, CreateAttempt, CreatedSetupFact, CreationEffect, HarnessSessionId,
        Held, LifecycleRefusal, LifecycleReply, OpenAttempt, OpenRequest, OpenSetup, OpeningFact,
        OpeningFailure, RestoreLoadReadback, RestoreSettingRefusal, RestoredOpening, RunRetirement,
        SessionCreatedSetupFact, SessionLifecycleFact, SessionLifecyclePort, SettingAttempt,
        SettingFact, SetupReport,
    },
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_owner::{OwnerAddress, ProviderWireFailureKind, RunAttempt, TurnMembership},
    session_setting_machine::SettingValueId,
};
use router_clock::RuntimeClock;
use session_event_model::{CapabilityReport, SessionSettings, SkillView};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};
use switchboard_harness_contracts::{ReplyAdmission, WorkOfferRefusal};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

pub(super) struct OwnerOpenings {
    owner: OwnerAddress,
    wire: Arc<OwnerAcpWire>,
    session: Arc<OnceLock<String>>,
    clock: Arc<dyn RuntimeClock>,
    tasks: TaskTracker,
    stopped: CancellationToken,
    attempts: Arc<Mutex<OpeningAttempts>>,
}

#[derive(Default)]
struct OpeningAttempts {
    creates: BTreeMap<Uuid, CreateOpening>,
    runs: BTreeMap<SessionGeneration, OpenedRun>,
}

enum CreateOpening {
    Opening,
    Held(Box<HeldCreate>),
    Configuring,
    Configured(Box<OwnerOpenedSession>),
}

struct HeldCreate {
    session: OwnerOpenedSession,
    requested: RequestedProviderSettings,
}

struct OpenedRun {
    attempt: OpenAttempt,
    stopped: CancellationToken,
    phase: RunOpening,
}

enum RunOpening {
    Opening,
    Held(Box<HeldRestore>),
    Active,
}

struct HeldRestore {
    session: OwnerOpenedSession,
    accepted_mode_fallback: Option<String>,
    skills: Option<SkillView>,
}

struct RestoreOpening {
    run: RunAttempt<LoadAttempt>,
    session: HarnessSessionId,
    via: RestoreVia,
    setup: OpenSetup,
    requested_mode: Option<String>,
}

impl OwnerOpenings {
    pub(super) fn new(
        owner: OwnerAddress,
        wire: Arc<OwnerAcpWire>,
        session: Arc<OnceLock<String>>,
        clock: Arc<dyn RuntimeClock>,
        tasks: TaskTracker,
    ) -> Self {
        Self {
            owner,
            wire,
            session,
            clock,
            tasks,
            stopped: CancellationToken::new(),
            attempts: Arc::default(),
        }
    }

    pub(super) fn owner(&self) -> OwnerAddress {
        self.owner
    }

    pub(super) fn open(
        &self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        match request {
            OpenRequest::Create {
                owner,
                attempt,
                setup,
                requested,
            } if owner == self.owner => self.open_create(attempt, setup, requested, reply, retired),
            OpenRequest::Restore {
                owner,
                run,
                session,
                via,
                setup,
                requested_mode,
            } if owner == self.owner => self.open_restore(
                RestoreOpening {
                    run,
                    session,
                    via,
                    setup,
                    requested_mode,
                },
                reply,
                events,
                retired,
            ),
            _ => Err(LifecycleRefusal::AttemptMismatch),
        }
    }

    fn open_create(
        &self,
        attempt: CreateAttempt,
        setup: OpenSetup,
        requested: RequestedProviderSettings,
        reply: LifecycleReply<OpeningFact>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        let mut attempts = lock(&self.attempts);
        if self.session.get().is_some() || !attempts.creates.is_empty() {
            return Err(LifecycleRefusal::AttemptMismatch);
        }
        let receipt = self
            .wire
            .create(self.owner.incarnation, setup)
            .map_err(lifecycle_transport_refusal)?;
        attempts.creates.insert(attempt.0, CreateOpening::Opening);
        drop(attempts);

        let attempts = Arc::clone(&self.attempts);
        let owned_session = Arc::clone(&self.session);
        let stopped = self.stopped.clone();
        self.tasks.spawn(async move {
            let result = tokio::select! {
                result = receipt => result,
                () = retired => {
                    lock(&attempts).creates.remove(&attempt.0);
                    return;
                }
                () = stopped.cancelled() => return,
            };
            let fact = match result {
                Ok(Ok(session)) => {
                    let session_id = session.provider_id();
                    let mut attempts = lock(&attempts);
                    let Some(held @ CreateOpening::Opening) = attempts.creates.get_mut(&attempt.0)
                    else {
                        return;
                    };
                    if stopped.is_cancelled() || owned_session.set(session_id.clone()).is_err() {
                        return;
                    }
                    *held = CreateOpening::Held(Box::new(HeldCreate { session, requested }));
                    OpeningFact::Created {
                        attempt,
                        session: HarnessSessionId(session_id),
                    }
                }
                Ok(Err(error)) => {
                    lock(&attempts).creates.remove(&attempt.0);
                    let remote = if error
                        .coded_wire_failure()
                        .is_some_and(|failure| failure.definitely_no_effect())
                    {
                        CreationEffect::None
                    } else {
                        CreationEffect::Unknown
                    };
                    OpeningFact::CreateFailed {
                        attempt,
                        remote,
                        failure: opening_failure(error),
                    }
                }
                Err(_) => {
                    lock(&attempts).creates.remove(&attempt.0);
                    OpeningFact::CreateFailed {
                        attempt,
                        remote: CreationEffect::Unknown,
                        failure: OpeningFailure::OutcomeUnknown,
                    }
                }
            };
            reply.send(fact);
        });
        Ok(())
    }

    fn open_restore(
        &self,
        opening: RestoreOpening,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        let RestoreOpening {
            run,
            session,
            via,
            setup,
            requested_mode,
        } = opening;
        let mut attempts = lock(&self.attempts);
        if !attempts.creates.is_empty() || attempts.runs.contains_key(&run.generation) {
            return Err(LifecycleRefusal::AttemptMismatch);
        }
        if self.session.get_or_init(|| session.0.clone()) != &session.0 {
            return Err(LifecycleRefusal::AttemptMismatch);
        }
        let transport = transport_run(self.owner, run.generation);
        self.wire.open_route(transport, session.0.clone());
        let receipt = self
            .wire
            .restore(transport, run.attempt, session.0, via, setup);
        #[cfg(feature = "test-observation")]
        self.wire
            .fence_dispatch_for_test(transport, crate::owner_wire::FixtureDispatchPoint::Restore);
        let receipt = receipt.map_err(lifecycle_transport_refusal)?;
        let stopped = self.stopped.child_token();
        attempts.runs.insert(
            run.generation,
            OpenedRun {
                attempt: OpenAttempt::Restore(run),
                stopped: stopped.clone(),
                phase: RunOpening::Opening,
            },
        );
        drop(attempts);

        let attempts = Arc::clone(&self.attempts);
        self.tasks.spawn(async move {
            let open = async {
                let mut session = match receipt.await {
                    Ok(Ok(session)) => session,
                    Ok(Err(error)) => return Err(opening_failure(error)),
                    Err(_) => return Err(OpeningFailure::OutcomeUnknown),
                };
                let (skills, completeness) = if via == RestoreVia::Load {
                    let replay = session
                        .prepare_replay()
                        .map_err(|_| OpeningFailure::ReplayNotKnown)?;
                    if events
                        .send(SessionLifecycleFact::Replay {
                            run,
                            events: replay.events,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(None);
                    }
                    (replay.skills, Some(replay.completeness))
                } else {
                    (None, None)
                };
                let RestoreSettingPlan {
                    mode,
                    accepted_mode_fallback,
                    fast_off,
                } = session.restore_setting_plan(requested_mode.as_deref());
                let readback = RestoreLoadReadback {
                    catalogue: session.catalog().clone().into_record(),
                    router_auto_requested: requested_mode.as_deref()
                        == Some(crate::AUTO_PERMISSION_MODE),
                    mode,
                    fast_on: fast_off,
                };
                let reported_fallback = accepted_mode_fallback.clone().map(SettingValueId);
                let mut attempts = lock(&attempts);
                let Some(entry) = attempts.runs.get_mut(&run.generation).filter(|entry| {
                    entry.attempt == OpenAttempt::Restore(run)
                        && !entry.stopped.is_cancelled()
                        && matches!(entry.phase, RunOpening::Opening)
                }) else {
                    return Ok(None);
                };
                entry.phase = RunOpening::Held(Box::new(HeldRestore {
                    session,
                    accepted_mode_fallback,
                    skills,
                }));
                let opening = match completeness {
                    Some(replay) => RestoredOpening::Loaded {
                        readback,
                        accepted_mode_fallback: reported_fallback,
                        replay,
                    },
                    None => RestoredOpening::Resumed {
                        readback,
                        accepted_mode_fallback: reported_fallback,
                    },
                };
                Ok(Some(opening))
            };
            tokio::select! {
                biased;
                () = stopped.cancelled() => {}
                () = retired => stopped.cancel(),
                result = open => match result {
                    Ok(Some(opening)) => reply.send(OpeningFact::Restored { run, opening }),
                    Ok(None) => {}
                    Err(failure) => reply.send(OpeningFact::RestoreFailed { run, failure }),
                },
            }
        });
        Ok(())
    }

    pub(super) fn configure_created(
        &self,
        attempt: CreateAttempt,
        reply: LifecycleReply<SessionCreatedSetupFact>,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        let mut attempts = lock(&self.attempts);
        let Some(held @ CreateOpening::Held(_)) = attempts.creates.get_mut(&attempt.0) else {
            return Err(LifecycleRefusal::OpeningNotHeld);
        };
        let CreateOpening::Held(held) = std::mem::replace(held, CreateOpening::Configuring) else {
            return Err(LifecycleRefusal::OpeningNotHeld);
        };
        drop(attempts);

        let HeldCreate {
            mut session,
            requested,
        } = *held;
        let stopped = self.stopped.clone();
        let attempts = Arc::clone(&self.attempts);
        let clock = Arc::clone(&self.clock);
        self.tasks.spawn(async move {
            let fact = tokio::select! {
                biased;
                () = stopped.cancelled() => return,
                () = retired => {
                    lock(&attempts).creates.remove(&attempt.0);
                    return;
                }
                result = session.configure_lifecycle_create(attempt, &requested, clock.as_ref()) => result,
            };
            {
                let mut attempts = lock(&attempts);
                let Some(held @ CreateOpening::Configuring) = attempts.creates.get_mut(&attempt.0) else {
                    return;
                };
                if matches!(fact, CreatedSetupFact::ClosedAfterInvalidSetting { .. }) {
                    attempts.creates.remove(&attempt.0);
                } else {
                    *held = CreateOpening::Configured(Box::new(session));
                }
            }
            reply.send(fact);
        });
        Ok(())
    }

    pub(super) fn opening_is_held(&self, attempt: OpenAttempt) -> Held {
        let attempts = lock(&self.attempts);
        let held = match attempt {
            OpenAttempt::Create(attempt) => matches!(
                attempts.creates.get(&attempt.0),
                Some(CreateOpening::Held(_) | CreateOpening::Configured(_))
            ),
            OpenAttempt::Restore(run) => held_restore(&attempts, run).is_some(),
        };
        if held {
            Held::Held
        } else {
            Held::NotHeld(LifecycleRefusal::OpeningNotHeld)
        }
    }

    pub(super) fn accept_settings(
        &self,
        attempt: OpenAttempt,
        accepted: &AcceptedRunSettings,
    ) -> Result<SetupReport<CapabilityReport, SessionSettings>, LifecycleRefusal> {
        let mut attempts = lock(&self.attempts);
        let session = match attempt {
            OpenAttempt::Create(attempt) => match attempts.creates.get_mut(&attempt.0) {
                Some(CreateOpening::Configured(session)) => session.as_mut(),
                _ => return Err(LifecycleRefusal::OpeningNotHeld),
            },
            OpenAttempt::Restore(run) => attempts
                .runs
                .get_mut(&run.generation)
                .filter(|entry| {
                    entry.attempt == OpenAttempt::Restore(run) && !entry.stopped.is_cancelled()
                })
                .and_then(|entry| match &mut entry.phase {
                    RunOpening::Held(held) => Some(&mut held.session),
                    RunOpening::Opening | RunOpening::Active => None,
                })
                .ok_or(LifecycleRefusal::OpeningNotHeld)?,
        };
        session.accept_restore_catalogue(&ProviderSettingsCatalog::from_record(Arc::clone(
            &accepted.catalogue,
        )));
        Ok(SetupReport {
            capabilities: session.capabilities().to_session_model(),
            settings: session.settings(),
            observed: catalogue_evidence(session.catalog()),
        })
    }

    pub(super) fn put_setting_back(
        &self,
        attempt: SettingAttempt,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<SettingFact>,
    ) -> Result<(), RestoreSettingRefusal> {
        let refusal = RestoreSettingRefusal::Harness;
        let attempts = lock(&self.attempts);
        let held = held_restore(&attempts, attempt.restore)
            .ok_or_else(|| refusal(LifecycleRefusal::OpeningNotHeld))?;
        if attempt.setting.change.generation != attempt.restore.generation {
            return Err(refusal(LifecycleRefusal::AttemptMismatch));
        }
        let session = self
            .session
            .get()
            .ok_or_else(|| refusal(LifecycleRefusal::RouteUnavailable))?;
        let request = OwnerSettingWirePlan::from_work_catalog(
            session.clone(),
            &ProviderSettingsCatalog::from_record(attempt.catalogue),
            attempt.setting.kind,
            attempt.value.0,
            held.accepted_mode_fallback.clone(),
        )
        .map_err(|reason| {
            refusal(match reason {
                WorkOfferRefusal::SettingValueNotOffered => {
                    LifecycleRefusal::SettingValueNotOffered
                }
                WorkOfferRefusal::SettingSetterUnavailable => {
                    LifecycleRefusal::SettingSetterUnavailable
                }
                _ => LifecycleRefusal::Encoding,
            })
        })?;
        let reply = match reserve_reply() {
            ReplyAdmission::Reserved(reply) => reply,
            ReplyAdmission::Refused(reason) => {
                return Err(RestoreSettingRefusal::ReplyReservation(reason));
            }
        };
        self.wire
            .reapply_lifecycle_setting(
                self.owner.incarnation,
                attempt.restore,
                attempt.setting,
                request,
                reply,
            )
            .map_err(|reason| refusal(lifecycle_transport_refusal(reason)))
    }

    pub(super) fn retire(&self, generation: SessionGeneration) {
        if let Some(run) = lock(&self.attempts).runs.remove(&generation) {
            run.stopped.cancel();
        }
        self.wire
            .apply_membership(self.owner.incarnation, TurnMembership::Retire(generation));
    }
}

impl Drop for OwnerOpenings {
    fn drop(&mut self) {
        self.stopped.cancel();
    }
}

fn held_restore(
    attempts: &OpeningAttempts,
    restore: RunAttempt<LoadAttempt>,
) -> Option<&HeldRestore> {
    attempts
        .runs
        .get(&restore.generation)
        .filter(|run| run.attempt == OpenAttempt::Restore(restore) && !run.stopped.is_cancelled())
        .and_then(|run| match &run.phase {
            RunOpening::Held(held) => Some(held.as_ref()),
            RunOpening::Opening | RunOpening::Active => None,
        })
}

fn opening_failure(error: ExternalProviderRuntimeError) -> OpeningFailure {
    if let Some(failure) = error.coded_wire_failure() {
        return match failure.kind {
            ProviderWireFailureKind::SessionNotFound
            | ProviderWireFailureKind::ResourceNotFound => OpeningFailure::Lost {
                reported: Some(failure),
            },
            ProviderWireFailureKind::RestoreFailed
            | ProviderWireFailureKind::Rejected
            | ProviderWireFailureKind::RequestCancelled => OpeningFailure::ProviderError(failure),
            _ => OpeningFailure::Refused(failure),
        };
    }
    match error {
        ExternalProviderRuntimeError::RestoreDisconnected
        | ExternalProviderRuntimeError::TransportFailure => OpeningFailure::Disconnected,
        ExternalProviderRuntimeError::RestoreNoAnswer => OpeningFailure::NoAnswer,
        ExternalProviderRuntimeError::SessionReleased => OpeningFailure::Closed,
        _ => OpeningFailure::OutcomeUnknown,
    }
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[path = "owner_acp_lifecycle_refusal_test.rs"]
mod refusal_tests;
