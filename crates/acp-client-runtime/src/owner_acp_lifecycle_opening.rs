//! Held ACP openings remain with their owner through adoption.

#[path = "owner_acp_lifecycle_activation.rs"]
mod activation;
#[path = "owner_acp_opening_guidance.rs"]
mod guidance;

use super::{lifecycle_transport_refusal, transport_run};
use crate::{
    ExternalProviderRuntimeError, RequestedProviderSettings,
    owner_session_creation::catalogue_evidence,
    owner_session_open::OwnerOpenedSession,
    owner_session_setting::{OwnerSettingWirePlan, RestoreSettingPlan},
    owner_wire::OwnerAcpWire,
};
use guidance::{
    AttemptRelation, OpeningPhase, OpeningRefusal, WriterAvailability, classify_refusal,
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
use switchboard_harness_contracts::{HeldOpeningKind, ReplyAdmission, WorkOfferRefusal};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

pub(super) struct OwnerOpenings {
    owner: OwnerAddress,
    wire: Arc<OwnerAcpWire>,
    session: Arc<OnceLock<String>>,
    clock: Arc<dyn RuntimeClock>,
    tasks: TaskTracker,
    stopped: CancellationToken,
    connection_retired: CancellationToken,
    attempts: Arc<Mutex<OpeningAttempts>>,
}

#[derive(Default)]
struct OpeningAttempts {
    creates: BTreeMap<Uuid, CreateOpening>,
    runs: BTreeMap<SessionGeneration, OpenedRun>,
}

impl OpeningAttempts {
    fn describe(&self, attempt: OpenAttempt) -> (OpeningPhase, AttemptRelation) {
        match attempt {
            OpenAttempt::Create(create) => {
                if let Some(opening) = self.creates.get(&create.0) {
                    return (opening.phase(), AttemptRelation::Same);
                }
                if self.runs.values().any(|run| run.attempt == attempt) {
                    return (OpeningPhase::Consumed, AttemptRelation::Same);
                }
            }
            OpenAttempt::Restore(restore) => {
                if let Some(opening) = self.runs.get(&restore.generation) {
                    let relation = if opening.attempt == attempt {
                        AttemptRelation::Same
                    } else {
                        AttemptRelation::Different
                    };
                    return (opening.phase(), relation);
                }
            }
        }
        // One provisional create can block a competing open. Other generations
        // need not share a phase, so do not select one of them.
        if let Some(opening) = self.creates.values().next() {
            (opening.phase(), AttemptRelation::Different)
        } else if self.runs.is_empty() {
            (OpeningPhase::None, AttemptRelation::NoStoredMatch)
        } else {
            (OpeningPhase::None, AttemptRelation::Different)
        }
    }
}

enum CreateOpening {
    Opening,
    Held(Box<HeldCreate>),
    Configuring,
    Configured(Box<OwnerOpenedSession>),
}

impl CreateOpening {
    fn phase(&self) -> OpeningPhase {
        match self {
            Self::Opening => OpeningPhase::Opening,
            Self::Held(_) => OpeningPhase::Held(HeldOpeningKind::Create),
            Self::Configuring => OpeningPhase::Configuring,
            Self::Configured(_) => OpeningPhase::Configured,
        }
    }
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

impl OpenedRun {
    fn phase(&self) -> OpeningPhase {
        if self.stopped.is_cancelled() {
            return OpeningPhase::Consumed;
        }
        match &self.phase {
            RunOpening::Opening(_) => OpeningPhase::Opening,
            RunOpening::Held(_) => OpeningPhase::Held(HeldOpeningKind::Restore),
            RunOpening::Active => OpeningPhase::Consumed,
        }
    }
}

enum RunOpening {
    Opening(Arc<()>),
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
        connection_retired: CancellationToken,
    ) -> Self {
        Self {
            owner,
            wire,
            session,
            clock,
            tasks,
            stopped: CancellationToken::new(),
            connection_retired,
            attempts: Arc::default(),
        }
    }

    pub(super) fn owner(&self) -> OwnerAddress {
        self.owner
    }

    fn writer_availability(&self) -> WriterAvailability {
        if self.connection_retired.is_cancelled() {
            WriterAvailability::ConnectionRetired
        } else if self.wire.writer_has_stopped() {
            WriterAvailability::Stopped
        } else {
            WriterAvailability::Live
        }
    }

    fn stopped_writer_refusal(&self) -> Option<OpeningRefusal> {
        let writer = self.writer_availability();
        (writer != WriterAvailability::Live)
            .then(|| classify_refusal(writer, OpeningPhase::None, AttemptRelation::Unnamed))
    }

    pub(super) fn writer_refusal(&self) -> Option<LifecycleRefusal> {
        self.stopped_writer_refusal().map(OpeningRefusal::lifecycle)
    }

    fn opening_refusal(&self, attempts: &OpeningAttempts, attempt: OpenAttempt) -> OpeningRefusal {
        let (phase, caller) = attempts.describe(attempt);
        classify_refusal(self.writer_availability(), phase, caller)
    }

    fn mismatched_call(&self) -> OpeningRefusal {
        classify_refusal(
            self.writer_availability(),
            OpeningPhase::None,
            AttemptRelation::Different,
        )
    }

    pub(super) fn open(
        &self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        if let Some(refusal) = self.writer_refusal() {
            return Err(refusal);
        }
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
            _ => Err(self.mismatched_call().lifecycle()),
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
        if !attempts.creates.is_empty() || !attempts.runs.is_empty() {
            return Err(self
                .opening_refusal(&attempts, OpenAttempt::Create(attempt))
                .lifecycle());
        }
        if self.session.get().is_some() {
            return Err(self.mismatched_call().lifecycle());
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
            return Err(self
                .opening_refusal(&attempts, OpenAttempt::Restore(run))
                .lifecycle());
        }
        if self.session.get_or_init(|| session.0.clone()) != &session.0 {
            return Err(self.mismatched_call().lifecycle());
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
        let instance = Arc::new(());
        attempts.runs.insert(
            run.generation,
            OpenedRun {
                attempt: OpenAttempt::Restore(run),
                stopped: stopped.clone(),
                phase: RunOpening::Opening(instance.clone()),
            },
        );
        drop(attempts);

        let attempts = Arc::clone(&self.attempts);
        let wire = Arc::clone(&self.wire);
        let owner = self.owner;
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
                        && matches!(&entry.phase, RunOpening::Opening(current) if Arc::ptr_eq(current, &instance))
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
            let answer = tokio::select! {
                biased;
                () = stopped.cancelled() => None,
                () = retired => None,
                result = open => Some(result),
            };
            match answer {
                Some(Ok(Some(opening))) => reply.send(OpeningFact::Restored { run, opening }),
                other => {
                    discard_restore_opening(&attempts, &wire, owner, run, &instance);
                    stopped.cancel();
                    if let Some(Err(failure)) = other {
                        reply.send(OpeningFact::RestoreFailed { run, failure });
                    }
                }
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
        if let Some(refusal) = self.writer_refusal() {
            return Err(refusal);
        }
        let mut attempts = lock(&self.attempts);
        let Some(held @ CreateOpening::Held(_)) = attempts.creates.get_mut(&attempt.0) else {
            return Err(self
                .opening_refusal(&attempts, OpenAttempt::Create(attempt))
                .lifecycle());
        };
        let CreateOpening::Held(held) = std::mem::replace(held, CreateOpening::Configuring) else {
            return Err(self
                .opening_refusal(&attempts, OpenAttempt::Create(attempt))
                .lifecycle());
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
        if let Some(refusal) = self.writer_refusal() {
            return Held::NotHeld(refusal);
        }
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
            Held::NotHeld(self.opening_refusal(&attempts, attempt).lifecycle())
        }
    }

    pub(super) fn accept_settings(
        &self,
        attempt: OpenAttempt,
        accepted: &AcceptedRunSettings,
    ) -> Result<SetupReport<CapabilityReport, SessionSettings>, LifecycleRefusal> {
        if let Some(refusal) = self.writer_refusal() {
            return Err(refusal);
        }
        let mut attempts = lock(&self.attempts);
        let session = match attempt {
            OpenAttempt::Create(attempt) => match attempts.creates.get_mut(&attempt.0) {
                Some(CreateOpening::Configured(session)) => session.as_mut(),
                _ => {
                    return Err(self
                        .opening_refusal(&attempts, OpenAttempt::Create(attempt))
                        .lifecycle());
                }
            },
            OpenAttempt::Restore(run) => {
                let Some(session) = attempts
                    .runs
                    .get_mut(&run.generation)
                    .filter(|entry| {
                        entry.attempt == OpenAttempt::Restore(run) && !entry.stopped.is_cancelled()
                    })
                    .and_then(|entry| match &mut entry.phase {
                        RunOpening::Held(held) => Some(&mut held.session),
                        RunOpening::Opening(_) | RunOpening::Active => None,
                    })
                else {
                    return Err(self
                        .opening_refusal(&attempts, OpenAttempt::Restore(run))
                        .lifecycle());
                };
                session
            }
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
        if let Some(reason) = self.writer_refusal() {
            return Err(refusal(reason));
        }
        let attempts = lock(&self.attempts);
        let held = held_restore(&attempts, attempt.restore).ok_or_else(|| {
            refusal(
                self.opening_refusal(&attempts, OpenAttempt::Restore(attempt.restore))
                    .lifecycle(),
            )
        })?;
        if attempt.setting.change.generation != attempt.restore.generation {
            return Err(refusal(self.mismatched_call().lifecycle()));
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
        let retired = {
            let mut attempts = lock(&self.attempts);
            let retired = attempts.runs.remove(&generation);
            self.wire
                .apply_membership(self.owner.incarnation, TurnMembership::Retire(generation));
            retired
        };
        if let Some(run) = retired {
            run.stopped.cancel();
        }
    }
}

fn discard_restore_opening(
    attempts: &Mutex<OpeningAttempts>,
    wire: &OwnerAcpWire,
    owner: OwnerAddress,
    run: RunAttempt<LoadAttempt>,
    instance: &Arc<()>,
) {
    let mut attempts = lock(attempts);
    if attempts.runs.get(&run.generation).is_some_and(|entry| {
        entry.attempt == OpenAttempt::Restore(run)
            && matches!(&entry.phase, RunOpening::Opening(current) if Arc::ptr_eq(current, instance))
    }) {
        wire.apply_membership(owner.incarnation, TurnMembership::Retire(run.generation));
        attempts.runs.remove(&run.generation);
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
            RunOpening::Opening(_) | RunOpening::Active => None,
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
