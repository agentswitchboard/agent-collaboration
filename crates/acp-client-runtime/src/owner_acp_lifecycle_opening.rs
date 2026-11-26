//! Held ACP openings remain with their owner through adoption.

#[path = "owner_acp_lifecycle_activation.rs"]
mod activation;
#[path = "owner_acp_lifecycle_close.rs"]
mod close;
#[path = "owner_acp_opening_policy.rs"]
mod policy;

use super::{lifecycle_transport_refusal, transport_run};
use crate::{
    ExternalProviderRuntimeError, RequestedProviderSettings,
    owner_restore_updates::OpeningInstance,
    owner_session_creation::catalogue_evidence,
    owner_session_open::OwnerOpenedSession,
    owner_session_setting::{OwnerSettingWirePlan, RestoreSettingPlan},
    owner_wire::OwnerAcpWire,
};
use policy::{OpeningEvent, OpeningPhase};
use provider_session_machines::{
    ProviderSettingsCatalog, SessionGeneration,
    harness_lifecycle::{
        AcceptedRunSettings, CreateAttempt, CreatedSetupFact, CreationEffect, HarnessSessionId,
        Held, LifecycleRefusal, LifecycleReply, OpenAttempt, OpenRequest, OpenSetup, OpeningFact,
        OpeningFailure, RestoreLoadReadback, RestoreSettingRefusal, RestoredOpening, RunRetirement,
        SessionCreatedSetupFact, SessionLifecycleFact, SessionLifecyclePort, SettingAttempt,
        SettingFact, SetupReport,
    },
    session_lifecycle_machine::{CloseAttempt, LoadAttempt, RestoreVia},
    session_owner::{OwnerAddress, ProviderWireFailureKind, RunAttempt, TurnMembership},
    session_setting_machine::SettingValueId,
};
use router_clock::RuntimeClock;
use session_event_model::{CapabilityReport, SessionSettings, SkillView};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};
use switchboard_harness_contracts::{ReplyAdmission, WorkOfferRefusal, WriterState};
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

struct CreateOpening {
    phase: OpeningPhase,
    instance: OpeningInstance,
    held: Option<Box<HeldCreate>>,
    configured: Option<Box<OwnerOpenedSession>>,
}

struct HeldCreate {
    session: OwnerOpenedSession,
    requested: RequestedProviderSettings,
}

struct OpenedRun {
    attempt: OpenAttempt,
    stopped: CancellationToken,
    phase: OpeningPhase,
    instance: OpeningInstance,
    held: Option<Box<HeldRestore>>,
    close_attempt: Option<CloseAttempt>,
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

    pub(super) fn writer_refusal(&self) -> Option<LifecycleRefusal> {
        if self.connection_retired.is_cancelled() {
            Some(LifecycleRefusal::ConnectionRetired)
        } else if self.wire.writer_has_stopped() {
            Some(LifecycleRefusal::Writer(WriterState::Stopped))
        } else {
            None
        }
    }

    fn idle_phase(&self) -> OpeningPhase {
        if self.session.get().is_some() {
            OpeningPhase::BoundIdle
        } else {
            OpeningPhase::UnboundIdle
        }
    }

    fn opening_refusal(
        &self,
        attempts: &OpeningAttempts,
        attempt: OpenAttempt,
    ) -> LifecycleRefusal {
        if let Some(refusal) = self.writer_refusal() {
            return refusal;
        }
        let phase = match attempt {
            OpenAttempt::Create(create) => attempts
                .creates
                .get(&create.0)
                .map(|entry| entry.phase)
                .or_else(|| {
                    attempts
                        .runs
                        .values()
                        .find(|entry| entry.attempt == attempt)
                        .map(|entry| entry.phase)
                }),
            OpenAttempt::Restore(restore) => attempts
                .runs
                .get(&restore.generation)
                .filter(|entry| entry.attempt == attempt)
                .map(|entry| entry.phase),
        };
        match phase {
            Some(phase) => phase.refusal(),
            None if !attempts.creates.is_empty() || !attempts.runs.is_empty() => {
                self.mismatched_call()
            }
            None => self.idle_phase().refusal(),
        }
    }

    fn mismatched_call(&self) -> LifecycleRefusal {
        self.writer_refusal()
            .unwrap_or(LifecycleRefusal::AttemptMismatch)
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
            _ => Err(self.mismatched_call()),
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
            return Err(self.opening_refusal(&attempts, OpenAttempt::Create(attempt)));
        }
        let mut phase = self.idle_phase();
        if !phase.advance(OpeningEvent::OpenCreate) {
            return Err(phase.refusal());
        }
        let receipt = match self.wire.create(self.owner.incarnation, setup) {
            Ok(receipt) => receipt,
            Err(refusal) => {
                phase.advance(OpeningEvent::WriterRefused);
                return Err(lifecycle_transport_refusal(refusal));
            }
        };
        phase.advance(OpeningEvent::WriterAccepted);
        let instance = OpeningInstance::default();
        attempts.creates.insert(
            attempt.0,
            CreateOpening {
                phase,
                instance: instance.clone(),
                held: None,
                configured: None,
            },
        );
        drop(attempts);

        let attempts = Arc::clone(&self.attempts);
        let owned_session = Arc::clone(&self.session);
        let stopped = self.stopped.clone();
        self.tasks.spawn(async move {
            let result = tokio::select! {
                biased;
                () = stopped.cancelled() => None,
                () = retired => None,
                result = receipt => Some(result),
            };
            let fact = {
                let mut attempts = lock(&attempts);
                let Some(entry) = attempts
                    .creates
                    .get_mut(&attempt.0)
                    .filter(|entry| entry.instance.is_same_as(&instance))
                else {
                    return;
                };
                match result {
                    Some(Ok(Ok(session))) => {
                        let session_id = session.provider_id();
                        if !entry.phase.accepts(OpeningEvent::ProviderCreated)
                            || owned_session.set(session_id.clone()).is_err()
                        {
                            attempts.creates.remove(&attempt.0);
                            return;
                        }
                        entry.phase.advance(OpeningEvent::ProviderCreated);
                        entry.held = Some(Box::new(HeldCreate { session, requested }));
                        OpeningFact::Created {
                            attempt,
                            session: HarnessSessionId(session_id),
                        }
                    }
                    Some(Ok(Err(error))) => {
                        entry.phase.advance(OpeningEvent::OpenFailed);
                        attempts.creates.remove(&attempt.0);
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
                    Some(Err(_)) => {
                        entry.phase.advance(OpeningEvent::OpenFailed);
                        attempts.creates.remove(&attempt.0);
                        OpeningFact::CreateFailed {
                            attempt,
                            remote: CreationEffect::Unknown,
                            failure: OpeningFailure::OutcomeUnknown,
                        }
                    }
                    None => {
                        entry.phase.advance(OpeningEvent::WorkerExited);
                        attempts.creates.remove(&attempt.0);
                        return;
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
        if self.session.get().is_some_and(|bound| bound != &session.0) {
            return Err(self.mismatched_call());
        }
        if !attempts.creates.is_empty() || attempts.runs.contains_key(&run.generation) {
            return Err(self.opening_refusal(&attempts, OpenAttempt::Restore(run)));
        }
        let mut phase = self.idle_phase();
        if phase.advance(OpeningEvent::BindSavedSession) {
            let _bound = self.session.set(session.0.clone());
        }
        if !phase.advance(OpeningEvent::OpenRestore) {
            return Err(phase.refusal());
        }
        let transport = transport_run(self.owner, run.generation);
        self.wire.open_route(transport, session.0.clone());
        let receipt = self
            .wire
            .restore_opening(transport, run.attempt, session.0, via, setup);
        #[cfg(feature = "test-observation")]
        self.wire
            .fence_dispatch_for_test(transport, crate::owner_wire::FixtureDispatchPoint::Restore);
        let (instance, receipt) = match receipt {
            Ok(receipt) => receipt,
            Err(refusal) => {
                phase.advance(OpeningEvent::WriterRefused);
                self.wire.apply_membership(
                    self.owner.incarnation,
                    TurnMembership::Retire(run.generation),
                );
                return Err(lifecycle_transport_refusal(refusal));
            }
        };
        phase.advance(OpeningEvent::WriterAccepted);
        let stopped = self.stopped.child_token();
        attempts.runs.insert(
            run.generation,
            OpenedRun {
                attempt: OpenAttempt::Restore(run),
                stopped: stopped.clone(),
                phase,
                instance: instance.clone(),
                held: None,
                close_attempt: None,
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
                {
                    let mut attempts = lock(&attempts);
                    let Some(entry) = exact_restore(&mut attempts, run, &instance) else {
                        return Ok(None);
                    };
                    let event = match via {
                        RestoreVia::Load => OpeningEvent::RestoreLoadReply,
                        RestoreVia::Resume => OpeningEvent::RestoreResumeReply,
                    };
                    if !entry.phase.advance(event) {
                        entry.phase.advance(OpeningEvent::LateOpenReply);
                        return Ok(None);
                    }
                }
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
                let Some(entry) = exact_restore(&mut attempts, run, &instance) else {
                    return Ok(None);
                };
                if via == RestoreVia::Load {
                    entry.phase.advance(OpeningEvent::ReplayAdmitted);
                }
                if entry.phase != OpeningPhase::HeldRestore {
                    return Ok(None);
                }
                entry.held = Some(Box::new(HeldRestore {
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
                    let event = if matches!(other, Some(Err(_))) {
                        OpeningEvent::OpenFailed
                    } else {
                        OpeningEvent::WorkerExited
                    };
                    if discard_restore_opening(&attempts, &wire, owner, run, &instance, event) {
                        stopped.cancel();
                        if let Some(Err(failure)) = other {
                            reply.send(OpeningFact::RestoreFailed { run, failure });
                        }
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
        let Some(entry) = attempts.creates.get_mut(&attempt.0) else {
            return Err(self.opening_refusal(&attempts, OpenAttempt::Create(attempt)));
        };
        if !entry.phase.accepts(OpeningEvent::Configure) {
            return Err(entry.phase.refusal());
        }
        let Some(held) = entry.held.take() else {
            return Err(entry.phase.refusal());
        };
        entry.phase.advance(OpeningEvent::Configure);
        let instance = entry.instance.clone();
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
                () = stopped.cancelled() => None,
                () = retired => None,
                result = session.configure_lifecycle_create(attempt, &requested, clock.as_ref()) => Some(result),
            };
            {
                let mut attempts = lock(&attempts);
                let Some(entry) = attempts.creates.get_mut(&attempt.0)
                    .filter(|entry| entry.instance.is_same_as(&instance))
                else { return; };
                let event = match &fact {
                    Some(CreatedSetupFact::ClosedAfterInvalidSetting { .. }) => OpeningEvent::ClosedDuringConfigure,
                    Some(_) => OpeningEvent::Configured,
                    None => OpeningEvent::WorkerExited,
                };
                if !entry.phase.advance(event) {
                    return;
                }
                if entry.phase == OpeningPhase::ConfiguredCreate {
                    entry.configured = Some(Box::new(session));
                } else {
                    attempts.creates.remove(&attempt.0);
                }
            }
            if let Some(fact) = fact {
                reply.send(fact);
            }
        });
        Ok(())
    }

    pub(super) fn opening_is_held(&self, attempt: OpenAttempt) -> Held {
        if let Some(refusal) = self.writer_refusal() {
            return Held::NotHeld(refusal);
        }
        let attempts = lock(&self.attempts);
        let held = match attempt {
            OpenAttempt::Create(attempt) => attempts.creates.get(&attempt.0).is_some_and(|entry| {
                matches!(
                    entry.phase,
                    OpeningPhase::HeldCreate | OpeningPhase::ConfiguredCreate
                )
            }),
            OpenAttempt::Restore(run) => held_restore(&attempts, run).is_some(),
        };
        if held {
            Held::Held
        } else {
            Held::NotHeld(self.opening_refusal(&attempts, attempt))
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
            OpenAttempt::Create(attempt) => {
                let Some(entry) = attempts.creates.get_mut(&attempt.0) else {
                    return Err(self.opening_refusal(&attempts, OpenAttempt::Create(attempt)));
                };
                if !entry.phase.advance(OpeningEvent::AcceptCreatedSettings) {
                    return Err(entry.phase.refusal());
                }
                entry
                    .configured
                    .as_deref_mut()
                    .ok_or_else(|| entry.phase.refusal())?
            }
            OpenAttempt::Restore(run) => {
                let Some(entry) = attempts
                    .runs
                    .get_mut(&run.generation)
                    .filter(|entry| entry.attempt == attempt && !entry.stopped.is_cancelled())
                else {
                    return Err(self.opening_refusal(&attempts, attempt));
                };
                if !entry.phase.advance(OpeningEvent::AcceptSettings) {
                    return Err(entry.phase.refusal());
                }
                &mut entry
                    .held
                    .as_deref_mut()
                    .ok_or_else(|| entry.phase.refusal())?
                    .session
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
        let held = attempts
            .runs
            .get(&attempt.restore.generation)
            .filter(|entry| {
                entry.attempt == OpenAttempt::Restore(attempt.restore)
                    && !entry.stopped.is_cancelled()
                    && entry.phase.accepts(OpeningEvent::PutSettingBack)
            })
            .and_then(|entry| entry.held.as_deref())
            .ok_or_else(|| {
                refusal(self.opening_refusal(&attempts, OpenAttempt::Restore(attempt.restore)))
            })?;
        if attempt.setting.change.generation != attempt.restore.generation {
            return Err(refusal(self.mismatched_call()));
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
            let mut retired = attempts.runs.remove(&generation);
            if let Some(entry) = retired.as_mut() {
                entry.phase.advance(OpeningEvent::Retire);
            }
            self.wire
                .apply_membership(self.owner.incarnation, TurnMembership::Retire(generation));
            retired
        };
        if let Some(run) = retired {
            run.stopped.cancel();
        }
    }
}

fn exact_restore<'a>(
    attempts: &'a mut OpeningAttempts,
    run: RunAttempt<LoadAttempt>,
    instance: &OpeningInstance,
) -> Option<&'a mut OpenedRun> {
    attempts.runs.get_mut(&run.generation).filter(|entry| {
        entry.attempt == OpenAttempt::Restore(run) && entry.instance.is_same_as(instance)
    })
}

fn discard_restore_opening(
    attempts: &Mutex<OpeningAttempts>,
    wire: &OwnerAcpWire,
    owner: OwnerAddress,
    run: RunAttempt<LoadAttempt>,
    instance: &OpeningInstance,
    event: OpeningEvent,
) -> bool {
    let mut attempts = lock(attempts);
    let Some(entry) = exact_restore(&mut attempts, run, instance) else {
        return false;
    };
    if !entry.phase.advance(event) {
        if event == OpeningEvent::OpenFailed {
            entry.phase.advance(OpeningEvent::LateOpenReply);
        }
        return false;
    }
    wire.apply_membership(owner.incarnation, TurnMembership::Retire(run.generation));
    attempts.runs.remove(&run.generation);
    true
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
        .filter(|run| {
            run.attempt == OpenAttempt::Restore(restore)
                && !run.stopped.is_cancelled()
                && matches!(
                    run.phase,
                    OpeningPhase::HeldRestore | OpeningPhase::PreparedRestore
                )
        })
        .and_then(|run| run.held.as_deref())
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
