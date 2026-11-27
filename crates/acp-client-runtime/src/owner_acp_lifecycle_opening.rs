//! One payload-owning ACP opening table for each owner binding.

#[path = "owner_acp_lifecycle_activation.rs"]
mod activation;
#[path = "owner_acp_lifecycle_close.rs"]
mod close;
#[path = "owner_acp_opening_create.rs"]
mod create;
#[path = "owner_acp_opening_observations.rs"]
mod observations;
#[path = "owner_acp_opening_policy.rs"]
mod policy;
#[path = "owner_acp_opening_restore.rs"]
mod restore;
#[path = "owner_acp_opening_row.rs"]
mod row;
#[path = "owner_acp_opening_setter.rs"]
mod setter;

use super::{lifecycle_transport_refusal, transport_run};
use crate::{
    ExternalProviderRuntimeError, RequestedProviderSettings,
    owner_opening_instance::OpeningInstance, owner_wire::OwnerAcpWire,
};
use policy::OpeningPhase;
use provider_session_machines::harness_lifecycle::{ActivationRefusal, ActivationSetup};
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        CreateAttempt, HarnessSessionId, Held, LifecycleRefusal, LifecycleReply, OpenAttempt,
        OpenRequest, OpenSetup, OpeningFact, OpeningFailure, RunRetirement, SessionLifecycleFact,
        SessionLifecyclePort,
    },
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_owner::{OwnerAddress, ProviderWireFailureKind, RunAttempt},
};
use router_clock::RuntimeClock;
use row::{
    Command, Effect, OpenCreateRequest, OpenRestoreRequest, OpeningRow, Rejected, RunResources,
    Slot as OpeningSlot,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};
use switchboard_harness_contracts::WriterState;
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
    creates: BTreeMap<Uuid, OpeningRow>,
    runs: BTreeMap<SessionGeneration, OpeningRow>,
    port_refusal: Option<LifecycleRefusal>,
}

impl OpeningAttempts {
    fn get(&self, slot: OpeningSlot) -> Option<&OpeningRow> {
        match slot {
            OpeningSlot::Create(create) => self.creates.get(&create.0),
            OpeningSlot::Run(run) => self.runs.get(&run),
        }
    }

    fn insert(&mut self, slot: OpeningSlot, row: OpeningRow) {
        match slot {
            OpeningSlot::Create(create) => {
                self.creates.insert(create.0, row);
            }
            OpeningSlot::Run(run) => {
                self.runs.insert(run, row);
            }
        }
    }

    fn remove(&mut self, slot: OpeningSlot) -> Option<OpeningRow> {
        match slot {
            OpeningSlot::Create(create) => self.creates.remove(&create.0),
            OpeningSlot::Run(run) => self.runs.remove(&run),
        }
    }

    fn exact(&self, slot: OpeningSlot, instance: &OpeningInstance) -> Option<&OpeningRow> {
        self.get(slot)
            .filter(|row| row.instance().is_same_as(instance))
    }

    fn apply(
        &mut self,
        slot: OpeningSlot,
        instance: &OpeningInstance,
        command: Command,
    ) -> Result<Effect, Rejected> {
        if self.exact(slot, instance).is_none() {
            return Err(Rejected {
                reason: LifecycleRefusal::AttemptMismatch,
                _command: Box::new(command),
            });
        }
        let Some(row) = self.remove(slot) else {
            return Err(Rejected {
                reason: LifecycleRefusal::AttemptMismatch,
                _command: Box::new(command),
            });
        };
        self.commit(slot, row, instance, command)
    }

    fn commit(
        &mut self,
        slot: OpeningSlot,
        row: OpeningRow,
        instance: &OpeningInstance,
        command: Command,
    ) -> Result<Effect, Rejected> {
        match row.apply(instance, command) {
            Ok((next, effect)) => {
                effect.fence();
                if let Some(row) = next {
                    self.insert(slot, row);
                }
                Ok(effect)
            }
            Err(refused) => {
                if !refused.row.is_vacant() {
                    self.insert(slot, *refused.row);
                }
                Err(refused.rejection)
            }
        }
    }

    fn begin_create(
        &mut self,
        opening: &OwnerOpenings,
        attempt: CreateAttempt,
        request: OpenCreateRequest,
    ) -> Result<(OpeningInstance, Effect), Rejected> {
        let identity = OpenAttempt::Create(attempt);
        let command = Command::OpenCreate(Box::new(request));
        if !self.creates.is_empty() || !self.runs.is_empty() || self.port_refusal.is_some() {
            return Err(Rejected {
                reason: opening.opening_refusal(self, identity),
                _command: Box::new(command),
            });
        }
        self.begin_vacant(opening, identity, command)
    }

    fn begin_restore(
        &mut self,
        opening: &OwnerOpenings,
        request: RestoreOpening,
        stopped: CancellationToken,
    ) -> Result<(OpeningInstance, Effect), Rejected> {
        let identity = OpenAttempt::Restore(request.run);
        let wrong_session = opening
            .session
            .get()
            .is_some_and(|bound| bound != &request.session.0);
        let command = Command::OpenRestore(Box::new(OpenRestoreRequest {
            request,
            owner: opening.owner,
            stopped,
        }));
        if let Some(reason) = self.port_refusal {
            return Err(Rejected {
                reason,
                _command: Box::new(command),
            });
        }
        if wrong_session {
            return Err(Rejected {
                reason: LifecycleRefusal::AttemptMismatch,
                _command: Box::new(command),
            });
        }
        if !self.creates.is_empty() || self.get(OpeningSlot::for_attempt(identity)).is_some() {
            return Err(Rejected {
                reason: opening.opening_refusal(self, identity),
                _command: Box::new(command),
            });
        }
        self.begin_vacant(opening, identity, command)
    }

    fn begin_vacant(
        &mut self,
        opening: &OwnerOpenings,
        identity: OpenAttempt,
        command: Command,
    ) -> Result<(OpeningInstance, Effect), Rejected> {
        let instance = OpeningInstance::default();
        let row = OpeningRow::vacant(identity, instance.clone(), opening.session.get().is_some());
        let effect = self.commit(OpeningSlot::for_attempt(identity), row, &instance, command)?;
        if let Effect::OfferRestore(start) = &effect {
            opening
                .session
                .get_or_init(|| start.request.session.0.clone());
        }
        Ok((instance, effect))
    }

    fn begin_reader(
        &mut self,
        opening: &OwnerOpenings,
        setup: ActivationSetup,
    ) -> Result<(OpeningInstance, SessionGeneration, Effect), ActivationRefusal> {
        use policy::activation_refusal;
        if let Some(reason) = self.port_refusal {
            return Err(activation_refusal(reason));
        }
        let (identity, generation) = match setup {
            ActivationSetup::Created {
                attempt,
                generation,
            } => (OpenAttempt::Create(attempt), generation),
            ActivationSetup::Restored(run) => (OpenAttempt::Restore(run), run.generation),
        };
        let run_slot = OpeningSlot::Run(generation);
        if generation == SessionGeneration::UNTRACKED {
            return Err(ActivationRefusal::AttemptMismatch);
        }
        if let Some(row) = self.get(run_slot) {
            if row.attempt() != identity {
                return Err(ActivationRefusal::AttemptMismatch);
            }
            if row.phase() == OpeningPhase::Active {
                return Err(ActivationRefusal::AlreadyActive);
            }
            if matches!(identity, OpenAttempt::Create(_)) {
                return Err(activation_refusal(row.phase().refusal()));
            }
        }
        let slot = OpeningSlot::for_attempt(identity);
        let instance = self
            .targeted(identity)
            .map(|row| row.instance().clone())
            .ok_or_else(|| activation_refusal(opening.opening_refusal(self, identity)))?;
        let created = matches!(identity, OpenAttempt::Create(_));
        let effect = self
            .apply(
                slot,
                &instance,
                Command::Activate {
                    run: RunResources::new(opening.owner, generation, &opening.stopped),
                    created,
                },
            )
            .map_err(|rejected| activation_refusal(rejected.reason))?;
        if created
            && matches!(effect, Effect::StartReader(_))
            && let Some(row) = self.remove(slot)
        {
            self.insert(run_slot, row);
        }
        Ok((instance, generation, effect))
    }

    fn targeted(&self, attempt: OpenAttempt) -> Option<&OpeningRow> {
        self.get(OpeningSlot::for_attempt(attempt))
            .filter(|row| row.attempt() == attempt)
            .or_else(|| match attempt {
                OpenAttempt::Create(_) => self.runs.values().find(|row| row.attempt() == attempt),
                OpenAttempt::Restore(_) => None,
            })
    }
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
        let openings = Self {
            owner,
            wire,
            session,
            clock,
            tasks,
            stopped: CancellationToken::new(),
            connection_retired,
            attempts: Arc::default(),
        };
        openings.watch_writer();
        openings
    }

    pub(super) fn owner(&self) -> OwnerAddress {
        self.owner
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
        if let Some(refusal) = attempts.port_refusal {
            return refusal;
        }
        if let Some(row) = attempts.targeted(attempt) {
            return row.phase().refusal();
        }
        if attempts.creates.is_empty() && attempts.runs.is_empty() {
            self.idle_phase().refusal()
        } else {
            LifecycleRefusal::AttemptMismatch
        }
    }

    pub(super) fn open(
        &self,
        request: OpenRequest,
        reply: LifecycleReply<OpeningFact>,
        events: SessionLifecyclePort,
        retired: RunRetirement,
    ) -> Result<(), LifecycleRefusal> {
        self.observe_health()?;
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

    pub(super) fn opening_is_held(&self, attempt: OpenAttempt) -> Held {
        if let Err(refusal) = self.observe_health() {
            return Held::NotHeld(refusal);
        }
        let attempts = lock(&self.attempts);
        if attempts.targeted(attempt).is_some_and(OpeningRow::held) {
            Held::Held
        } else {
            Held::NotHeld(self.opening_refusal(&attempts, attempt))
        }
    }
}

impl Drop for OwnerOpenings {
    fn drop(&mut self) {
        self.stopped.cancel();
    }
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

#[cfg(not(any(test, feature = "test-observation")))]
type OpeningTableGuard<'a> = MutexGuard<'a, OpeningAttempts>;

#[cfg(any(test, feature = "test-observation"))]
struct OpeningTableGuard<'a> {
    guard: MutexGuard<'a, OpeningAttempts>,
    _scope: crate::owner_opening_disposal::OpeningTableScope,
}

#[cfg(any(test, feature = "test-observation"))]
impl std::ops::Deref for OpeningTableGuard<'_> {
    type Target = OpeningAttempts;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

#[cfg(any(test, feature = "test-observation"))]
impl std::ops::DerefMut for OpeningTableGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

fn lock(value: &Mutex<OpeningAttempts>) -> OpeningTableGuard<'_> {
    let guard = value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    #[cfg(any(test, feature = "test-observation"))]
    return OpeningTableGuard {
        guard,
        _scope: crate::owner_opening_disposal::OpeningTableScope::enter(value),
    };
    #[cfg(not(any(test, feature = "test-observation")))]
    guard
}

#[cfg(any(test, feature = "test-observation"))]
impl OwnerOpenings {
    fn observe_reply<Fact: 'static>(
        &self,
        kind: crate::owner_opening_disposal::OpeningDisposalKind,
        reply: LifecycleReply<Fact>,
    ) -> LifecycleReply<Fact> {
        crate::owner_opening_disposal::observe_reply(
            reply,
            self.wire
                .take_opening_disposal_for_test(kind, &self.attempts),
        )
    }
}

#[cfg(test)]
#[path = "owner_acp_lifecycle_refusal_test.rs"]
mod refusal_tests;
