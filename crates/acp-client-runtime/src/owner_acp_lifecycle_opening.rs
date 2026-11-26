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

use super::{lifecycle_transport_refusal, transport_run};
use crate::{
    ExternalProviderRuntimeError, RequestedProviderSettings,
    owner_opening_instance::OpeningInstance, owner_wire::OwnerAcpWire,
};
use policy::{OpeningEvent, OpeningPhase};
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
use row::{OpeningRow, OpeningState, Refused, Transition};
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

#[derive(Clone, Copy)]
enum OpeningSlot {
    Create(CreateAttempt),
    Run(SessionGeneration),
}

impl OpeningSlot {
    fn for_attempt(attempt: OpenAttempt) -> Self {
        match attempt {
            OpenAttempt::Create(create) => Self::Create(create),
            OpenAttempt::Restore(restore) => Self::Run(restore.generation),
        }
    }
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
            .filter(|row| row.instance.is_same_as(instance))
    }

    fn apply<Payload, Effects>(
        &mut self,
        slot: OpeningSlot,
        instance: &OpeningInstance,
        event: OpeningEvent,
        payload: Payload,
        apply: impl FnOnce(
            OpeningState,
            Payload,
        ) -> Result<Transition<Effects>, (OpeningState, Payload)>,
    ) -> Result<Effects, (LifecycleRefusal, Payload)> {
        if self.exact(slot, instance).is_none() {
            return Err((LifecycleRefusal::AttemptMismatch, payload));
        }
        let Some(row) = self.remove(slot) else {
            return Err((LifecycleRefusal::AttemptMismatch, payload));
        };
        match row.apply(event, payload, apply) {
            Ok((next, effects)) => {
                if let Some(row) = next {
                    self.insert(slot, row);
                }
                Ok(effects)
            }
            Err(Refused {
                row,
                reason,
                payload,
            }) => {
                self.insert(slot, row);
                Err((reason, payload))
            }
        }
    }

    fn targeted(&self, attempt: OpenAttempt) -> Option<&OpeningRow> {
        self.get(OpeningSlot::for_attempt(attempt))
            .filter(|row| row.attempt == attempt)
            .or_else(|| match attempt {
                OpenAttempt::Create(_) => self.runs.values().find(|row| row.attempt == attempt),
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
        if attempts.targeted(attempt).is_some_and(|row| {
            matches!(
                row.state,
                OpeningState::HeldCreate(_)
                    | OpeningState::ConfiguredCreate(_)
                    | OpeningState::HeldRestore(_)
                    | OpeningState::PreparedRestore(_)
            )
        }) {
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

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
#[path = "owner_acp_lifecycle_refusal_test.rs"]
mod refusal_tests;
