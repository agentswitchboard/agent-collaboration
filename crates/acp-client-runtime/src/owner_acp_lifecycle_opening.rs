//! One payload-owning ACP opening table for each owner binding.

#[path = "owner_acp_lifecycle_activation.rs"]
mod activation;
#[path = "owner_acp_opening_adapter.rs"]
mod adapter;
#[path = "owner_acp_lifecycle_close.rs"]
mod close;
#[path = "owner_acp_opening_create.rs"]
mod create;
#[path = "owner_acp_opening_observations.rs"]
mod observations;
#[path = "owner_acp_opening_restore.rs"]
mod restore;
#[path = "owner_acp_opening_setter.rs"]
mod setter;

use super::lifecycle_transport_refusal;
use crate::{ExternalProviderRuntimeError, RequestedProviderSettings, owner_wire::OwnerAcpWire};
use adapter::AcpOpening;
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{
        CreateAttempt, HarnessSessionId, Held, LifecycleRefusal, LifecycleReply, OpenAttempt,
        OpenRequest, OpenSetup, OpeningFact, OpeningFailure, RunRetirement, SessionLifecycleFact,
        SessionLifecyclePort,
    },
    session_lifecycle_machine::RestoreVia,
    session_owner::{OwnerAddress, ProviderWireFailureKind, RunAttempt},
};
use router_clock::RuntimeClock;
use switchboard_harness_shared_mechanics::opening::{
    CreateStart, OpenCreateRequest, OpeningRow, OpeningTable, ReaderEnd, RestoreDecision,
    RestoreOpening, RestoreStart, Slot as OpeningSlot,
};
type Command = switchboard_harness_shared_mechanics::opening::Command<AcpOpening>;
type Effect = switchboard_harness_shared_mechanics::opening::Effect<AcpOpening>;
type Rejected = switchboard_harness_shared_mechanics::opening::Rejected<AcpOpening>;
type ReaderHandoff = switchboard_harness_shared_mechanics::opening::ReaderHandoff<AcpOpening>;
type RestoreMilestone = switchboard_harness_shared_mechanics::opening::RestoreMilestone<AcpOpening>;
type OpeningAttempts = OpeningTable<AcpOpening>;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use switchboard_harness_contracts::WriterState;
use switchboard_harness_shared_mechanics::opening::{OpeningInstance, OpeningPhase};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

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
            Held::NotHeld(attempts.refusal_for(attempt, self.session.get().is_some()))
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

#[cfg(test)]
#[path = "owner_acp_opening_reply_envelopes_test.rs"]
mod reply_envelopes;
