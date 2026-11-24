use std::sync::{Arc, Mutex, PoisonError};

use state_machine_core::StateMachine;
use tokio::sync::{Mutex as AsyncMutex, watch};

use crate::{
    BindingGeneration, EnrollmentEnd, EnrollmentKey,
    retirement_machine::{
        RetirementEvent, RetirementMachine, RetirementOutcome, RetirementRequest, RetirementState,
    },
};

/// Synchronous retirement admission and observation for one owner.
#[derive(Clone, Default)]
pub struct RetirementFence(Arc<RetirementEntries>);

struct RetirementEntries {
    entries: Mutex<Vec<FencedEnrollment>>,
    changes: watch::Sender<()>,
}

impl Default for RetirementEntries {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
            changes: watch::channel(()).0,
        }
    }
}

struct FencedEnrollment {
    key: EnrollmentKey,
    state: RetirementState,
    cleanup: Arc<AsyncMutex<()>>,
}

impl FencedEnrollment {
    fn new(key: &EnrollmentKey, state: RetirementState) -> Self {
        Self {
            key: key.clone(),
            state,
            cleanup: Arc::default(),
        }
    }

    fn pending_attempt(&self) -> Option<RetirementAttempt> {
        let RetirementState::Pending { request, .. } = self.state else {
            return None;
        };
        Some(RetirementAttempt {
            request,
            cleanup: self.cleanup.clone(),
        })
    }
}

pub(super) struct RetirementAttempt {
    pub(super) request: RetirementRequest,
    pub(super) cleanup: Arc<AsyncMutex<()>>,
}

impl RetirementAttempt {
    pub(super) fn same_cleanup(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cleanup, &other.cleanup)
    }
}

impl RetirementFence {
    /// Records the definitive report immediately, behind an earlier owner ending if pending.
    pub fn request_definitive(&self, key: &EnrollmentKey, why: EnrollmentEnd) {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let event = RetirementEvent::DefinitiveRequested(why);
        if let Some(entry) = entries.iter_mut().find(|entry| &entry.key == key) {
            let Ok((next, _)) = RetirementMachine::transition(&entry.state, event) else {
                return;
            };
            // Queuing a reason keeps the current cleanup identity.
            entry.state = next;
        } else {
            let Ok((next, _)) = RetirementMachine::transition(&RetirementState::Unfenced, event)
            else {
                return;
            };
            entries.push(FencedEnrollment::new(key, next));
        }
        drop(entries);
        self.0.changes.send_replace(());
    }

    /// Wakes a reconciler when the fence changes. Pending entries can also predate this receiver.
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.0.changes.subscribe()
    }

    /// Returns all keys with unsettled retirement work, independently of any lease caller.
    pub fn pending_keys(&self) -> Vec<EnrollmentKey> {
        self.0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|entry| matches!(entry.state, RetirementState::Pending { .. }))
            .map(|entry| entry.key.clone())
            .collect()
    }

    /// Returns whether the owner has fenced this exact enrollment.
    pub fn is_fenced(&self, key: &EnrollmentKey) -> bool {
        self.ending(key).is_some()
    }

    pub(super) fn ending(&self, key: &EnrollmentKey) -> Option<EnrollmentEnd> {
        self.0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|entry| &entry.key == key)
            .and_then(|entry| entry.state.ending())
    }

    pub(super) fn pending_attempt(&self, key: &EnrollmentKey) -> Option<RetirementAttempt> {
        self.0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|entry| &entry.key == key)
            .and_then(FencedEnrollment::pending_attempt)
    }

    pub(super) fn record_owner_ending(
        &self,
        key: &EnrollmentKey,
        generation: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Option<RetirementAttempt> {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let current = entries
            .iter()
            .find(|entry| &entry.key == key)
            .map_or(RetirementState::Unfenced, |entry| entry.state);
        let (next, _) = RetirementMachine::transition(
            &current,
            RetirementEvent::OwnerEndingRequested {
                why,
                through: generation,
            },
        )
        .ok()?;
        let entry = FencedEnrollment::new(key, next);
        let attempt = entry.pending_attempt()?;
        entries.push(entry);
        drop(entries);
        self.0.changes.send_replace(());
        Some(attempt)
    }

    pub(super) fn settle(
        &self,
        key: &EnrollmentKey,
        attempt: &RetirementAttempt,
        outcome: RetirementOutcome,
    ) {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = entries
            .iter_mut()
            .find(|entry| &entry.key == key && Arc::ptr_eq(&entry.cleanup, &attempt.cleanup))
        else {
            return;
        };
        let Ok((next, _)) =
            RetirementMachine::transition(&entry.state, RetirementEvent::CleanupSettled(outcome))
        else {
            return;
        };
        if matches!(next, RetirementState::StaleOwnerEnding(_)) {
            // Both transitions run under this one lock. StaleOwnerEnding is
            // never stored or observable, so a definitive report cannot
            // interleave between settlement and promotion or removal.
            let Ok((released, _)) =
                RetirementMachine::transition(&next, RetirementEvent::ReleaseStaleOwnerEnding)
            else {
                return;
            };
            if released == RetirementState::Unfenced {
                entries.retain(|entry| &entry.key != key);
            } else {
                *entry = FencedEnrollment::new(key, released);
            }
        } else {
            entry.state = next;
        }
        drop(entries);
        self.0.changes.send_replace(());
    }
}
