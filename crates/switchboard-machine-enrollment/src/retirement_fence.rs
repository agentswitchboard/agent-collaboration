use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;

use crate::{BindingGeneration, EnrollmentEnd, EnrollmentKey};

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
    attempt: Arc<RetirementAttempt>,
    waiting: Option<EnrollmentEnd>,
    status: RetirementStatus,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RetirementStatus {
    Pending,
    Durable,
    Superseded,
}

#[derive(Clone, Copy)]
pub(crate) enum RetirementOutcome {
    Durable,
    Superseded,
    StaleOwnerEnding,
}

pub(crate) struct RetirementAttempt {
    pub(crate) why: EnrollmentEnd,
    pub(crate) through: Option<BindingGeneration>,
    pub(crate) cleanup: tokio::sync::Mutex<()>,
}

impl RetirementAttempt {
    fn new(why: EnrollmentEnd, through: Option<BindingGeneration>) -> Arc<Self> {
        Arc::new(Self {
            why,
            through,
            cleanup: tokio::sync::Mutex::new(()),
        })
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
        if let Some(existing) = entries.iter_mut().find(|entry| &entry.key == key) {
            if existing.status == RetirementStatus::Pending
                && existing.attempt.through.is_some()
                && existing.waiting.is_none()
            {
                existing.waiting = Some(why);
                drop(entries);
                self.0.changes.send_replace(());
            }
            return;
        }
        entries.push(FencedEnrollment {
            key: key.clone(),
            attempt: RetirementAttempt::new(why, None),
            waiting: None,
            status: RetirementStatus::Pending,
        });
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
            .filter(|entry| entry.status == RetirementStatus::Pending)
            .map(|entry| entry.key.clone())
            .collect()
    }

    /// Returns whether the owner has fenced this exact enrollment.
    pub fn is_fenced(&self, key: &EnrollmentKey) -> bool {
        self.ending(key).is_some()
    }

    pub(crate) fn ending(&self, key: &EnrollmentKey) -> Option<EnrollmentEnd> {
        self.0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .find(|entry| &entry.key == key)
            .map(|entry| entry.attempt.why)
    }

    pub(crate) fn pending_attempt(&self, key: &EnrollmentKey) -> Option<Arc<RetirementAttempt>> {
        let entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let entry = entries
            .iter()
            .find(|entry| &entry.key == key && entry.status == RetirementStatus::Pending)?;
        Some(entry.attempt.clone())
    }

    pub(crate) fn record_owner_ending(
        &self,
        key: &EnrollmentKey,
        generation: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Option<Arc<RetirementAttempt>> {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if entries.iter().any(|entry| &entry.key == key) {
            return None;
        }
        let attempt = RetirementAttempt::new(why, Some(generation));
        entries.push(FencedEnrollment {
            key: key.clone(),
            attempt: attempt.clone(),
            waiting: None,
            status: RetirementStatus::Pending,
        });
        drop(entries);
        self.0.changes.send_replace(());
        Some(attempt)
    }

    pub(crate) fn settle(
        &self,
        key: &EnrollmentKey,
        attempt: &Arc<RetirementAttempt>,
        outcome: RetirementOutcome,
    ) {
        let mut entries = self
            .0
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = entries
            .iter_mut()
            .find(|entry| &entry.key == key && entry.status == RetirementStatus::Pending)
        else {
            return;
        };
        if !Arc::ptr_eq(&entry.attempt, attempt) {
            return;
        }
        match outcome {
            RetirementOutcome::Durable => entry.status = RetirementStatus::Durable,
            RetirementOutcome::Superseded => entry.status = RetirementStatus::Superseded,
            RetirementOutcome::StaleOwnerEnding => match entry.waiting.take() {
                Some(why) => entry.attempt = RetirementAttempt::new(why, None),
                None => entries.retain(|entry| &entry.key != key),
            },
        }
        drop(entries);
        self.0.changes.send_replace(());
    }
}
