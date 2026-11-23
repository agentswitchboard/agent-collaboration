use std::sync::{Arc, Mutex, PoisonError};

use crate::{BindingGeneration, ConditionalWrite, EnrollmentEnd, EnrollmentKey};

/// The reason reported to the owner by a link.
#[derive(Clone, Copy)]
pub enum RetirementRequest {
    Refusal,
    Definitive(EnrollmentEnd),
}

/// Synchronous retirement admission for one owner. It cannot refresh a credential.
#[derive(Clone, Default)]
pub struct RetirementFence(Arc<Mutex<Vec<FencedEnrollment>>>);

struct FencedEnrollment {
    key: EnrollmentKey,
    state: FenceState,
}

enum FenceState {
    Provisional {
        attempt: Arc<RetirementAttempt>,
        waiting: Option<EnrollmentEnd>,
        cleanup_requested: bool,
    },
    Terminal(Arc<RetirementAttempt>),
}

impl FenceState {
    fn attempt(&self) -> &Arc<RetirementAttempt> {
        match self {
            Self::Provisional { attempt, .. } | Self::Terminal(attempt) => attempt,
        }
    }
}

#[derive(Clone, Copy)]
enum Coverage {
    UnverifiedRefusal,
    Through(BindingGeneration),
    ExactKey,
}

pub(crate) struct RetirementAttempt {
    pub(crate) why: EnrollmentEnd,
    pub(crate) cleanup: tokio::sync::Mutex<()>,
    coverage: Coverage,
}

impl RetirementAttempt {
    fn new(why: EnrollmentEnd, coverage: Coverage) -> Arc<Self> {
        Arc::new(Self {
            why,
            coverage,
            cleanup: tokio::sync::Mutex::new(()),
        })
    }

    fn definitive(why: EnrollmentEnd) -> Arc<Self> {
        Self::new(why, Coverage::ExactKey)
    }

    pub(crate) fn covers(&self, generation: BindingGeneration) -> bool {
        match self.coverage {
            Coverage::UnverifiedRefusal => false,
            Coverage::Through(ceiling) => generation <= ceiling,
            Coverage::ExactKey => true,
        }
    }

    pub(crate) fn accepts_ending(&self, recorded: EnrollmentEnd) -> bool {
        !matches!(self.coverage, Coverage::ExactKey) || recorded == self.why
    }
}

impl RetirementFence {
    /// Reports an ending immediately. The first caller to request cleanup must queue it.
    pub fn request(&self, key: &EnrollmentKey, request: RetirementRequest) -> bool {
        let mut fences = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(existing) = fences.iter_mut().find(|entry| &entry.key == key) {
            let FenceState::Provisional {
                waiting,
                cleanup_requested,
                ..
            } = &mut existing.state
            else {
                return false;
            };
            if let RetirementRequest::Definitive(why) = request {
                waiting.get_or_insert(why);
            }
            return !std::mem::replace(cleanup_requested, true);
        }
        let state = match request {
            RetirementRequest::Refusal => FenceState::Provisional {
                attempt: RetirementAttempt::new(
                    EnrollmentEnd::RefreshRefused,
                    Coverage::UnverifiedRefusal,
                ),
                waiting: None,
                cleanup_requested: true,
            },
            RetirementRequest::Definitive(why) => {
                FenceState::Terminal(RetirementAttempt::definitive(why))
            }
        };
        fences.push(FencedEnrollment {
            key: key.clone(),
            state,
        });
        true
    }

    /// Returns whether the owner has fenced this exact enrollment.
    pub fn is_fenced(&self, key: &EnrollmentKey) -> bool {
        self.ending(key).is_some()
    }

    pub(crate) fn ending(&self, key: &EnrollmentKey) -> Option<EnrollmentEnd> {
        self.attempt(key).map(|attempt| attempt.why)
    }

    pub(crate) fn attempt(&self, key: &EnrollmentKey) -> Option<Arc<RetirementAttempt>> {
        let fences = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = fences.iter().find(|entry| &entry.key == key)?;
        Some(entry.state.attempt().clone())
    }

    pub(crate) fn is_current(&self, key: &EnrollmentKey, attempt: &Arc<RetirementAttempt>) -> bool {
        self.attempt(key)
            .is_some_and(|current| Arc::ptr_eq(&current, attempt))
    }

    pub(crate) fn record_refusal(
        &self,
        key: &EnrollmentKey,
        generation: BindingGeneration,
        why: EnrollmentEnd,
    ) -> Option<Arc<RetirementAttempt>> {
        let mut fences = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if fences.iter().any(|entry| &entry.key == key) {
            return None;
        }
        let attempt = RetirementAttempt::new(why, Coverage::Through(generation));
        fences.push(FencedEnrollment {
            key: key.clone(),
            state: FenceState::Provisional {
                attempt: attempt.clone(),
                waiting: None,
                cleanup_requested: false,
            },
        });
        Some(attempt)
    }

    pub(crate) fn settle(
        &self,
        key: &EnrollmentKey,
        completed: &Arc<RetirementAttempt>,
        result: ConditionalWrite,
    ) {
        let mut fences = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(entry) = fences.iter_mut().find(|entry| &entry.key == key) else {
            return;
        };
        let FenceState::Provisional {
            attempt, waiting, ..
        } = &entry.state
        else {
            return;
        };
        if !Arc::ptr_eq(attempt, completed) {
            return;
        }
        match (result, waiting) {
            (ConditionalWrite::Applied, _) => {
                entry.state = FenceState::Terminal(attempt.clone());
            }
            (ConditionalWrite::Superseded, Some(why)) => {
                entry.state = FenceState::Terminal(RetirementAttempt::definitive(*why));
            }
            (ConditionalWrite::Superseded, None) => {
                fences.retain(|entry| &entry.key != key);
            }
        }
    }
}
