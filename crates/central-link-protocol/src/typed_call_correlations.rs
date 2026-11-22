//! Per-connection routing of typed replies to live waiters.
use crate::CorrelationId;
use std::collections::{BTreeMap, BTreeSet};

pub struct TypedCallCorrelations<Waiter> {
    pending: BTreeMap<CorrelationId, Waiter>,
    issued: BTreeSet<CorrelationId>,
}

impl<Waiter> Default for TypedCallCorrelations<Waiter> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            issued: BTreeSet::new(),
        }
    }
}

impl<Waiter> TypedCallCorrelations<Waiter> {
    /// Reserve a fresh ID for this attempt, including across timed-out attempts.
    pub fn register(&mut self, correlation: CorrelationId, waiter: Waiter) -> Result<(), Waiter> {
        if !self.issued.insert(correlation.clone()) {
            return Err(waiter);
        }
        self.pending.insert(correlation, waiter);
        Ok(())
    }

    /// Replies, duplicate replies and replies after retirement use the same lookup.
    pub fn retire(&mut self, correlation: &CorrelationId) -> Option<Waiter> {
        self.pending.remove(correlation)
    }

    /// Return every unsettled attempt when the connection ends.
    pub fn drain_pending(&mut self) -> impl Iterator<Item = Waiter> {
        std::mem::take(&mut self.pending).into_values()
    }
}

#[cfg(test)]
#[path = "typed_call_correlations_test.rs"]
mod tests;
