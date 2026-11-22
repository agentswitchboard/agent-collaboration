//! Per-connection routing of typed replies to live waiters.
use crate::CorrelationId;
use std::collections::{BTreeMap, VecDeque};

pub const MAX_PENDING_TYPED_CALLS: usize = 64;
const MAX_RECENT_TYPED_CORRELATIONS: usize = 128;

pub struct TypedCallCorrelations<Waiter> {
    pending: BTreeMap<CorrelationId, Waiter>,
    resolved: VecDeque<CorrelationId>,
}

impl<Waiter> Default for TypedCallCorrelations<Waiter> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            resolved: VecDeque::new(),
        }
    }
}

impl<Waiter> TypedCallCorrelations<Waiter> {
    /// Reserve space before dispatch. A retry must mint a fresh correlation.
    pub fn register(&mut self, correlation: CorrelationId, waiter: Waiter) -> Result<(), Waiter> {
        if self.pending.len() == MAX_PENDING_TYPED_CALLS
            || self.pending.contains_key(&correlation)
            || self.resolved.contains(&correlation)
        {
            return Err(waiter);
        }
        self.pending.insert(correlation, waiter);
        Ok(())
    }

    /// Replies, duplicate replies and replies after retirement use the same lookup.
    pub fn retire(&mut self, correlation: &CorrelationId) -> Option<Waiter> {
        let waiter = self.pending.remove(correlation)?;
        if self.resolved.len() == MAX_RECENT_TYPED_CORRELATIONS {
            self.resolved.pop_front();
        }
        self.resolved.push_back(correlation.clone());
        Some(waiter)
    }

    /// Return every unsettled attempt when the connection ends.
    pub fn drain_pending(&mut self) -> impl Iterator<Item = Waiter> {
        self.resolved.clear();
        std::mem::take(&mut self.pending).into_values()
    }
}

#[cfg(test)]
#[path = "typed_call_correlations_test.rs"]
mod tests;
