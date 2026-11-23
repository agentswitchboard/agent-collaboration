//! Per-connection routing of typed replies to live waiters.
use crate::{CorrelationId, FreshCorrelationId};
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
    /// Reserve space before dispatch. Parsed or previously issued wire IDs are lookup-only.
    ///
    /// ```compile_fail,E0308
    /// use central_link_protocol::{CorrelationId, TypedCallCorrelations};
    /// let parsed: CorrelationId = "019f0000-0000-7000-8000-000000000001".parse().unwrap();
    /// let mut pending = TypedCallCorrelations::default();
    /// pending.register(parsed, ()).unwrap();
    /// ```
    pub fn register(&mut self, fresh: FreshCorrelationId, waiter: Waiter) -> Result<(), Waiter> {
        let correlation = fresh.into_wire_id();
        if self.pending.len() == MAX_PENDING_TYPED_CALLS
            || self.pending.contains_key(&correlation)
            || self.resolved.contains(&correlation)
        {
            return Err(waiter);
        }
        self.pending.insert(correlation, waiter);
        Ok(())
    }

    /// Observe the live waiter from a test fixture without settling it.
    #[cfg(feature = "test-support")]
    pub fn pending(&self, correlation: &CorrelationId) -> Option<&Waiter> {
        self.pending.get(correlation)
    }

    /// Retire only after the pending attempt and its expected answer agree.
    pub fn take_if(
        &mut self,
        correlation: &CorrelationId,
        matches: impl FnOnce(&Waiter) -> bool,
    ) -> Option<Waiter> {
        if !matches(self.pending.get(correlation)?) {
            return None;
        }
        self.retire(correlation)
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
