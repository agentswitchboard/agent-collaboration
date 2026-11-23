use std::sync::{Arc, Mutex, PoisonError};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum DueDiscoveryState {
    #[default]
    Idle,
    Claimed,
    Rediscover,
}

/// One active due drain for a loaded organization object.
#[derive(Default)]
pub struct DueBatchState {
    discovery: Mutex<DueDiscoveryState>,
    #[cfg(feature = "test-support")]
    unit_starts: Mutex<Vec<DueUnitStart>>,
    #[cfg(feature = "test-support")]
    batch_starts: Mutex<Vec<i64>>,
}

/// The holder can move the drain to another task. Dropping it releases the claim.
pub struct DueBatchClaim {
    owner: Option<Arc<DueBatchState>>,
}

/// Test-support receipt of one due unit, in visit order.
#[cfg(feature = "test-support")]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DueUnitStart {
    pub batch_start_unix_millis: i64,
    pub machine_kind: String,
    pub subject_key: String,
}

impl DueBatchState {
    /// A competing event leaves a rediscovery request for the current holder.
    pub fn claim(self: &Arc<Self>) -> Option<DueBatchClaim> {
        let mut state = self
            .discovery
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match *state {
            DueDiscoveryState::Idle => {
                *state = DueDiscoveryState::Claimed;
                Some(DueBatchClaim {
                    owner: Some(self.clone()),
                })
            }
            DueDiscoveryState::Claimed | DueDiscoveryState::Rediscover => {
                *state = DueDiscoveryState::Rediscover;
                None
            }
        }
    }

    #[cfg(feature = "test-support")]
    pub fn record_batch_start(&self, milliseconds: i64) {
        self.batch_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(milliseconds);
    }

    #[cfg(feature = "test-support")]
    pub fn record_unit_start(&self, start: DueUnitStart) {
        self.unit_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(start);
    }

    #[cfg(feature = "test-support")]
    pub fn batch_starts(&self) -> Vec<i64> {
        self.batch_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    #[cfg(feature = "test-support")]
    pub fn unit_starts(&self) -> Vec<DueUnitStart> {
        self.unit_starts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl DueBatchClaim {
    /// The pass begins a discovery that covers all events already reported.
    pub fn start_discovery(&self) {
        if let Some(owner) = &self.owner {
            *owner
                .discovery
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = DueDiscoveryState::Claimed;
        }
    }

    /// Release a clean drain atomically with the check for a new event.
    /// A dirty drain keeps the claim and the caller can take another pass.
    pub fn release_if_current(&mut self) -> bool {
        let Some(owner) = &self.owner else {
            return true;
        };
        let mut state = owner
            .discovery
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if *state == DueDiscoveryState::Rediscover {
            return false;
        }
        *state = DueDiscoveryState::Idle;
        drop(state);
        self.owner.take();
        true
    }
}

impl Drop for DueBatchClaim {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            *owner
                .discovery
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = DueDiscoveryState::Idle;
        }
    }
}

#[cfg(test)]
#[path = "due_batch_state_test.rs"]
mod tests;
