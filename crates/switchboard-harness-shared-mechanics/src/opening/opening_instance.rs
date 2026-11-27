//! Identity and dispatch custody shared by an opening and its exact effects.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

#[derive(Clone, Default)]
pub struct OpeningInstance(Arc<()>);

impl OpeningInstance {
    pub fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// The ordered writer and local retirement compete for one offer. Taking it
/// requires a reserved writer slot so a taken claim cannot later mean Full.
#[derive(Clone, Default)]
pub struct OpeningOffer(Arc<AtomicU8>);

const UNCLAIMED: u8 = 0;
const TAKEN: u8 = 1;
const WITHDRAWN: u8 = 2;

impl OpeningOffer {
    pub fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn take(&self) -> bool {
        self.0
            .compare_exchange(UNCLAIMED, TAKEN, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Withdraw an untaken offer and report whether the writer took it first.
    pub fn was_taken_or_withdraw(&self) -> bool {
        self.0
            .compare_exchange(UNCLAIMED, WITHDRAWN, Ordering::AcqRel, Ordering::Acquire)
            == Err(TAKEN)
    }
}
