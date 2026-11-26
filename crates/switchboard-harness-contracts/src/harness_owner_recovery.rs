//! Hand-back of a failed controller's writer custody.

use std::collections::{BTreeMap, BTreeSet};

use crate::{DispatchAuthorization, InputId, OwnerIncarnation, SlotEvidence};

/// The harness's answer for an input held by the failed owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnerInputEvidence {
    /// The harness withdrew the input's offers and has proof none was taken.
    Withdrawn {
        slots: Vec<(DispatchAuthorization, SlotEvidence)>,
    },
    /// At least one authorization was taken. Keep all records for the input,
    /// including those on retired runs. The remote effect may still be unknown.
    Taken {
        slots: Vec<(DispatchAuthorization, SlotEvidence)>,
    },
    /// The harness established that the input never reached an offer.
    NeverOffered,
    /// The harness could not establish this input's custody. Retain any
    /// slot records it did observe.
    NoEvidence {
        slots: Vec<(DispatchAuthorization, SlotEvidence)>,
    },
}

/// Exactly one answer for each requested input, keyed by its Router identity.
/// An unavailable record is reported as `NoEvidence` for every requested input.
pub type OwnerEvidence = BTreeMap<InputId, OwnerInputEvidence>;

pub trait HarnessOwnerRecovery: Send + Sync + 'static {
    /// Withdraw this incarnation's pending offers before handing back the evidence
    /// and retiring its routes. Preserve every recorded take. Answer every input
    /// with an accepted mark or a waiting caller. The caller draws no conclusion
    /// from an omitted entry.
    fn give_up_owner(&self, owner: OwnerIncarnation, inputs: &BTreeSet<InputId>) -> OwnerEvidence;
}
