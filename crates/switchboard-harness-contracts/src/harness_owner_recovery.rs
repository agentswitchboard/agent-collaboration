//! Hand-back of a failed controller's writer custody.

use crate::{DispatchAuthorization, OwnerIncarnation, SlotEvidence};

/// Evidence for one exact dispatch authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnerSlotEvidence {
    /// Revoked proves withdrawal. Taken proves custody. An offered slot
    /// supplies no proof of withdrawal.
    Recorded(SlotEvidence),
    /// The harness has positive evidence this work never reached an offer.
    NeverOffered,
    /// Custody could not be established for this authorization.
    NoEvidence,
}

/// What the harness knew about the failed owner's dispatch authorizations.
/// A taken slot establishes writer custody. Its remote effect may still be unknown.
/// An omitted authorization supplies no evidence even in a complete hand-back.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnerEvidence {
    /// The complete record for this incarnation, including takes on retired runs.
    Kept(Vec<(DispatchAuthorization, OwnerSlotEvidence)>),
    /// These entries are known. An omitted authorization has unknown custody.
    Partial(Vec<(DispatchAuthorization, OwnerSlotEvidence)>),
    /// Custody could not be established for this incarnation.
    Unavailable,
}

pub trait HarnessOwnerRecovery: Send + Sync + 'static {
    /// Withdraw this incarnation's pending offers before handing back the evidence
    /// and retiring its routes. Preserve every recorded take.
    fn give_up_owner(&self, owner: OwnerIncarnation) -> OwnerEvidence;
}
