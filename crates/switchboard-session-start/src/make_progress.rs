//! Durable Make inputs and evidence shared by harness adapters.

use workspace_ids::{RequestId, SessionId};

/// A Router-minted correlation for the one authorized initial input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FirstInputId(pub RequestId);

/// The live ask supplies the content. The row holds its ID and fingerprint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FirstInputIntent {
    pub input_id: FirstInputId,
    pub fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MakePlan {
    HeldOnLaunch,
    FirstInputRequired(FirstInputIntent),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchProgress {
    Intent,
    Allocated {
        harness: SessionId,
    },
    FirstInputIntended {
        harness: SessionId,
        input: FirstInputIntent,
    },
}

/// Positive evidence from the harness adapter's durable lookup of this exact input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableFirstInput {
    pub harness: SessionId,
    pub input: FirstInputIntent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FirstInputObservation {
    /// A stored harness session holds exactly the authorized input correlation.
    DurablyTaken,
    /// A live answer needs a read of the durable session before it can be trusted.
    NeedsRead,
    /// The lookup or dispatch did not establish the exact durable input.
    Unconfirmed,
    /// Exact ownership is known, and setup cannot continue.
    SetupFailed,
}
