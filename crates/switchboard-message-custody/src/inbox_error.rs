//! Errors shared by the recipient inbox and its consumers.

use switchboard_machine_core_store::InboxStoreError;
use workspace_ids::{MachineId, MessageId};

/// Admission failures reported by the recipient inbox to the custody task.
#[derive(Debug, thiserror::Error)]
pub enum InboxCustodyError {
    #[error("invalid central delivery offer")]
    InvalidOffer,
    #[error("central offered a delivery addressed to machine {0}")]
    WrongMachine(MachineId),
    #[error("delivery expiry is outside the supported clock range")]
    ExpiryOutOfRange,
    #[error("inbox storage unavailable")]
    Store(#[from] InboxStoreError),
    #[error("the inbox could not read the earlier decision for message {0}")]
    MissingDecision(MessageId),
}
