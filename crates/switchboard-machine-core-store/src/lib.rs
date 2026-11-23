//! SQLite persistence for the Mac Router's machine identity, outbox and inbox.

mod enrollment_row;
mod enrollment_row_store;
mod inbox_store;
mod machine_identity_store;
mod machine_local_store;
mod outbox_deadline;
mod outbox_item_state;
mod outbox_queued_write;
mod outbox_receipt_profile;
mod outbox_settlement;
mod outbox_status;
mod outbox_storage;
mod outbox_store;
mod outbox_views;
mod sender_outcome;
mod store_scan;

pub use enrollment_row::{
    EnrollmentRowCorruption, StoredEnrollmentEnd, StoredEnrollmentRow,
    StoredInvalidEnrollmentReason,
};
pub use enrollment_row_store::{EnrollmentRowChange, EnrollmentRowStoreError, EnrollmentRowWrite};
pub use inbox_store::{
    InboxDecision, InboxOfferExpiry, InboxReceipt, InboxRefusalReason, InboxStore, InboxStoreError,
    MAX_INBOX_DELIVERY_WINDOW_MS, RecordDelivery,
};
pub use machine_identity_store::{MachineIdentityError, load_or_create_machine_id};
pub use machine_local_store::{MACHINE_LOCAL_DATABASE, MachineLocalStore, MachineLocalStoreError};
pub use outbox_queued_write::OutboxQueuedWrite;
pub use outbox_settlement::{InvalidOutboxOutcome, OutboxWriteOutcome};
pub use outbox_status::{
    LocalOutboxRefusal, OutboxAccepted, OutboxCancellation, OutboxCentralRefusal, OutboxRefusal,
    OutboxRequestKey, OutboxRequestStatus, OutboxScope,
};
pub use outbox_storage::{MachineLocalOutbox, OutboxFuture, OutboxStorage};
pub use outbox_store::{
    ClaimedBatch, MacOutbox, NewOutboxItem, OutboxInsert, OutboxStoreError, StoredOutboxWrite,
    UnansweredOutboxClaim,
};
pub use outbox_views::{
    EndedWrite, OutboxWriteTarget, OutgoingOutcome, OutgoingWrite, PendingWrite, PendingWriteState,
};
pub use sender_outcome::StoredSenderOutcome;
pub use store_scan::{RowCorruption, RowCorruptionReason, StoreScan};

pub(crate) use outbox_views::OutboxWriteContent;
