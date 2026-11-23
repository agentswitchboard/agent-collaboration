//! SQLite persistence for the Mac Router's machine identity, outbox and inbox.

mod inbox_store;
mod machine_identity_store;
mod machine_local_store;

pub use inbox_store::{
    InboxDecision, InboxReceipt, InboxRefusalReason, InboxStore, InboxStoreError, RecordDelivery,
};
pub use machine_identity_store::{MachineIdentityError, load_or_create_machine_id};
pub use machine_local_store::{MACHINE_LOCAL_DATABASE, MachineLocalStore, MachineLocalStoreError};
