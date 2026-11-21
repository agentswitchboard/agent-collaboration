//! Owns FleetRuntime mailbox rows, delivery and timed message transitions.
#![forbid(unsafe_code)]

pub mod direct_mark_apply;
mod direct_message;
mod direct_message_rows;
mod direct_store_port;
pub mod direct_store_reads;
mod direct_store_sender;
pub mod direct_store_writes;
mod due_queries;
pub mod message_deadline;
mod message_effects;
mod message_insert;
mod queued_notices;
mod request_plan;
mod stored_messages;
mod system_message_expiry;
mod system_message_expiry_store;

pub use direct_store_port::*;
pub use due_queries::{due_direct_messages, due_system_message_expiries};
pub use message_insert::insert_message;
pub use queued_notices::{plan_cancel_queued_notices_held, validate_queued_notices_held};
pub use request_plan::plan_sourced_notice;
pub use stored_messages::queued_for_session_held;
pub use system_message_expiry::{
    ExpireAt, ExpiryRefusal, State as SystemExpiryState, SystemMessageExpiry,
};
pub use system_message_expiry_store::SystemMessageExpiryStore;
