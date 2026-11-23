//! Mac message custody policy. The machine core store owns durable records.

pub mod custody_link;
pub mod custody_operator;
pub mod custody_status;
pub mod custody_submission;
pub mod custody_task;
pub mod delivery_custody;
pub mod flush_clock;
pub mod flush_marks;
mod offer_taker;
pub mod outbox_dispatch;
pub mod outbox_flush;
mod outbox_wire;
pub mod outbox_work;
pub mod receipt_delivery;
pub mod recipient_inbox;
pub mod recipient_sessions;
pub mod write_custody;
