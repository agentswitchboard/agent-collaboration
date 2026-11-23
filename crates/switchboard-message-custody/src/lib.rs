//! Mac message custody policy. The machine core store owns durable records.

pub mod delivery_custody;
pub mod flush_clock;
pub mod flush_marks;
pub mod outbox_flush;
pub mod write_custody;
