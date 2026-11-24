//! Builds and owns one Mac message link for the collaboration runtime.
//!
//! If startup found no active enrollment, restart the Router after enrolling
//! to pick it up. The snapshot is taken when the collaboration runtime is
//! created. After an active start, link settings and leases handle changes
//! within their existing enrollment scope.
//! The enrollment store enforces one organization per Mac.

mod message_front;
mod message_runtime;
mod message_unit;
mod unit_shutdown;

pub use message_front::MessageFront;
pub use message_runtime::{MessageRuntime, MessageRuntimeStopReport};
pub use message_unit::{ActiveEnrollment, MessageUnit, MessageUnitBuildError, MessageUnitEvents};
pub use switchboard_message_system::FrontRefusal;
pub use unit_shutdown::{LinkTaskFailure, MessageUnitStopReport, PieceStop};

#[cfg(feature = "test-support")]
pub mod test_support;
