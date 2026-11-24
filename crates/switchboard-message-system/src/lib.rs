//! Supervision and composition for one Router's message system.

mod interpreter;
mod machine;
mod message_front;
mod message_system;
mod message_tool_port;
mod message_unit;
mod protocol;
mod status;
mod stop_part;
mod teardown;
mod transition_table;

#[cfg(all(feature = "test-support", debug_assertions))]
pub mod test_support;

pub use interpreter::{CloseReport, MessageRoots, MessageSystemEvents};
pub use machine::{ObservationPhase, State, Supervisor, SupervisorError};
pub use message_front::MessageFront;
pub use message_system::MessageSystem;
pub use protocol::{
    ActiveEnrollment, Effect, EnrollmentId, EnrollmentPair, Event, Fault, Front, FrontRefusal,
    Generation, LeaseStopResult, Metadata, OrganizationId, PairError, ReadId, StopBudgets,
    StopReport, StopResult, StopTarget, UnitPiece,
};
pub use status::{MessageSystemStatus, message_system_status};
pub use teardown::{EnrollmentDuringDrain, QuiesceReason, Quiescing};
