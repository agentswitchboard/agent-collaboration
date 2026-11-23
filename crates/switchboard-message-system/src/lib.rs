//! Pure supervision for one collaboration runtime's message system.
//!
//! An existing driver parked on a terminal ending cannot recover the same
//! enrollment key at a newer binding generation until the unit restarts.
//! Process-lifetime notice history belongs to the caller of this machine.

mod machine;
mod protocol;
mod teardown;
mod transition_table;

pub use machine::{ObservationPhase, State, Supervisor, SupervisorError};
pub use protocol::{
    ActiveEnrollment, Effect, EnrollmentId, EnrollmentPair, Event, Fault, Front, FrontRefusal,
    Generation, Metadata, OrganizationId, PairError, ReadId, StopBudgets, StopReport, StopResult,
    StopTarget, UnitPiece,
};
pub use teardown::{EnrollmentDuringDrain, QuiesceReason, Quiescing};
