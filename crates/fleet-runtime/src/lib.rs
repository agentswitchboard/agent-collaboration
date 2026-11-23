//! Organization-scoped execution using the host's injected capabilities.
#![forbid(unsafe_code)]

mod alarm_decision;
mod alarm_schedule;
mod clock_ids;
mod due_batch_state;
mod fleet_runtime;
#[cfg(feature = "test-support")]
mod pause_points;
mod reply_due_work;
#[cfg(feature = "test-support")]
mod test_faults;
pub mod workspace_request_log;
mod workspace_turns;

pub use alarm_decision::engine_alarm_decider;
pub use alarm_schedule::{AlarmDeadline, AlarmPublicationStatus, AlarmSchedule};
pub use clock_ids::ClockIds;
#[cfg(feature = "test-support")]
pub use due_batch_state::DueUnitStart;
pub use due_batch_state::{DueBatchClaim, DueBatchState};
pub use fleet_runtime::{FleetRuntime, TurnGuard};
#[cfg(feature = "test-support")]
pub use pause_points::{HeldPause, PausePoint, PausePoints};
pub use reply_due_work::{
    DeferredDueTask, DueBatchYield, DueBatchYieldFailure, ReplyDueWork, ReplyDueWorkOwner,
    collect_reply_due_work, has_reply_due_scope, queue_reply_due_work, yield_shared_due_batches,
};
#[cfg(feature = "test-support")]
pub use test_faults::{
    DirectMarkAnswerLoss, EngineCommitFault, EngineCommitFaultKind, EngineReadFault,
    LegacyUnitInvocations, TestFaults,
};
pub use workspace_turns::{WorkspaceTurn, WorkspaceTurns};
