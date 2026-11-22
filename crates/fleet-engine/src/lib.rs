//! Generic execution, attempts and administration for Fleet machines.
#![forbid(unsafe_code)]

mod machine_event_engine;

pub use machine_event_engine::admin_failure_class::EngineFailureClass;
pub use machine_event_engine::attempt_administration::{
    AdminMachineKindRegistry, AttemptRefusal, DiscardOutcome, DiscardSubjectStatements,
    EngineHealthFacts, MachineKindAttemptHealth, PreparedAttemptDiscard, RetiredAttemptSummary,
};
pub use machine_event_engine::attempt_statements::{
    UPSERT_CONTROL_ATTEMPT_SQL, plan_attempt_delete, plan_observed_attempt_delete,
};
pub use machine_event_engine::due_batch_execution::{
    DueCutoff, DueDiscoveryError, TimedDueDispatch, TimedRowOutcome, TimedSourceSchedule,
    WholeDueUnitControl, WholeUnitOutcome, read_due_rows,
};
pub use machine_event_engine::engine_host_binding::{EngineHost, FleetEngine};
pub use machine_event_engine::machine_event_execution::MachineEventOutcome;
#[cfg(feature = "test-support")]
pub use machine_event_engine::machine_request_execution::EngineRequestOutcome;
pub use machine_event_engine::machine_store_adapter::{
    MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState,
    classify_subject_decode_failure, classify_subject_read_failure, read_observed_subject_version,
};
#[cfg(feature = "test-support")]
pub use machine_event_engine::request_receipt_store::{EngineReceipt, EngineRefusal};

mod central_queries;
pub use central_queries::{read_alarm_inputs, read_timed_due_rows};
