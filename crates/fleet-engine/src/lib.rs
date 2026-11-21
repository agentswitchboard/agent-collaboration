//! Generic execution, attempts and administration for Fleet machines.
#![forbid(unsafe_code)]

mod admin_failure_class;
mod attempt_administration;
mod attempt_control_gate;
mod attempt_failure_writes;
mod attempt_records;
mod attempt_statements;
mod attempt_store;
mod due_batch_execution;
mod engine_host_binding;
mod event_source_policy;
mod machine_event_execution;
#[cfg(feature = "test-support")]
mod machine_request_execution;
mod machine_store_adapter;
#[cfg(any(feature = "test-support", test))]
mod request_receipt_store;
mod transition_batch_writes;

pub use admin_failure_class::EngineFailureClass;
pub use attempt_administration::{
    AdminMachineKindRegistry, AttemptRefusal, DiscardOutcome, DiscardSubjectStatements,
    EngineHealthFacts, MachineKindAttemptHealth, PreparedAttemptDiscard, RetiredAttemptSummary,
};
pub use attempt_statements::{
    UPSERT_CONTROL_ATTEMPT_SQL, plan_attempt_delete, plan_observed_attempt_delete,
};
pub use due_batch_execution::{
    DueCutoff, DueDiscoveryError, TimedDueDispatch, TimedRowOutcome, TimedSourceSchedule,
    WholeDueUnitControl, WholeUnitOutcome, read_due_rows,
};
pub use engine_host_binding::{EngineHost, FleetEngine};
pub use machine_event_execution::MachineEventOutcome;
#[cfg(feature = "test-support")]
pub use machine_request_execution::EngineRequestOutcome;
pub use machine_store_adapter::{
    MachineStoreAdapter, SubjectLoadFailure, VersionedSubjectState,
    classify_subject_decode_failure, classify_subject_read_failure, read_observed_subject_version,
};
#[cfg(feature = "test-support")]
pub use request_receipt_store::{EngineReceipt, EngineRefusal};

mod central_due_queries;
pub use central_due_queries::{CentralTransaction, read_alarm_inputs, read_timed_due_rows};
