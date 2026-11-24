//! Generic execution, attempts and administration for Fleet machines.
#![forbid(unsafe_code)]

mod approval_clock_projection;
pub use approval_clock_projection::{approval_time_floor_statement, decode_approval_time_floor};

mod machine_event_engine;

pub use central_engine_model::FailureRecord;

pub use machine_event_engine::admin_failure_class::EngineFailureClass;

#[cfg(feature = "test-support")]
pub fn is_retired_attempt_page_sql(sql: &str) -> bool {
    sql == include_str!("queries/read_retired_attempt_page.sql")
        || sql == include_str!("queries/read_retired_attempt_continuation.sql")
}
pub use machine_event_engine::attempt_administration::{
    AdminMachineKindRegistry, AttemptRefusal, DiscardOutcome, DiscardSubjectStatements,
    EngineHealthFacts, MachineKindAttemptHealth, PreparedAttemptDiscard, RetiredAttemptSummary,
};
pub use machine_event_engine::attempt_statements::{
    plan_attempt_delete, plan_observed_attempt_delete,
};
pub use machine_event_engine::checked_attempts::{
    CheckedAttempt, CheckedDueFailure, CheckedDueUnit, checked_statement_rejection,
    observe_checked_attempt, record_checked_attempt,
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
    MachineStoreAdapter, SubjectAttemptWriter, SubjectLoadFailure, VersionedSubjectState,
    classify_subject_decode_failure, classify_subject_read_failure, read_observed_subject_version,
};
#[cfg(feature = "test-support")]
pub use machine_event_engine::request_receipt_store::{EngineReceipt, EngineRefusal};

#[doc(hidden)]
pub mod owner_sql;

mod due_table_contract;
pub use due_table_contract::{
    DueEntryKey, DueItemKey, DueKind, DueLane, DueMillis, DueOwnerTiming, DueTableChange,
};

mod due_table_planner;
pub use due_table_planner::{
    apply_due_table_change, due_change_for_attempt, plan_due_item_attempt_clear,
    plan_due_table_change,
};

mod central_queries;
pub use central_queries::{DueItemTimes, DueTableDifference, InvalidDueEntry, compare_due_table};
pub use central_queries::{read_alarm_inputs, read_timed_due_rows};
