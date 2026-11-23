//! Machine enrollment decisions and their organization-owned storage statements.
#![forbid(unsafe_code)]

mod enrollment_reads;
pub mod enrollment_row_codec;
pub mod enrollment_row_values;
mod enrollment_writes;
pub mod link_authority;
pub mod link_transitions;
mod machine_login_service;
pub mod policy;
pub mod receipt_authority;

pub use machine_login_service::{
    MachineLoginPlan, MachineLoginService, PreparedEnrollmentRevocation,
};

mod session_enrollment_reads;
pub use session_enrollment_reads::{
    SessionEnrollmentStatus, machine_can_start_in_workspace,
    machine_can_start_in_workspace_in_transaction, read_session_enrollment,
};
