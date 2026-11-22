//! Machine enrollment decisions and their organization-owned storage statements.
#![forbid(unsafe_code)]

mod enrollment_reads;
pub mod enrollment_row_codec;
pub mod enrollment_row_values;
mod enrollment_writes;
mod machine_login_service;
pub mod policy;

pub use machine_login_service::{
    MachineLoginPlan, MachineLoginService, PreparedEnrollmentRevocation,
};
