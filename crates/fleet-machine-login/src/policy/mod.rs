//! Pure organization-owned machine-login decisions and guarded change plans.
mod allowance_transitions;
mod decision_support;
mod device_login_decision;
mod enrollment_records;
mod enrollment_transitions;
mod machine_token_decision;
mod revoke_enrollment;

pub use decision_support::{DecisionTime, IDLE_EXPIRY_SECONDS};
pub use device_login_decision::decide_device_login;
pub use enrollment_records::*;
pub use machine_token_decision::decide_machine_token;
pub use revoke_enrollment::revoke_machine_enrollment;

#[cfg(test)]
mod policy_fixture;
