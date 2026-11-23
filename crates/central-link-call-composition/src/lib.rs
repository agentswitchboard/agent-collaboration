//! Apply a machine's typed session calls through its organization's storage owner.
#![forbid(unsafe_code)]

mod apply_failure;
mod organization_loan;
mod session_dispatch;

pub use apply_failure::{SessionApplyFailure, SessionApplyFault, SessionApplyResult};
pub use organization_loan::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture,
};
pub use session_dispatch::{SessionEntropyUnavailable, SessionLinkDispatcher};

#[cfg(feature = "test-support")]
pub use apply_failure::inject_reserved_reply_fault;
