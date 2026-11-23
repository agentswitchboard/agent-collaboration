//! Apply a machine's typed session calls through its organization's storage owner.
#![forbid(unsafe_code)]

mod organization_loan;
mod session_dispatch;

pub use organization_loan::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture,
};
pub use session_dispatch::{SessionEntropyUnavailable, SessionLinkDispatcher};
