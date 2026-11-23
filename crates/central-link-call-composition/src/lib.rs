//! Apply a machine's typed machine-link calls through its organization's storage owner.
#![forbid(unsafe_code)]

mod apply_failure;
mod approval_applying_time;
mod approval_authority_end;
mod approval_deadline;
mod approval_delivery;
mod approval_dispatch;
#[cfg(feature = "test-support")]
mod approval_fixture;
mod approval_notice_recovery;
mod approval_transaction;
mod organization_dispatch;
mod organization_loan;
mod parent_session_dispatch;

pub use apply_failure::{
    OrganizationLoanFailure, SessionApplyFailure, SessionApplyFault, SessionApplyResult,
    SessionApplySuccess, SessionWiringFault,
};
pub use approval_delivery::{ApplyEndingAnswer, ApprovalDeliveryTransport, SelectedApprovalSocket};
pub use approval_transaction::{
    ApprovalAlarmPublication, ApprovalStatementLoan, ApprovalTransactionFailure,
    ApprovalTransactionFuture, CommittedApprovalApply, OrganizationApprovalTransactions,
};
pub use organization_dispatch::{OrganizationLinkDispatcher, SessionEntropyUnavailable};
pub use organization_loan::{
    OrganizationLoan, OrganizationSessionTransactions, SessionApplyFuture,
};

#[cfg(feature = "test-support")]
pub use apply_failure::inject_reserved_reply_fault;

pub use fleet_approvals::CommittedApprovalNotice;
