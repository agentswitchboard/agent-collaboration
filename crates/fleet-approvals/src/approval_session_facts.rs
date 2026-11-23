//! Session facts the approvals service reads inside its applying transaction.
use std::future::Future;

use central_storage::StoreFailure;
use fleet_lent_transaction::OrganizationTransaction;

use crate::{ApprovalRouting, ApprovalSession};

/// Implemented by the owner of the Central session records.
pub trait ApprovalSessionFacts<Transaction: OrganizationTransaction + Send>: Sync {
    /// Read the requester's route for its exact organization, machine, enrollment
    /// and Central session ID. Return None when that session does not match the scope.
    /// A delegate route carries the real parent's full session scope.
    fn session_initial_route(
        &self,
        transaction: &mut Transaction,
        requester: &ApprovalSession,
    ) -> impl Future<Output = Result<Option<ApprovalRouting>, StoreFailure>> + Send;

    /// Find the next live ancestor of this session or the top of the chain.
    fn next_approval_parent(
        &self,
        transaction: &mut Transaction,
        session: &ApprovalSession,
    ) -> impl Future<Output = Result<Option<ApprovalSession>, StoreFailure>> + Send;

    /// Return false for a missing, out-of-scope, or ended session.
    fn session_is_live(
        &self,
        transaction: &mut Transaction,
        session: &ApprovalSession,
    ) -> impl Future<Output = Result<bool, StoreFailure>> + Send;
}
