use std::future::Future;

use central_storage::{ReadOutcome, Statement, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::OrganizationTransaction;
use workspace_ids::ActorIdentityId;

use crate::{ApprovalActor, approval_store_scope::ApprovalEnrollmentScope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalEnrollmentStanding {
    Live,
    Ended,
    Unavailable,
}

/// The host lends the already applying transaction and retains commit and rollback.
/// Its enrollment lookup delegates to the machine-login owner in that transaction.
pub trait ApprovalStoreTransaction: OrganizationTransaction {
    fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<ReadOutcome, StoreFailure>>;

    fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<u64, StoreFailure>>;

    fn approval_enrollment_standing(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> impl Future<Output = Result<ApprovalEnrollmentStanding, StoreFailure>>;

    /// Resolve the already admitted destination through Central's actor identity owner.
    fn approval_decision_caller(
        &mut self,
        organization: &OrganizationId,
        actor: &ApprovalActor,
    ) -> impl Future<Output = Result<ActorIdentityId, StoreFailure>>;
}
