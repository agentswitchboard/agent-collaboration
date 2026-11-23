use crate::{OrganizationLoan, OrganizationLoanFailure};
use central_storage::{AlarmPublicationFailure, ReadOutcome, Statement, StoreFailure};
use fleet_approvals::{
    ApprovalActor, ApprovalApplyFailure, ApprovalCommitOwner, ApprovalCommitProof,
    ApprovalCommitScope, ApprovalEnrollmentScope, ApprovalEnrollmentStanding,
    ApprovalStoreTransaction,
};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use fleet_machine_login::{SessionEnrollmentStatus, read_session_enrollment};
use fleet_runtime_ports::{SyncTime, workspace_decode_error::corrupt};
use fleet_signin::ApprovalPersonTransaction;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{convert::Infallible, future::Future, pin::Pin};
use workspace_ids::ActorIdentityId;

#[derive(Clone, Debug)]
pub enum ApprovalAlarmPublication {
    WithCommit,
    /// Native publication is after commit. Preserve the committed person output
    /// for its holder before reporting publication failure to the HTTP caller.
    WithPersonCommit(std::sync::Arc<std::sync::OnceLock<AlarmPublicationFailure>>),
    AtDueBatchEnd,
}

impl ApprovalAlarmPublication {
    pub const fn publishes_with_commit(&self) -> bool {
        match self {
            Self::WithCommit | Self::WithPersonCommit(_) => true,
            Self::AtDueBatchEnd => false,
        }
    }
}

#[derive(Debug)]
pub enum ApprovalTransactionFailure {
    Store(StoreFailure),
    Loan(OrganizationLoanFailure),
    RolledBack(ApprovalApplyFailure),
}

impl From<OrganizationLoanFailure> for ApprovalTransactionFailure {
    fn from(failure: OrganizationLoanFailure) -> Self {
        Self::Loan(failure)
    }
}

impl From<StoreFailure> for ApprovalTransactionFailure {
    fn from(failure: StoreFailure) -> Self {
        Self::Store(failure)
    }
}

pub type ApprovalTransactionFuture<'loan, Output> =
    Pin<Box<dyn Future<Output = Result<Output, ApprovalApplyFailure>> + Send + 'loan>>;

/// Only the backend knows how to bind a statement to its physical transaction.
pub trait ApprovalStatementLoan: CentralTransaction + Send {
    fn sample_time(&self) -> Result<SyncTime, StoreFailure>;

    fn read_lent(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<ReadOutcome, StoreFailure>> + Send;

    fn execute_lent(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<u64, StoreFailure>> + Send;
}

/// The socket and the engine share the same stamped approving loan on each host.
pub trait OrganizationApprovalTransactions: Send + Sync + 'static {
    type Transaction<'connection>: ApprovalStoreTransaction;

    fn apply_approval<Output: Send + 'static>(
        &self,
        organization: OrganizationId,
        alarm: ApprovalAlarmPublication,
        apply: impl for<'loan, 'connection> FnOnce(
            &'loan mut Self::Transaction<'connection>,
        ) -> ApprovalTransactionFuture<'loan, Output>
        + Send
        + 'static,
    ) -> impl Future<Output = Result<Output, ApprovalTransactionFailure>> + Send;
}

type ApprovalCommitFuture<T> =
    Pin<Box<dyn Future<Output = Result<T, ApprovalTransactionFailure>> + Send>>;

pub(crate) fn commit_approval<Owner, Output>(
    transactions: std::sync::Arc<Owner>,
    organization: OrganizationId,
    alarm: ApprovalAlarmPublication,
    apply: impl for<'loan, 'connection> FnOnce(
        &'loan mut Owner::Transaction<'connection>,
        ApprovalCommitScope,
    ) -> ApprovalTransactionFuture<'loan, Output>
    + Send
    + 'static,
) -> ApprovalCommitFuture<(ApprovalCommitProof, Output)>
where
    Owner: OrganizationApprovalTransactions,
    Output: Send + 'static,
{
    let owner = ApprovalCommit(move |scope| -> ApprovalCommitFuture<Output> {
        Box::pin(async move {
            transactions
                .apply_approval(organization, alarm, move |loan| apply(loan, scope))
                .await
        })
    });
    Box::pin(fleet_approvals::complete_approval_commit(owner))
}

struct ApprovalCommit<Apply>(Apply);

impl<Apply, Output> ApprovalCommitOwner<Output> for ApprovalCommit<Apply>
where
    Apply: FnOnce(ApprovalCommitScope) -> ApprovalCommitFuture<Output> + Send,
{
    type Error = ApprovalTransactionFailure;
    type Commit = ApprovalCommitFuture<Output>;

    fn apply_and_commit(self, scope: ApprovalCommitScope) -> Self::Commit {
        (self.0)(scope)
    }
}

impl<Loan, Db> ApprovalStoreTransaction for OrganizationLoan<Loan>
where
    Loan: ApprovalStatementLoan<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    async fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<ReadOutcome, StoreFailure> {
        self.transaction_mut().read_lent(statement).await
    }

    async fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<u64, StoreFailure> {
        self.transaction_mut().execute_lent(statement).await
    }

    async fn approval_enrollment_standing(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> Result<ApprovalEnrollmentStanding, StoreFailure> {
        use fleet_lent_transaction::OrganizationTransaction;
        if &scope.organization_id != self.organization_id() {
            return Ok(ApprovalEnrollmentStanding::Unavailable);
        }
        let enrollment = &scope.enrollment;
        let standing =
            match read_session_enrollment(self, &enrollment.machine_id, &enrollment.enrollment_id)
                .await?
            {
                SessionEnrollmentStatus::Current => ApprovalEnrollmentStanding::Live,
                SessionEnrollmentStatus::Ended { .. } => ApprovalEnrollmentStanding::Ended,
                SessionEnrollmentStatus::Unavailable => ApprovalEnrollmentStanding::Unavailable,
            };
        Ok(standing)
    }

    async fn approval_decision_caller(
        &mut self,
        organization: &OrganizationId,
        actor: &ApprovalActor,
    ) -> Result<ActorIdentityId, StoreFailure> {
        ApprovalAnswerActorsNotAdmitted
            .resolve(organization, actor)
            .map(|never| match never {})
    }
}

/// The holder/maintenance composition has no constructor for an admitted answer actor.
struct ApprovalAnswerActorsNotAdmitted;

impl ApprovalAnswerActorsNotAdmitted {
    fn resolve(
        &self,
        _organization: &OrganizationId,
        _actor: &ApprovalActor,
    ) -> Result<Infallible, StoreFailure> {
        Err(corrupt("approval_link.actor_not_admitted"))
    }
}

#[cfg(feature = "test-support")]
impl ApprovalTransactionFailure {
    pub fn fixture_rejected_write() -> Self {
        Self::RolledBack(ApprovalApplyFailure::GuardChanged)
    }
}

impl<Loan: ApprovalStatementLoan> ApprovalPersonTransaction for OrganizationLoan<Loan> {
    fn sample_person_authorization_time(&self) -> Result<SyncTime, StoreFailure> {
        self.transaction().sample_time()
    }

    async fn read_person_authorization(
        &mut self,
        statement: Statement,
    ) -> Result<ReadOutcome, StoreFailure> {
        self.transaction_mut().read_lent(statement).await
    }
}
