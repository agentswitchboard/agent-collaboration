use crate::{OrganizationLoan, OrganizationLoanFailure};
use central_storage::{ReadOutcome, Statement, StoreFailure};
use fleet_approvals::{
    ApprovalActor, ApprovalApplyFailure, ApprovalCommitOwner, ApprovalCommitProof,
    ApprovalCommitScope, ApprovalEnrollmentScope, ApprovalEnrollmentStanding,
    ApprovalStoreTransaction,
};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use fleet_machine_login::{SessionEnrollmentStatus, read_session_enrollment};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{convert::Infallible, future::Future, pin::Pin};
use workspace_ids::ActorIdentityId;

#[derive(Clone, Copy, Debug)]
pub enum ApprovalAlarmPublication {
    WithCommit,
    AtDueBatchEnd,
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
    fn read_lent(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<ReadOutcome, StoreFailure>> + Send;

    fn execute_lent(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<u64, StoreFailure>> + Send;
}

/// A committed apply can still fail to publish the native platform alarm.
pub struct CommittedApprovalApply<Output> {
    pub output: Output,
    pub publication_failure: Option<StoreFailure>,
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
    ) -> impl Future<Output = Result<CommittedApprovalApply<Output>, ApprovalTransactionFailure>> + Send;
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
    Box::pin(async move {
        let (proof, committed) =
            commit_approval_with_publication(transactions, organization, alarm, apply).await?;
        match committed.publication_failure {
            None => Ok((proof, committed.output)),
            Some(failure) => Err(failure.into()),
        }
    })
}

pub(crate) fn commit_approval_with_publication<Owner, Output>(
    transactions: std::sync::Arc<Owner>,
    organization: OrganizationId,
    alarm: ApprovalAlarmPublication,
    apply: impl for<'loan, 'connection> FnOnce(
        &'loan mut Owner::Transaction<'connection>,
        ApprovalCommitScope,
    ) -> ApprovalTransactionFuture<'loan, Output>
    + Send
    + 'static,
) -> ApprovalCommitFuture<(ApprovalCommitProof, CommittedApprovalApply<Output>)>
where
    Owner: OrganizationApprovalTransactions,
    Output: Send + 'static,
{
    let owner = ApprovalCommit(
        move |scope| -> ApprovalCommitFuture<CommittedApprovalApply<Output>> {
            Box::pin(async move {
                transactions
                    .apply_approval(organization, alarm, move |loan| apply(loan, scope))
                    .await
            })
        },
    );
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
        if self.organization_id() != organization {
            return Err(corrupt("approval_link.decision_organization"));
        }
        match actor {
            ApprovalActor::Session(session) => session
                .session_id
                .as_str()
                .parse()
                .map_err(|_| corrupt("approval_link.decision_session")),
            ApprovalActor::Person(_) => PersonApprovalAnswersNotAdmitted
                .resolve()
                .map(|never| match never {}),
        }
    }
}

/// The link has no person approval admission.
struct PersonApprovalAnswersNotAdmitted;

impl PersonApprovalAnswersNotAdmitted {
    fn resolve(&self) -> Result<Infallible, StoreFailure> {
        Err(corrupt("approval_link.person_not_admitted"))
    }
}

#[cfg(feature = "test-support")]
impl ApprovalTransactionFailure {
    pub fn fixture_rejected_write() -> Self {
        Self::RolledBack(ApprovalApplyFailure::GuardChanged)
    }
}
