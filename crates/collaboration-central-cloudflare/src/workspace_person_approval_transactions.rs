use std::sync::Arc;

use central_link_call_composition::OrganizationLoan;
use central_storage::{ReadOutcome, Statement, StoreFailure};
use collaboration_central_core::{
    AppliedPersonApproval, PersonApprovalApplication, PersonApprovalTransactions,
};
use fleet_approvals::{ApprovalCommitScope, ApprovalStatementTransaction};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use futures_util::future::BoxFuture;
use sqlx_durable_object::{Do, DoConnection, DoTransaction};
use worker::send::SendWrapper;

use crate::{
    statement_store::{self, WorkerExecutorLender},
    store_wire::OwnedStatement,
};

pub(super) struct WorkerPersonApprovalTransactions {
    pub(super) connection: Arc<SendWrapper<DoConnection>>,
    pub(super) ids: Arc<dyn fleet_runtime_ports::IdSource>,
}

impl PersonApprovalTransactions for WorkerPersonApprovalTransactions {
    fn apply_and_commit<'request>(
        &'request self,
        application: PersonApprovalApplication,
        scope: ApprovalCommitScope,
    ) -> BoxFuture<'request, Result<AppliedPersonApproval, StoreFailure>> {
        Box::pin(async move {
            let ids = self.ids.clone();
            self.connection
                .transaction(move |transaction| async move {
                    let loan = OrganizationLoan::verify(
                        WorkerExecutorLender::from_storage_executor(&transaction),
                        application.organization(),
                    )
                    .await
                    .map_err(|_| TransactionFailure::Apply(StoreFailure::Unavailable))?;
                    application
                        .apply(&mut PersonApprovalLoan(loan), &scope, ids.as_ref())
                        .await
                        .map_err(TransactionFailure::Apply)
                })
                .await
                .map_err(|failure| match failure {
                    TransactionFailure::Apply(failure) => failure,
                    TransactionFailure::Commit => StoreFailure::AnswerLost,
                })
        })
    }
}

enum TransactionFailure {
    Apply(StoreFailure),
    Commit,
}

impl From<sqlx_core::Error> for TransactionFailure {
    fn from(_: sqlx_core::Error) -> Self {
        Self::Commit
    }
}

struct PersonApprovalLoan<'connection>(
    OrganizationLoan<WorkerExecutorLender<'connection, DoTransaction>>,
);

impl CentralTransaction for PersonApprovalLoan<'_> {
    type Database = Do;

    fn executor(&mut self) -> impl sqlx_core::executor::Executor<'_, Database = Do> {
        self.0.executor()
    }

    fn rows_affected(result: <Do as sqlx_core::database::Database>::QueryResult) -> u64 {
        result.rows_affected()
    }
}

impl OrganizationTransaction for PersonApprovalLoan<'_> {
    fn organization_id(&self) -> &OrganizationId {
        self.0.organization_id()
    }
}

impl ApprovalStatementTransaction for PersonApprovalLoan<'_> {
    async fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<ReadOutcome, StoreFailure> {
        let rows = statement_store::bound(&OwnedStatement::from(statement))
            .fetch_all(self.executor())
            .await
            .map_err(|_| StoreFailure::Unavailable)?;
        statement_store::read_outcome(&rows)
    }

    async fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<u64, StoreFailure> {
        statement_store::bound(&OwnedStatement::from(statement))
            .execute(self.executor())
            .await
            .map(|result| result.rows_affected())
            .map_err(|_| StoreFailure::Unavailable)
    }
}
