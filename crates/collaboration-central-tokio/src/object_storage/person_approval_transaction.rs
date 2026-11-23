use std::{ops::ControlFlow, sync::Arc};

use central_link_call_composition::OrganizationLoan;
use central_storage::{ReadOutcome, Statement, StoreFailure};
use collaboration_central_core::{
    AppliedPersonApproval, PersonApprovalApplication, PersonApprovalTransactions,
};
use fleet_approvals::{ApprovalCommitScope, ApprovalStatementTransaction};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use futures_util::future::BoxFuture;

use super::{NativeTransaction, ObjectStorage};

pub(crate) struct NativePersonApprovalTransactions {
    pub(crate) storage: Arc<ObjectStorage>,
    pub(crate) ids: Arc<dyn fleet_runtime_ports::IdSource>,
}

impl PersonApprovalTransactions for NativePersonApprovalTransactions {
    fn apply_and_commit<'request>(
        &'request self,
        application: PersonApprovalApplication,
        scope: ApprovalCommitScope,
    ) -> BoxFuture<'request, Result<AppliedPersonApproval, StoreFailure>> {
        Box::pin(async move {
            let _order = self.storage.lock_batch_order().await;
            let result = self
                .storage
                .opened()
                .await?
                .run_transaction(async |connection| {
                    let loan = OrganizationLoan::verify(
                        NativeTransaction(connection),
                        application.organization(),
                    )
                    .await
                    .map_err(|_| StoreFailure::Unavailable)?;
                    Ok(
                        match application
                            .apply(&mut PersonApprovalLoan(loan), &scope, self.ids.as_ref())
                            .await
                        {
                            Ok(reply) => ControlFlow::Continue(reply),
                            Err(failure) => ControlFlow::Break(failure),
                        },
                    )
                })
                .await?;
            match result {
                ControlFlow::Continue(reply) => Ok(reply),
                ControlFlow::Break(failure) => Err(failure),
            }
        })
    }
}

struct PersonApprovalLoan<'connection>(OrganizationLoan<NativeTransaction<'connection>>);

impl CentralTransaction for PersonApprovalLoan<'_> {
    type Database = sqlx::Sqlite;

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Self::Database> {
        self.0.executor()
    }

    fn rows_affected(result: <Self::Database as sqlx::Database>::QueryResult) -> u64 {
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
        fleet_store_sqlx::read_on_transaction(self.executor(), statement).await
    }

    async fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<u64, StoreFailure> {
        fleet_store_sqlx::write_on_transaction(self.executor(), statement).await
    }
}
