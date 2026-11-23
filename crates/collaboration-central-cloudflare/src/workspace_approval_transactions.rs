use crate::{
    statement_store::{self, AlarmPolicy, WorkerExecutorLender},
    worker_clock::WorkspaceClock,
};
use central_link_call_composition::{
    ApprovalAlarmPublication, ApprovalStatementLoan, ApprovalTransactionFailure,
    ApprovalTransactionFuture, OrganizationApprovalTransactions, OrganizationLoan,
};
use central_storage::{EngineAlarmDecider, ReadOutcome, Statement, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{Clock, SyncTime};
use sqlx_core::{database::Database, executor::Executor};
use sqlx_durable_object::{Do, DoConnection, DoTransaction};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use worker::{State, send::SendWrapper};

pub(super) struct WorkerApprovalTransactions {
    pub(super) connection: Arc<SendWrapper<DoConnection>>,
    pub(super) state: Arc<SendWrapper<State>>,
    pub(super) clock: WorkspaceClock,
    pub(super) decide_alarm: EngineAlarmDecider,
    #[cfg(feature = "test-support")]
    pub(super) fail_after_write: Arc<AtomicBool>,
}

pub(super) struct WorkerApprovalLoan<'connection> {
    transaction: &'connection DoTransaction,
    wrote: &'connection AtomicBool,
    clock: &'connection WorkspaceClock,
}

impl CentralTransaction for WorkerApprovalLoan<'_> {
    type Database = Do;

    fn executor(&mut self) -> impl Executor<'_, Database = Do> {
        self.transaction
    }

    fn rows_affected(result: <Do as Database>::QueryResult) -> u64 {
        WorkerExecutorLender::<DoTransaction>::rows_affected(result)
    }
}

impl ApprovalStatementLoan for WorkerApprovalLoan<'_> {
    fn sample_time(&self) -> Result<SyncTime, StoreFailure> {
        Ok(self.clock.now())
    }

    async fn read_lent(&mut self, statement: Statement) -> Result<ReadOutcome, StoreFailure> {
        statement_store::read_lent_approval_statement(self.transaction, statement).await
    }

    async fn execute_lent(&mut self, statement: Statement) -> Result<u64, StoreFailure> {
        let rows =
            statement_store::execute_lent_approval_statement(self.transaction, statement).await?;
        self.wrote.store(true, Ordering::Relaxed);
        Ok(rows)
    }
}

impl OrganizationApprovalTransactions for WorkerApprovalTransactions {
    type Transaction<'connection> = OrganizationLoan<WorkerApprovalLoan<'connection>>;

    async fn apply_approval<Output: Send + 'static>(
        &self,
        organization: OrganizationId,
        alarm: ApprovalAlarmPublication,
        apply: impl for<'loan, 'connection> FnOnce(
            &'loan mut Self::Transaction<'connection>,
        ) -> ApprovalTransactionFuture<'loan, Output>
        + Send
        + 'static,
    ) -> Result<Output, ApprovalTransactionFailure> {
        let policy = AlarmPolicy::Engine(
            self.state.storage(),
            self.clock.clone(),
            self.decide_alarm.clone(),
        );
        #[cfg(feature = "test-support")]
        let fail_after_write = self.fail_after_write.clone();
        let clock = self.clock.clone();
        self.connection
            .transaction(move |transaction| async move {
                let wrote = AtomicBool::new(false);
                let mut loan = OrganizationLoan::verify(
                    WorkerApprovalLoan {
                        transaction: &transaction,
                        wrote: &wrote,
                        clock: &clock,
                    },
                    &organization,
                )
                .await
                .map_err(|failure| WorkerApprovalAbort::Applying(failure.into()))?;
                let result = apply(&mut loan).await.map_err(|failure| {
                    WorkerApprovalAbort::Applying(ApprovalTransactionFailure::RolledBack(failure))
                })?;
                drop(loan);
                let wrote = wrote.into_inner();
                #[cfg(feature = "test-support")]
                if wrote && fail_after_write.swap(false, Ordering::AcqRel) {
                    return Err(WorkerApprovalAbort::Applying(
                        ApprovalTransactionFailure::fixture_rejected_write(),
                    ));
                }
                if wrote && alarm.publishes_with_commit() {
                    statement_store::publish_approval_alarm(&transaction, policy)
                        .await
                        .map_err(|failure| WorkerApprovalAbort::Applying(failure.into()))?;
                }
                Ok(result)
            })
            .await
            .map_err(|failure| match failure {
                WorkerApprovalAbort::Applying(failure) => failure,
                WorkerApprovalAbort::Commit => StoreFailure::AnswerLost.into(),
            })
    }
}

enum WorkerApprovalAbort {
    Applying(ApprovalTransactionFailure),
    Commit,
}

impl From<sqlx_core::Error> for WorkerApprovalAbort {
    fn from(_: sqlx_core::Error) -> Self {
        Self::Commit
    }
}
