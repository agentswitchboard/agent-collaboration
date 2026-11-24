use super::{
    NativeTransaction, ObjectStorage, session_transaction_owner::NativeSessionTransactions,
};
use central_link_call_composition::{
    ApprovalAlarmPublication, ApprovalStatementLoan, ApprovalTransactionFailure,
    ApprovalTransactionFuture, CommittedApprovalApply, OrganizationApprovalTransactions,
    OrganizationLoan,
};
use central_storage::{AlarmPublication, BatchOutcome, ReadOutcome, Statement, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{Clock, SyncTime};
use fleet_store_sqlx::{execute_lent_statement, read_lent_statement};
use std::{
    ops::ControlFlow,
    sync::atomic::{AtomicBool, Ordering},
};

pub(crate) struct NativeApprovalLoan<'connection> {
    transaction: NativeTransaction<'connection>,
    wrote: &'connection AtomicBool,
    clock: Option<&'connection dyn Clock>,
}

impl CentralTransaction for NativeApprovalLoan<'_> {
    type Database = sqlx::Sqlite;

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Self::Database> {
        self.transaction.executor()
    }

    fn rows_affected(result: <Self::Database as sqlx::Database>::QueryResult) -> u64 {
        NativeTransaction::rows_affected(result)
    }
}

impl ApprovalStatementLoan for NativeApprovalLoan<'_> {
    fn sample_time(&self) -> Result<SyncTime, StoreFailure> {
        self.clock.map(Clock::now).ok_or(StoreFailure::Unavailable)
    }

    async fn read_lent(&mut self, statement: Statement) -> Result<ReadOutcome, StoreFailure> {
        read_lent_statement(self.transaction.0, statement).await
    }

    async fn execute_lent(&mut self, statement: Statement) -> Result<u64, StoreFailure> {
        let rows = execute_lent_statement(self.transaction.0, statement).await?;
        self.wrote.store(true, Ordering::Relaxed);
        Ok(rows)
    }
}

#[cfg(feature = "test-support")]
impl ObjectStorage {
    pub fn hold_before_approval_commit(&self) -> fleet_runtime::HeldPause {
        self.central_publication_pauses
            .hold_central(fleet_runtime::PausePoint::BeforeApprovalCommit)
    }

    pub fn fail_next_approval_after_write(&self) {
        self.fail_approval_after_write
            .store(true, Ordering::Release);
    }
}

impl OrganizationApprovalTransactions for NativeSessionTransactions {
    type Transaction<'connection> = OrganizationLoan<NativeApprovalLoan<'connection>>;

    async fn apply_approval<Output: Send + 'static>(
        &self,
        organization: OrganizationId,
        alarm: ApprovalAlarmPublication,
        apply: impl for<'loan, 'connection> FnOnce(
            &'loan mut Self::Transaction<'connection>,
        ) -> ApprovalTransactionFuture<'loan, Output>
        + Send
        + 'static,
    ) -> Result<CommittedApprovalApply<Output>, ApprovalTransactionFailure> {
        let _order = self.storage.lock_batch_order().await;
        let result = self
            .storage
            .opened()
            .await?
            .run_transaction(async |connection| {
                let wrote = AtomicBool::new(false);
                let loan = OrganizationLoan::verify(
                    NativeApprovalLoan {
                        transaction: NativeTransaction(connection),
                        wrote: &wrote,
                        clock: self
                            .storage
                            .engine_publication
                            .as_ref()
                            .map(|publication| publication.clock.as_ref()),
                    },
                    &organization,
                )
                .await;
                let mut loan = match loan {
                    Ok(loan) => loan,
                    Err(failure) => {
                        return Ok(ControlFlow::Break(ApprovalTransactionFailure::Loan(
                            failure,
                        )));
                    }
                };
                let result = apply(&mut loan).await;
                drop(loan);
                let output = match result {
                    Ok(output) => output,
                    Err(failure) => {
                        return Ok(ControlFlow::Break(ApprovalTransactionFailure::RolledBack(
                            failure,
                        )));
                    }
                };
                let wrote = wrote.into_inner();
                #[cfg(feature = "test-support")]
                if wrote {
                    self.storage
                        .central_publication_pauses
                        .reached_central(fleet_runtime::PausePoint::BeforeApprovalCommit)
                        .await;
                }
                #[cfg(feature = "test-support")]
                if wrote
                    && self
                        .storage
                        .fail_approval_after_write
                        .swap(false, Ordering::AcqRel)
                {
                    return Ok(ControlFlow::Break(
                        ApprovalTransactionFailure::fixture_rejected_write(),
                    ));
                }
                let inputs = if wrote && alarm.publishes_with_commit() {
                    Some(ObjectStorage::read_engine_inputs(connection).await?)
                } else {
                    None
                };
                Ok(ControlFlow::Continue((output, inputs)))
            })
            .await?;
        let (output, inputs) = match result {
            ControlFlow::Continue(applied) => applied,
            ControlFlow::Break(failure) => return Err(failure),
        };
        let publication_failure = inputs.and_then(|inputs| {
            let publication = self.storage.publish_engine_alarm(
                inputs.earliest_on_grid_due_unix_millis,
                inputs.earliest_legacy_due_unix_millis,
            );
            self.storage.record_publication_status(
                true,
                &Ok(BatchOutcome::Applied {
                    rows_affected: vec![],
                    publication: publication.clone(),
                }),
            );
            if let AlarmPublication::Failed { reason } = publication {
                #[cfg(feature = "test-support")]
                self.storage
                    .publication_failures
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(reason);
                Some(reason)
            } else {
                None
            }
        });
        Ok(CommittedApprovalApply {
            output,
            publication_failure,
        })
    }
}
