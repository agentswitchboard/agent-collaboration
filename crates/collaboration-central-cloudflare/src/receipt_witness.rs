//! The Worker link fixture moves the held clock inside the applying transaction.
use crate::{statement_store::WorkerExecutorLender, worker_clock::WorkspaceClock};
use fleet_lent_transaction::CentralTransaction;
use sqlx_durable_object::{Do, DoTransaction};

pub(crate) struct WorkerReceiptTransaction<'transaction, 'clock> {
    transaction: WorkerExecutorLender<'transaction, DoTransaction>,
    clock: &'clock WorkspaceClock,
}

impl<'transaction, 'clock> WorkerReceiptTransaction<'transaction, 'clock> {
    pub(crate) fn new(
        transaction: &'transaction DoTransaction,
        clock: &'clock WorkspaceClock,
    ) -> Self {
        Self {
            transaction: WorkerExecutorLender::from_storage_executor(transaction),
            clock,
        }
    }
}

impl CentralTransaction for WorkerReceiptTransaction<'_, '_> {
    type Database = Do;

    fn executor(&mut self) -> impl sqlx_core::executor::Executor<'_, Database = Self::Database> {
        self.transaction.executor()
    }

    fn rows_affected(
        result: <Self::Database as sqlx_core::database::Database>::QueryResult,
    ) -> u64 {
        WorkerExecutorLender::<DoTransaction>::rows_affected(result)
    }

    fn after_receipt_enrollment_read(&mut self) {
        self.clock.observe_receipt_enrollment_read();
    }
}
