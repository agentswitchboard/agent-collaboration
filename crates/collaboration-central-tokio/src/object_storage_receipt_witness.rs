//! The native link fixture's one-shot clock move at the applying enrollment read.
use super::{NativeTransaction, ObjectStorage};
use fleet_lent_transaction::CentralTransaction;
use std::sync::{Mutex, PoisonError};

#[derive(Default)]
pub(super) struct ReceiptEnrollmentReadWitness {
    move_clock: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ObjectStorage {
    /// Arm the clock move for the next receipt's applying enrollment read.
    pub fn move_clock_after_receipt_enrollment_read(
        &self,
        move_clock: impl FnOnce() + Send + 'static,
    ) {
        *self
            .receipt_enrollment_read
            .move_clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Box::new(move_clock));
    }
}

pub(super) struct NativeReceiptTransaction<'connection, 'witness> {
    transaction: NativeTransaction<'connection>,
    witness: &'witness ReceiptEnrollmentReadWitness,
}

impl<'connection, 'witness> NativeReceiptTransaction<'connection, 'witness> {
    pub(super) fn new(
        connection: &'connection mut sqlx::SqliteConnection,
        witness: &'witness ReceiptEnrollmentReadWitness,
    ) -> Self {
        Self {
            transaction: NativeTransaction(connection),
            witness,
        }
    }
}

impl CentralTransaction for NativeReceiptTransaction<'_, '_> {
    type Database = sqlx::Sqlite;

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = Self::Database> {
        self.transaction.executor()
    }

    fn rows_affected(result: <Self::Database as sqlx::Database>::QueryResult) -> u64 {
        NativeTransaction::rows_affected(result)
    }

    fn after_receipt_enrollment_read(&mut self) {
        let move_clock = self
            .witness
            .move_clock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(move_clock) = move_clock {
            move_clock();
        }
    }
}
