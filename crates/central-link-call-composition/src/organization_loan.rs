use collaboration_central_core::CentralCallDispatch;
use fleet_actor_identity::OrganizationTransaction;
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use sqlx_core::{database::Database, executor::Executor};
use std::{future::Future, pin::Pin};

pub type SessionApplyFuture<'loan> =
    Pin<Box<dyn Future<Output = CentralCallDispatch> + Send + 'loan>>;

/// Each host retains its transaction while the dispatcher borrows its executor.
pub trait OrganizationSessionTransactions: Send + Sync + 'static {
    type Transaction<'loan>: CentralTransaction + Send;

    fn apply(
        &self,
        dispatch: impl for<'loan> FnOnce(
            OrganizationLoan<'loan, Self::Transaction<'loan>>,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> impl Future<Output = CentralCallDispatch> + Send;
}

/// Associates an already-open transaction with its storage owner's organization.
pub struct OrganizationLoan<'loan, Transaction> {
    organization: &'loan OrganizationId,
    transaction: Transaction,
}

impl<'loan, Transaction> OrganizationLoan<'loan, Transaction> {
    pub fn new(organization: &'loan OrganizationId, transaction: Transaction) -> Self {
        Self {
            organization,
            transaction,
        }
    }
}

impl<Transaction: CentralTransaction> CentralTransaction for OrganizationLoan<'_, Transaction> {
    type Database = Transaction::Database;

    fn executor(&mut self) -> impl Executor<'_, Database = Self::Database> {
        self.transaction.executor()
    }

    fn rows_affected(result: <Self::Database as Database>::QueryResult) -> u64 {
        Transaction::rows_affected(result)
    }
}

impl<Transaction: CentralTransaction> OrganizationTransaction
    for OrganizationLoan<'_, Transaction>
{
    fn organization_id(&self) -> &OrganizationId {
        self.organization
    }
}
