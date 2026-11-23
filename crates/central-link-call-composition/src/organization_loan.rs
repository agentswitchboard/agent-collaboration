use crate::{OrganizationLoanFailure, SessionApplyResult, SessionWiringFault};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use fleet_signin::{organization_authority::StoredStampRead, stamp_store::read_stamp};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    executor::Executor, types::Type,
};
use std::{future::Future, pin::Pin};

pub type SessionApplyFuture<'loan> =
    Pin<Box<dyn Future<Output = SessionApplyResult> + Send + 'loan>>;

/// Each host retains its transaction and lends its verified organization scope.
pub trait OrganizationSessionTransactions: Send + Sync + 'static {
    type Transaction<'connection>: OrganizationTransaction + Send;

    fn apply(
        &self,
        admitted_organization: OrganizationId,
        dispatch: impl for<'loan, 'connection> FnOnce(
            &'loan mut Self::Transaction<'connection>,
        ) -> SessionApplyFuture<'loan>
        + Send
        + 'static,
    ) -> impl Future<Output = SessionApplyResult> + Send;
}

/// The organization stamp was read from the same executor that applies this call.
pub struct OrganizationLoan<Transaction> {
    organization: OrganizationId,
    transaction: Transaction,
}

impl<Transaction: CentralTransaction> OrganizationLoan<Transaction> {
    pub async fn verify(
        mut transaction: Transaction,
        admitted_organization: &OrganizationId,
    ) -> Result<Self, OrganizationLoanFailure>
    where
        <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
        String: Type<Transaction::Database> + for<'row> Decode<'row, Transaction::Database>,
        Option<String>: Type<Transaction::Database> + for<'row> Decode<'row, Transaction::Database>,
        for<'row> &'row str: ColumnIndex<<Transaction::Database as Database>::Row>,
    {
        let StoredStampRead::Stamped(stamp) = read_stamp(transaction.executor()).await? else {
            return Err(SessionWiringFault::UnprovisionedStore.into());
        };
        let organization = stamp.organization_id();
        if organization != admitted_organization {
            return Err(SessionWiringFault::ForeignOrganizationStore.into());
        }
        Ok(Self {
            organization: organization.clone(),
            transaction,
        })
    }
}

impl<Transaction: CentralTransaction> CentralTransaction for OrganizationLoan<Transaction> {
    type Database = Transaction::Database;

    fn executor(&mut self) -> impl Executor<'_, Database = Self::Database> {
        self.transaction.executor()
    }

    fn rows_affected(result: <Self::Database as Database>::QueryResult) -> u64 {
        Transaction::rows_affected(result)
    }
}

impl<Transaction: CentralTransaction> OrganizationTransaction for OrganizationLoan<Transaction> {
    fn organization_id(&self) -> &OrganizationId {
        &self.organization
    }
}
