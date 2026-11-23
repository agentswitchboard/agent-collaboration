//! C3 borrows the existing approval transaction and delivers committed endings through its owner.

use central_storage::StoreFailure;
use collaboration_central_core::{
    AppliedPersonApproval, LinkCallCompletion, PersonApprovalApplication,
    PersonApprovalTransactions,
};
use fleet_approvals::{ApprovalApplyFailure, ApprovalCommitScope, CommittedApprovalOffer};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use futures_util::future::BoxFuture;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use super::OrganizationLinkDispatcher;
use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFailure, OrganizationApprovalTransactions,
    OrganizationLoanFailure, OrganizationSessionTransactions, approval_delivery::ApprovalDelivery,
};

impl<Transactions, Approvals, Db> PersonApprovalTransactions
    for OrganizationLinkDispatcher<Transactions, Approvals>
where
    Transactions: OrganizationSessionTransactions,
    Approvals: OrganizationApprovalTransactions,
    for<'connection> Approvals::Transaction<'connection>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    fn apply_and_commit<'request>(
        &'request self,
        application: PersonApprovalApplication,
        scope: ApprovalCommitScope,
    ) -> BoxFuture<'request, Result<AppliedPersonApproval, StoreFailure>> {
        let ids = self.inputs.ids.clone();
        let organization = application.organization().clone();

        Box::pin(async move {
            self.approvals
                .transactions
                .apply_approval(
                    organization,
                    ApprovalAlarmPublication::WithCommit,
                    move |loan| {
                        Box::pin(async move {
                            application
                                .apply(loan, &scope, ids.as_ref())
                                .await
                                .map_err(ApprovalApplyFailure::Store)
                        })
                    },
                )
                .await
                .map_err(person_approval_transaction_failure)
        })
    }

    fn delivery_after_commit(
        &self,
        offers: Vec<CommittedApprovalOffer>,
    ) -> Option<LinkCallCompletion> {
        ApprovalDelivery {
            application: self.approvals.clone(),
            transport: self.ending_transport.clone(),
        }
        .after_reply(offers, None)
    }
}

fn person_approval_transaction_failure(failure: ApprovalTransactionFailure) -> StoreFailure {
    match failure {
        ApprovalTransactionFailure::Store(failure)
        | ApprovalTransactionFailure::Loan(OrganizationLoanFailure::Store(failure))
        | ApprovalTransactionFailure::RolledBack(ApprovalApplyFailure::Store(failure)) => failure,
        ApprovalTransactionFailure::Loan(OrganizationLoanFailure::Wiring(_)) => {
            corrupt("signin_organization_identity.id")
        }
        ApprovalTransactionFailure::RolledBack(
            ApprovalApplyFailure::Lifecycle(_) | ApprovalApplyFailure::GuardChanged,
        ) => StoreFailure::Unavailable,
    }
}
