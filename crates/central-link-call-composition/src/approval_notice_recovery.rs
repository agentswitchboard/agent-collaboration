//! Re-offer unread approvals after the destination machine reconnects.
use std::sync::Arc;

use collaboration_central_core::{
    LinkCallCompletion, ValidatedMachineConnection, recheck_lent_link_authority,
};
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalCommitScope, ApprovalNoticeCursor, ApprovalNoticeRecovery,
    ApprovalStoreTransaction, PendingApprovalEffects,
};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{Clock, IdSource};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};

use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFuture, OrganizationApprovalTransactions,
    approval_delivery::ApprovalDelivery,
    approval_dispatch::{ApprovalApplyingClock, ApprovalApplyingIds, ApprovalStore},
    approval_transaction::commit_approval,
};

impl<Transactions, Db> ApprovalDelivery<Transactions>
where
    Transactions: OrganizationApprovalTransactions,
    for<'connection> Transactions::Transaction<'connection>: CentralTransaction<Database = Db>,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    pub(crate) fn on_welcome(self, machine: ValidatedMachineConnection) -> LinkCallCompletion {
        Box::pin(async move {
            let mut cursor = None;
            loop {
                let organization = machine.organization_id().clone();
                let connection = machine.clone();
                let clock = self.application.clock.clone();
                let ids = self.application.ids.clone();
                let outcome = commit_approval(
                    self.application.transactions.clone(),
                    organization,
                    ApprovalAlarmPublication::WithCommit,
                    move |loan, scope| recover_page(loan, scope, connection, cursor, clock, ids),
                )
                .await;
                let (proof, pending) = match outcome {
                    Ok((_, None)) => return,
                    Ok((proof, Some(pending))) => (proof, pending),
                    Err(failure) => {
                        tracing::warn!(?failure, "approval notice recovery failed");
                        return;
                    }
                };
                let (next, effects) = pending;
                match effects.release(&proof) {
                    Ok(effects) => self.offer_all(effects).await,
                    Err(error) => {
                        tracing::warn!(?error, "approval notice recovery lost its commit proof");
                        return;
                    }
                }
                cursor = next;
                if cursor.is_none() {
                    return;
                }
            }
        })
    }
}

type PendingNoticePage = Option<(Option<ApprovalNoticeCursor>, PendingApprovalEffects)>;

fn recover_page<'loan, Transaction, Db>(
    loan: &'loan mut Transaction,
    scope: ApprovalCommitScope,
    machine: ValidatedMachineConnection,
    cursor: Option<ApprovalNoticeCursor>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
) -> ApprovalTransactionFuture<'loan, PendingNoticePage>
where
    Transaction: ApprovalStoreTransaction<Database = Db> + 'loan,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    Box::pin(async move {
        let (sampled, lapse) =
            recheck_lent_link_authority(loan, machine.attachment_ref(), clock.as_ref()).await?;
        if lapse.is_some() {
            return Ok(None);
        }
        let time = crate::approval_applying_time::read_applying_time(loan, sampled).await?;
        let clock = ApprovalApplyingClock(time.effective());
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalStore::new(
            &scope,
            loan,
            &ActorApprovalSessionFacts,
            &clock,
            &mut row_ids,
        );
        match store
            .recover_machine_notices(&machine, cursor.as_ref())
            .await?
        {
            ApprovalNoticeRecovery::NoneOwed => Ok(None),
            ApprovalNoticeRecovery::Page { next } => {
                let effects = store.finish_effects();
                time.publish(loan).await?;
                Ok(Some((next, effects)))
            }
        }
    })
}
