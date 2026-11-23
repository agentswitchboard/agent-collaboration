//! Re-offer unread approvals after the destination machine reconnects.
use std::sync::Arc;

use central_link_protocol::approval_rpc::ApprovalNoticeEnvelope;
use central_storage::StoreFailure;
use collaboration_central_core::{
    LinkCallCompletion, ValidatedMachineConnection, recheck_lent_link_authority,
};
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalCommitScope, ApprovalEnrollment, ApprovalNoticeCursor,
    ApprovalNoticeRecovery, ApprovalSession, ApprovalStoreTransaction, PendingApprovalEffects,
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
    pub(crate) async fn ending_is_owed(
        &self,
        holder: &central_link_protocol::approval_rpc::ApprovalHolderTarget,
        request: &workspace_ids::ApprovalRequestId,
    ) -> Result<bool, StoreFailure> {
        let scope = fleet_approvals::ApprovalHolderScope {
            organization_id: holder.organization_id.clone(),
            run: fleet_approvals::ApprovalRun {
                enrollment: ApprovalEnrollment {
                    machine_id: holder.machine_id.clone(),
                    enrollment_id: holder.enrollment_id.clone(),
                },
                router_run: holder.router_run.clone(),
            },
        };
        let request = request.clone();
        let clock = self.application.clock.clone();
        let ids = self.application.ids.clone();
        let (_, owed) = commit_approval(
            self.application.transactions.clone(),
            scope.organization_id.clone(),
            ApprovalAlarmPublication::AtDueBatchEnd,
            move |loan, commit| {
                Box::pin(async move {
                    let clock = ApprovalApplyingClock(clock.now());
                    let mut ids = ApprovalApplyingIds(ids.as_ref());
                    Ok(ApprovalStore::new(
                        &commit,
                        loan,
                        &ActorApprovalSessionFacts,
                        &clock,
                        &mut ids,
                    )
                    .holder_ending_is_owed(&scope, &request)
                    .await?)
                })
            },
        )
        .await
        .map_err(|failure| match failure {
            crate::ApprovalTransactionFailure::Store(failure)
            | crate::ApprovalTransactionFailure::Loan(crate::OrganizationLoanFailure::Store(
                failure,
            ))
            | crate::ApprovalTransactionFailure::RolledBack(
                fleet_approvals::ApprovalApplyFailure::Store(failure),
            ) => failure,
            crate::ApprovalTransactionFailure::Loan(crate::OrganizationLoanFailure::Wiring(_))
            | crate::ApprovalTransactionFailure::RolledBack(
                fleet_approvals::ApprovalApplyFailure::Lifecycle(_)
                | fleet_approvals::ApprovalApplyFailure::GuardChanged,
            ) => StoreFailure::Unavailable,
        })?;
        Ok(owed)
    }

    pub(crate) async fn notice_deadline(
        &self,
        notice: &ApprovalNoticeEnvelope,
    ) -> Result<Option<central_engine_model::EngineTime>, StoreFailure> {
        let clock = self.application.clock.clone();
        let ids = self.application.ids.clone();
        let notice = notice.clone();
        let (_, deadline) = commit_approval(
            self.application.transactions.clone(),
            notice.target.organization_id.clone(),
            ApprovalAlarmPublication::AtDueBatchEnd,
            move |loan, scope| {
                Box::pin(async move {
                    let time =
                        fleet_approvals::ApprovalApplyingTime::read(loan, clock.now()).await?;
                    let clock = ApprovalApplyingClock(time.now());
                    let mut ids = ApprovalApplyingIds(ids.as_ref());
                    let target = notice.target;
                    let session = ApprovalSession {
                        enrollment: ApprovalEnrollment {
                            machine_id: target.machine_id,
                            enrollment_id: target.enrollment_id,
                        },
                        session_id: target.session_id,
                    };
                    ApprovalStore::new(&scope, loan, &ActorApprovalSessionFacts, &clock, &mut ids)
                        .owed_notice_deadline(
                            &target.organization_id,
                            &session,
                            &notice.body.request_id,
                        )
                        .await
                })
            },
        )
        .await
        .map_err(|failure| match failure {
            crate::ApprovalTransactionFailure::Store(failure)
            | crate::ApprovalTransactionFailure::Loan(crate::OrganizationLoanFailure::Store(
                failure,
            ))
            | crate::ApprovalTransactionFailure::RolledBack(
                fleet_approvals::ApprovalApplyFailure::Store(failure),
            ) => failure,
            crate::ApprovalTransactionFailure::Loan(crate::OrganizationLoanFailure::Wiring(_))
            | crate::ApprovalTransactionFailure::RolledBack(
                fleet_approvals::ApprovalApplyFailure::Lifecycle(_)
                | fleet_approvals::ApprovalApplyFailure::GuardChanged,
            ) => StoreFailure::Unavailable,
        })?;
        Ok(deadline)
    }

    pub(crate) fn recover_notices_after_announcement(
        self,
        machine: ValidatedMachineConnection,
    ) -> LinkCallCompletion {
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
        let time = fleet_approvals::ApprovalApplyingTime::read(loan, sampled).await?;
        let clock = ApprovalApplyingClock(time.now());
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
                time.persist(loan).await?;
                Ok(Some((next, effects)))
            }
        }
    })
}
