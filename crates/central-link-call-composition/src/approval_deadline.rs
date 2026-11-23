use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFailure, ApprovalTransactionFuture,
    OrganizationApprovalTransactions, OrganizationLoanFailure,
    approval_delivery::ApprovalDelivery,
    approval_dispatch::{ApprovalApplyingClock, ApprovalApplyingIds, ApprovalStore},
    approval_transaction::commit_approval,
};
use central_engine_model::{
    DueRow, EngineTime, FailureRecord, ItemFailureClass, MachineKind, RefusalTag,
};
use central_storage::{CorruptReason, StoreFailure};
use collaboration_central_core::AppliedApprovalDeadline;
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalApplyFailure, ApprovalCommitScope, ApprovalRefusal,
    ApprovalReplyAddress, PendingApprovalHolderEffects, PendingApprovalReply,
};
use fleet_contracts::EngineSubject;
use fleet_engine::{
    CheckedAttempt, CheckedDueFailure, CheckedDueUnit, observe_checked_attempt,
    record_checked_attempt,
};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{Clock, IdSource, workspace_decode_error::corrupt};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::{future::Future, pin::Pin, sync::Arc};

enum PendingDeadline {
    Applied {
        reply: PendingApprovalReply<()>,
        effects: PendingApprovalHolderEffects,
    },
    Stale,
    Failed(FailureRecord),
}

#[derive(Clone)]
struct ApprovalDeadline {
    subject: EngineSubject,
    due_at: EngineTime,
}

enum DeadlineFailure {
    Store(StoreFailure),
    Bounded(ItemFailureClass),
}

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
    pub fn apply_deadline(
        self,
        organization: OrganizationId,
        row: DueRow,
    ) -> Pin<Box<dyn Future<Output = Result<AppliedApprovalDeadline, CheckedDueFailure>> + Send>>
    {
        Box::pin(async move {
            let due_at = EngineTime::from_timestamp_millis(row.occurrence_ms).ok_or_else(|| {
                CheckedDueFailure::from_store(corrupt("approval_requests.deadline_ms"))
            })?;
            let deadline = ApprovalDeadline {
                subject: EngineSubject {
                    kind: MachineKind("approval_request"),
                    key: row.key,
                },
                due_at,
            };
            let transactions = self.application.transactions.clone();
            let clock = self.application.clock.clone();
            let ids = self.application.ids.clone();
            let (apply_deadline, apply_clock, apply_ids) =
                (deadline.clone(), clock.clone(), ids.clone());
            let committed = commit_approval(
                transactions.clone(),
                organization.clone(),
                ApprovalAlarmPublication::AtDueBatchEnd,
                move |loan, scope| {
                    run_deadline(loan, scope, apply_deadline, apply_clock, apply_ids)
                },
            )
            .await;
            let outcome = match committed {
                Ok((proof, PendingDeadline::Applied { reply, effects })) => {
                    reply
                        .take_for_recipient(&proof, &ApprovalReplyAddress::Maintenance)
                        .map_err(|_| CheckedDueFailure::AnswerLost)?;
                    let offers = effects
                        .release(&proof)
                        .map_err(|_| CheckedDueFailure::AnswerLost)?;
                    return Ok(AppliedApprovalDeadline {
                        outcome: CheckedDueUnit::Applied { ready: None },
                        completion: self.after_reply(offers, None),
                    });
                }
                Ok((_, PendingDeadline::Stale)) => CheckedDueUnit::Stale,
                Ok((_, PendingDeadline::Failed(failure))) => CheckedDueUnit::Failed(failure),
                Err(failure) => match deadline_failure(failure) {
                    DeadlineFailure::Store(failure) => {
                        return Err(CheckedDueFailure::from_store(failure));
                    }
                    DeadlineFailure::Bounded(class) => {
                        let recorded = transactions
                            .apply_approval(
                                organization,
                                ApprovalAlarmPublication::AtDueBatchEnd,
                                move |loan| charge_deadline(loan, deadline, class, clock, ids),
                            )
                            .await
                            .map_err(|failure| match deadline_failure(failure) {
                                DeadlineFailure::Store(failure) => {
                                    CheckedDueFailure::from_store(failure)
                                }
                                DeadlineFailure::Bounded(_) => CheckedDueFailure::Unavailable,
                            })?;
                        recorded.map_or(CheckedDueUnit::Stale, CheckedDueUnit::Failed)
                    }
                },
            };
            Ok(AppliedApprovalDeadline {
                outcome,
                completion: None,
            })
        })
    }
}

fn run_deadline<'loan, Transaction, Db>(
    transaction: &'loan mut Transaction,
    scope: ApprovalCommitScope,
    deadline: ApprovalDeadline,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
) -> ApprovalTransactionFuture<'loan, PendingDeadline>
where
    Transaction: fleet_approvals::ApprovalStoreTransaction<Database = Db> + 'loan,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    Box::pin(async move {
        let sampled = clock.now();
        let now = EngineTime::from_timestamp_millis(sampled.unix_millis())
            .ok_or_else(|| corrupt("approval_store.applying_time"))?;
        if !ApprovalStore::approval_deadline_is_due(
            transaction,
            &deadline.subject.key,
            deadline.due_at,
            now,
        )
        .await?
        {
            return Ok(PendingDeadline::Stale);
        }
        match observe_checked_attempt(transaction, &deadline.subject, now).await? {
            CheckedAttempt::Deferred => return Ok(PendingDeadline::Stale),
            CheckedAttempt::Ready(_) => {}
            CheckedAttempt::Corrupt(class) => {
                let failure = record_checked_attempt(
                    transaction,
                    &deadline.subject,
                    None,
                    class,
                    now,
                    ids.as_ref(),
                )
                .await?;
                return Ok(failure.map_or(PendingDeadline::Stale, PendingDeadline::Failed));
            }
        }
        let clock = ApprovalApplyingClock(sampled);
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalStore::new(
            &scope,
            transaction,
            &ActorApprovalSessionFacts,
            &clock,
            &mut row_ids,
        );
        let reply = store
            .apply_approval_deadline(&deadline.subject.key, deadline.due_at)
            .await?;
        Ok(PendingDeadline::Applied {
            reply,
            effects: store.finish_holder_effects(),
        })
    })
}

fn charge_deadline<'loan, Transaction, Db>(
    transaction: &'loan mut Transaction,
    deadline: ApprovalDeadline,
    class: ItemFailureClass,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
) -> ApprovalTransactionFuture<'loan, Option<FailureRecord>>
where
    Transaction: fleet_approvals::ApprovalStoreTransaction<Database = Db> + 'loan,
    Db: Database,
    Db::Arguments: IntoArguments<Db>,
    String: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<String>: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    Option<i64>: Type<Db> + for<'row> Decode<'row, Db>,
    i64: Type<Db> + for<'query> Encode<'query, Db> + for<'row> Decode<'row, Db>,
    for<'row> &'row str: ColumnIndex<Db::Row>,
{
    Box::pin(async move {
        let now = EngineTime::from_timestamp_millis(clock.now().unix_millis())
            .ok_or_else(|| corrupt("approval_store.applying_time"))?;
        if !ApprovalStore::approval_deadline_is_due(
            transaction,
            &deadline.subject.key,
            deadline.due_at,
            now,
        )
        .await?
        {
            return Ok(None);
        }
        let previous = match observe_checked_attempt(transaction, &deadline.subject, now).await? {
            CheckedAttempt::Deferred => return Ok(None),
            CheckedAttempt::Ready(previous) => previous,
            CheckedAttempt::Corrupt(_) => None,
        };
        Ok(record_checked_attempt(
            transaction,
            &deadline.subject,
            previous.as_ref(),
            class,
            now,
            ids.as_ref(),
        )
        .await?)
    })
}

fn deadline_failure(failure: ApprovalTransactionFailure) -> DeadlineFailure {
    match failure {
        ApprovalTransactionFailure::Store(failure)
        | ApprovalTransactionFailure::Loan(OrganizationLoanFailure::Store(failure)) => {
            DeadlineFailure::Store(failure)
        }
        ApprovalTransactionFailure::Loan(OrganizationLoanFailure::Wiring(_)) => {
            DeadlineFailure::Store(corrupt("signin_organization_identity.id"))
        }
        ApprovalTransactionFailure::RolledBack(failure) => match failure {
            ApprovalApplyFailure::Store(StoreFailure::Corrupt { reason }) => {
                let column = match reason {
                    CorruptReason::MissingColumn { column }
                    | CorruptReason::UnknownKind { column }
                    | CorruptReason::BadValue { column } => column,
                };
                DeadlineFailure::Bounded(ItemFailureClass::CorruptRow {
                    column,
                    raw_value: None,
                })
            }
            ApprovalApplyFailure::Store(
                failure @ (StoreFailure::Unavailable
                | StoreFailure::AnswerLost
                | StoreFailure::AlarmUnpublishable { .. }),
            ) => DeadlineFailure::Store(failure),
            ApprovalApplyFailure::GuardChanged => {
                DeadlineFailure::Bounded(ItemFailureClass::StatementRejected)
            }
            ApprovalApplyFailure::Lifecycle(
                failure @ (ApprovalRefusal::InvalidRetainedEnding
                | ApprovalRefusal::InvalidDigest
                | ApprovalRefusal::DeadlineOutOfRange
                | ApprovalRefusal::ApplyingTimeOutOfRange
                | ApprovalRefusal::InvalidInitialRoute),
            ) => DeadlineFailure::Bounded(ItemFailureClass::Refused {
                refusal: failure.refusal_tag().into(),
            }),
        },
    }
}
