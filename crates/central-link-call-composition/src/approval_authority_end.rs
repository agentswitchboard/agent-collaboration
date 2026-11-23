//! Settlement owed by the session or enrollment owner in its applying transaction.
use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFailure, ApprovalTransactionFuture,
    OrganizationApprovalTransactions, OrganizationLoanFailure,
    approval_delivery::ApprovalDelivery,
    approval_dispatch::{
        ApprovalApplyingClock, ApprovalApplyingIds, ApprovalDispatchFailure, ApprovalStore,
        report_approval_failure,
    },
    approval_transaction::commit_approval,
};
use central_engine_model::{Context, EngineTime};
use central_link_protocol::{
    CentralCallReply, CentralLinkRpc, LinkCallRefusal, MachineLinkCloseCode, TypedCallResult,
    session_lifecycle_rpc::EndFleetSessionRpc,
};
use central_storage::StoreFailure;
use collaboration_central_core::{
    CentralCallWork, TargetCheckedCentralCall, ValidatedMachineConnection,
    recheck_lent_link_authority,
};
use fleet_actor_identity::{SessionCallOutcome, SessionService, SessionServiceError};
use fleet_approvals::{
    ActorApprovalSessionFacts, ApprovalApplyFailure, ApprovalCommitScope, ApprovalEnrollment,
    ApprovalEnrollmentScope, ApprovalReplyAddress, ApprovalSession, ApprovalStoreTransaction,
    PendingApprovalEffects,
};
use fleet_identity_protocol::{EndFleetSession, OrganizationId};
use fleet_lent_transaction::CentralTransaction;
use fleet_rpc::{CorrelatedRpc, Envelope};
use fleet_runtime_ports::{Clock, IdSource, workspace_decode_error::corrupt};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use std::sync::Arc;

enum PendingSessionEnd {
    Refused(LinkCallRefusal),
    AuthorityLapsed(MachineLinkCloseCode),
    Answer {
        reply: CentralCallReply,
        effects: PendingApprovalEffects,
    },
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
    pub(crate) async fn end_session(
        &self,
        machine: &ValidatedMachineConnection,
        request: Envelope<OrganizationId, EndFleetSession>,
    ) -> Result<CentralCallWork, StoreFailure> {
        let organization = machine.organization_id().clone();
        let request =
            match TargetCheckedCentralCall::<EndFleetSessionRpc>::check(&organization, request) {
                Ok(request) => request,
                Err(reason) => return Ok(TypedCallResult::Refused { reason }.into()),
            };
        let machine = machine.clone();
        let clock = self.application.clock.clone();
        let ids = self.application.ids.clone();
        let result = commit_approval(
            self.application.transactions.clone(),
            organization,
            ApprovalAlarmPublication::WithCommit,
            move |loan, scope| apply_session_end(loan, scope, machine, request, clock, ids),
        )
        .await;
        match result {
            Ok((_, PendingSessionEnd::Refused(reason))) => {
                Ok(TypedCallResult::Refused { reason }.into())
            }
            Ok((_, PendingSessionEnd::AuthorityLapsed(cause))) => Ok(TypedCallResult::Refused {
                reason: LinkCallRefusal::AuthorityLapsed { cause },
            }
            .into()),
            Ok((proof, PendingSessionEnd::Answer { reply, effects })) => {
                let offers = effects
                    .release(&proof)
                    .map_err(|_| StoreFailure::AnswerLost)?;
                Ok(CentralCallWork {
                    result: TypedCallResult::Answered { reply },
                    completion: self.after_reply(offers, None),
                })
            }
            Err(failure) => report_approval_failure(ApprovalDispatchFailure::Transaction(failure)),
        }
    }
}

fn apply_session_end<'loan, Transaction, Db>(
    loan: &'loan mut Transaction,
    scope: ApprovalCommitScope,
    machine: ValidatedMachineConnection,
    request: TargetCheckedCentralCall<EndFleetSessionRpc>,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
) -> ApprovalTransactionFuture<'loan, PendingSessionEnd>
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
        if let Some(cause) = lapse {
            return Ok(PendingSessionEnd::AuthorityLapsed(cause));
        }
        let organization = machine.organization_id().clone();
        let body = request.into_body();
        let correlation = EndFleetSessionRpc::reply_context(&body);
        let now = EngineTime::from_timestamp_millis(sampled.unix_millis())
            .ok_or_else(|| corrupt("actor_sessions.applying_at"))?;
        let mut session_ids = ApprovalApplyingIds(ids.as_ref());
        let mut context = Context {
            now,
            ids: &mut session_ids,
        };
        let end = SessionService::in_transaction(loan)
            .end_with_fact(&machine, body, &mut context)
            .await
            .map_err(|failure| match failure {
                SessionServiceError::Store(failure) => failure,
                SessionServiceError::SecretUnavailable
                | SessionServiceError::TimeOutOfRange
                | SessionServiceError::InvalidSessionId
                | SessionServiceError::InvalidCreateContents
                | SessionServiceError::Machine(_) => corrupt("actor_sessions.end"),
            })?;
        let SessionCallOutcome::Answered(end) = end else {
            return Ok(PendingSessionEnd::Refused(LinkCallRefusal::CentralFault));
        };
        let checked = EndFleetSessionRpc::check_reply(correlation, end.reply)
            .map_err(|_| corrupt("actor_sessions.end_reply"))?;
        let clock = ApprovalApplyingClock(
            crate::approval_applying_time::observe_applying_time(loan, sampled).await?,
        );
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalStore::new(
            &scope,
            loan,
            &ActorApprovalSessionFacts,
            &clock,
            &mut row_ids,
        );
        if let Some(session_id) = end.ended_session {
            let session = ApprovalSession {
                enrollment: ApprovalEnrollment {
                    machine_id: machine.machine_id().clone(),
                    enrollment_id: machine.enrollment_id().clone(),
                },
                session_id,
            };
            store.end_approval_session(&organization, &session).await?;
        }
        Ok(PendingSessionEnd::Answer {
            reply: EndFleetSessionRpc::reply(Envelope {
                target: organization,
                body: EndFleetSessionRpc::into_wire(checked),
            }),
            effects: store.finish_effects(),
        })
    })
}

impl<Transactions, Db> crate::approval_dispatch::ApprovalLinkApplication<Transactions>
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
    pub(crate) async fn end_enrollment(
        &self,
        organization: OrganizationId,
        invalidation: fleet_contracts::EnrollmentInvalidation,
        statements: Vec<central_storage::GuardedStatement>,
    ) -> Result<(), StoreFailure> {
        let enrollment = ApprovalEnrollmentScope {
            organization_id: organization.clone(),
            enrollment: ApprovalEnrollment {
                machine_id: invalidation.machine_id,
                enrollment_id: invalidation.enrollment_id,
            },
        };
        let clock = self.clock.clone();
        let ids = self.ids.clone();
        let (proof, (reply, effects)) = commit_approval(
            self.transactions.clone(),
            organization,
            ApprovalAlarmPublication::WithCommit,
            move |loan, scope| {
                Box::pin(async move {
                    for guarded in statements {
                        let rows = loan.execute_approval_statement(guarded.statement).await?;
                        if !guarded.expect.admits(rows) {
                            return Err(ApprovalApplyFailure::GuardChanged);
                        }
                    }
                    let sampled = ApprovalApplyingClock(
                        crate::approval_applying_time::observe_applying_time(loan, clock.now())
                            .await?,
                    );
                    let mut row_ids = ApprovalApplyingIds(ids.as_ref());
                    let mut store = ApprovalStore::new(
                        &scope,
                        loan,
                        &ActorApprovalSessionFacts,
                        &sampled,
                        &mut row_ids,
                    );
                    let reply = store.end_approval_enrollment(&enrollment).await?;
                    Ok((reply, store.finish_effects()))
                })
            },
        )
        .await
        .map_err(settlement_store_failure)?;
        reply
            .take_for_recipient(&proof, &ApprovalReplyAddress::Maintenance)
            .map_err(|_| StoreFailure::AnswerLost)?;
        let offers = effects
            .release(&proof)
            .map_err(|_| StoreFailure::AnswerLost)?;
        if !offers.is_empty() {
            return Err(corrupt("approval_requests.ended_enrollment_offer"));
        }
        Ok(())
    }
}

pub(crate) fn settlement_store_failure(failure: ApprovalTransactionFailure) -> StoreFailure {
    match failure {
        ApprovalTransactionFailure::Store(failure)
        | ApprovalTransactionFailure::Loan(OrganizationLoanFailure::Store(failure))
        | ApprovalTransactionFailure::RolledBack(ApprovalApplyFailure::Store(failure)) => failure,
        ApprovalTransactionFailure::Loan(OrganizationLoanFailure::Wiring(_)) => {
            corrupt("signin_organization_identity.id")
        }
        ApprovalTransactionFailure::RolledBack(ApprovalApplyFailure::GuardChanged) => {
            StoreFailure::Unavailable
        }
        ApprovalTransactionFailure::RolledBack(ApprovalApplyFailure::Lifecycle(_)) => {
            corrupt("approval_requests.authority_end")
        }
    }
}
