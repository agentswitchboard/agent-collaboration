//! Test-only stand-in for the future person sign-in approval entry.
use crate::{
    ApprovalAlarmPublication, ApprovalTransactionFuture, OrganizationApprovalTransactions,
    approval_delivery::ApprovalDelivery,
    approval_dispatch::{ApprovalApplyingClock, ApprovalApplyingIds, ApprovalStore},
    approval_transaction::commit_approval,
};
use approval_request_protocol::{Answer, SessionCallDecision};
use central_storage::{ReadOutcome, Statement, StoreFailure};
use collaboration_central_core::{
    FixtureApprovalAnswer, FixtureApprovalDecision, LinkCallCompletion,
};
use fleet_approvals::{
    ActorApprovalSessionFacts, AdmittedDecider, ApprovalActor, ApprovalCommitScope,
    ApprovalDecideInput, ApprovalDecisionReply, ApprovalEnrollmentScope,
    ApprovalEnrollmentStanding, ApprovalReplyAddress, ApprovalStoreTransaction, FactorStanding,
    PendingApprovalHolderEffects, PendingApprovalReply,
};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::{CentralTransaction, OrganizationTransaction};
use fleet_runtime_ports::{Clock, IdSource, workspace_decode_error::corrupt};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, executor::Executor, types::Type,
};
use std::{future::Future, pin::Pin, sync::Arc};
use workspace_ids::{ActorIdentityId, PersonId};

type PendingFixtureDecision = (
    PendingApprovalReply<ApprovalDecisionReply>,
    PendingApprovalHolderEffects,
);

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
    pub(crate) fn fixture_decide(
        self,
        organization: OrganizationId,
        decision: FixtureApprovalDecision,
    ) -> Pin<Box<dyn Future<Output = Result<Option<LinkCallCompletion>, StoreFailure>> + Send>>
    {
        Box::pin(async move {
            let address = ApprovalReplyAddress::Person {
                organization: organization.clone(),
                person: decision.person_id.clone(),
            };
            let clock = self.application.clock.clone();
            let ids = self.application.ids.clone();
            let (proof, (reply, effects)) = commit_approval(
                self.application.transactions.clone(),
                organization,
                ApprovalAlarmPublication::WithCommit,
                move |loan, scope| fixture_apply(loan, scope, decision, clock, ids),
            )
            .await
            .map_err(|failure| {
                tracing::warn!(?failure, "fixture approval decision did not commit");
                StoreFailure::Unavailable
            })?;
            if !matches!(
                reply.take_for_recipient(&proof, &address),
                Ok(ApprovalDecisionReply::SavedDecision { .. })
            ) {
                return Err(corrupt("approval_fixture.decision_reply"));
            }
            let offers = effects
                .release(&proof)
                .map_err(|_| StoreFailure::AnswerLost)?;
            Ok(self.after_reply(offers, None))
        })
    }
}

fn fixture_apply<'loan, Transaction, Db>(
    transaction: &'loan mut Transaction,
    scope: ApprovalCommitScope,
    decision: FixtureApprovalDecision,
    clock: Arc<dyn Clock>,
    ids: Arc<dyn IdSource>,
) -> ApprovalTransactionFuture<'loan, PendingFixtureDecision>
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
        let organization = transaction.organization_id().clone();
        let mut fixture = FixtureDecisionLoan {
            transaction,
            person: decision.person_id.clone(),
        };
        let sampled = ApprovalApplyingClock(
            crate::approval_applying_time::observe_applying_time(&mut fixture, clock.now()).await?,
        );
        let mut row_ids = ApprovalApplyingIds(ids.as_ref());
        let mut store = ApprovalStore::new(
            &scope,
            &mut fixture,
            &ActorApprovalSessionFacts,
            &sampled,
            &mut row_ids,
        );
        let reply = store
            .fixture_decide_approval(
                &organization,
                AdmittedDecider::Person {
                    person_id: decision.person_id,
                    factor: FactorStanding::Fresh,
                },
                ApprovalDecideInput {
                    request_id: decision.request_id,
                    operation_id: decision.operation_id,
                    value: match decision.answer {
                        FixtureApprovalAnswer::HeldStart => Answer::UnprovenCall {
                            decision: SessionCallDecision::Allow,
                        },
                        FixtureApprovalAnswer::ProviderApproval => Answer::Approval {
                            option_id: approval_request_protocol::OfferedOptionId::new(1)
                                .map_err(|_| corrupt("approval_fixture.option"))?,
                        },
                    },
                },
            )
            .await?;
        Ok((reply, store.finish_holder_effects()))
    })
}

struct FixtureDecisionLoan<'loan, Transaction> {
    transaction: &'loan mut Transaction,
    person: PersonId,
}

impl<T: ApprovalStoreTransaction> CentralTransaction for FixtureDecisionLoan<'_, T> {
    type Database = T::Database;
    fn executor(&mut self) -> impl Executor<'_, Database = Self::Database> {
        self.transaction.executor()
    }
    fn rows_affected(result: <Self::Database as Database>::QueryResult) -> u64 {
        T::rows_affected(result)
    }
}

impl<T: ApprovalStoreTransaction> OrganizationTransaction for FixtureDecisionLoan<'_, T> {
    fn organization_id(&self) -> &OrganizationId {
        self.transaction.organization_id()
    }
}

impl<T: ApprovalStoreTransaction> ApprovalStoreTransaction for FixtureDecisionLoan<'_, T> {
    async fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<ReadOutcome, StoreFailure> {
        self.transaction.read_approval_statement(statement).await
    }

    async fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> Result<u64, StoreFailure> {
        self.transaction.execute_approval_statement(statement).await
    }

    async fn approval_enrollment_standing(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> Result<ApprovalEnrollmentStanding, StoreFailure> {
        self.transaction.approval_enrollment_standing(scope).await
    }

    async fn approval_decision_caller(
        &mut self,
        organization: &OrganizationId,
        actor: &ApprovalActor,
    ) -> Result<ActorIdentityId, StoreFailure> {
        if organization == self.organization_id()
            && matches!(actor, ApprovalActor::Person(person) if person == &self.person)
        {
            return self
                .person
                .as_str()
                .parse()
                .map_err(|_| corrupt("approval_fixture.person"));
        }
        Err(corrupt("approval_fixture.unadmitted_actor"))
    }
}
