use std::future::Future;

use central_engine_model::{EngineTime, IdSource};
use central_storage::{ReadOutcome, Statement, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::OrganizationTransaction;
use fleet_signin::ApprovalPersonAdmission;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use workspace_ids::{ActorIdentityId, PersonId};

use crate::{ApprovalActor, approval_store_scope::ApprovalEnrollmentScope};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalEnrollmentStanding {
    Live,
    Ended,
    Unavailable,
}

/// The host lends the already applying transaction and retains commit and rollback.
/// Its enrollment lookup delegates to the machine-login owner in that transaction.
pub trait ApprovalStoreTransaction: OrganizationTransaction + Send {
    fn read_approval_statement(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<ReadOutcome, StoreFailure>> + Send;

    fn execute_approval_statement(
        &mut self,
        statement: Statement,
    ) -> impl Future<Output = Result<u64, StoreFailure>> + Send;

    fn approval_enrollment_standing(
        &mut self,
        scope: &ApprovalEnrollmentScope,
    ) -> impl Future<Output = Result<ApprovalEnrollmentStanding, StoreFailure>> + Send;

    /// Resolve the already admitted destination through Central's actor identity owner.
    fn approval_decision_caller(
        &mut self,
        organization: &OrganizationId,
        actor: &ApprovalActor,
    ) -> impl Future<Output = Result<ActorIdentityId, StoreFailure>> + Send;
}

/// A C3 applying transaction can reach the actor owner's existing person mapping.
pub trait ApprovalPersonStoreTransaction: ApprovalStoreTransaction {
    fn find_person_actor(
        &mut self,
        person: &PersonId,
    ) -> impl Future<Output = Result<Option<ActorIdentityId>, StoreFailure>> + Send;

    fn resolve_person_actor(
        &mut self,
        admission: &ApprovalPersonAdmission,
        now: EngineTime,
        ids: &mut dyn IdSource,
    ) -> impl Future<Output = Result<ActorIdentityId, StoreFailure>> + Send;
}

impl<Transaction> ApprovalPersonStoreTransaction for Transaction
where
    Transaction: ApprovalStoreTransaction + Send,
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'query> Encode<'query, Transaction::Database>
        + for<'row> Decode<'row, Transaction::Database>,
    Option<String>: Type<Transaction::Database> + for<'row> Decode<'row, Transaction::Database>,
    for<'row> &'row str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    async fn find_person_actor(
        &mut self,
        person: &PersonId,
    ) -> Result<Option<ActorIdentityId>, StoreFailure> {
        fleet_actor_identity::PersonActorRepository::new(self)
            .lookup(person)
            .await
    }

    async fn resolve_person_actor(
        &mut self,
        admission: &ApprovalPersonAdmission,
        now: EngineTime,
        ids: &mut dyn IdSource,
    ) -> Result<ActorIdentityId, StoreFailure> {
        fleet_actor_identity::PersonActorRepository::new(self)
            .resolve_or_mint(admission, now, ids)
            .await
    }
}
