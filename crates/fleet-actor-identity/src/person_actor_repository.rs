use central_engine_model::{EngineTime, IdSource};
use central_storage::StoreFailure;
use fleet_lent_transaction::OrganizationTransaction;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};
use fleet_signin::ApprovalPersonAdmission;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query::query, query_as::query_as, types::Type,
};
use workspace_ids::{ActorIdentityId, PersonId};

/// The person-to-actor mapping in the organization that owns this transaction.
pub struct PersonActorRepository<'a, Transaction> {
    transaction: &'a mut Transaction,
}

#[derive(sqlx::FromRow)]
struct StoredPersonActor {
    actor_id: String,
    actor_kind: Option<String>,
}

impl StoredPersonActor {
    fn decode(self) -> Result<ActorIdentityId, StoreFailure> {
        if self.actor_kind.as_deref() != Some("person") {
            return Err(corrupt("actor_identities.actor_kind"));
        }
        ActorIdentityId::try_from(self.actor_id).map_err(|_| corrupt("actor_persons.actor_id"))
    }
}

macro_rules! insert_one {
    ($transaction:expr, $file:literal $(, $value:expr)* $(,)?) => {{
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file!($file $(,$value)*);
        let result = query(include_str!(concat!("../", $file)))$(.bind($value))*
            .execute($transaction.executor())
            .await
            .map_err(store_error)?;
        if Transaction::rows_affected(result) != 1 {
            return Err(StoreFailure::Unavailable);
        }
    }};
}

impl<Transaction> PersonActorRepository<'_, Transaction>
where
    Transaction: OrganizationTransaction,
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'q> Encode<'q, Transaction::Database>
        + for<'r> Decode<'r, Transaction::Database>,
    Option<String>: Type<Transaction::Database> + for<'r> Decode<'r, Transaction::Database>,
    for<'r> &'r str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    pub const fn new(transaction: &mut Transaction) -> PersonActorRepository<'_, Transaction> {
        PersonActorRepository { transaction }
    }

    /// Look up the verified person on their organization's transaction for replay.
    pub async fn lookup(
        &mut self,
        person: &PersonId,
    ) -> Result<Option<ActorIdentityId>, StoreFailure> {
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file_as!(
            StoredPersonActor,
            "src/queries/person_actor_by_person.sql",
            person.as_str()
        );

        query_as::<_, StoredPersonActor>(include_str!("queries/person_actor_by_person.sql"))
            .bind(person.as_str().to_owned())
            .fetch_optional(self.transaction.executor())
            .await
            .map_err(store_error)?
            .map(StoredPersonActor::decode)
            .transpose()
    }

    /// Resolve or mint the actor while applying a first admitted decision.
    /// The applying turn owns rollback if either of the actor inserts fails.
    pub async fn resolve_or_mint(
        &mut self,
        admission: &ApprovalPersonAdmission,
        now: EngineTime,
        ids: &mut dyn IdSource,
    ) -> Result<ActorIdentityId, StoreFailure> {
        if self.transaction.organization_id() != admission.organization() {
            return Err(StoreFailure::Unavailable);
        }
        if let Some(actor) = self.lookup(admission.person()).await? {
            return Ok(actor);
        }

        let actor = ActorIdentityId::try_from(ids.next_id().to_string())
            .map_err(|_| StoreFailure::Unavailable)?;
        let written_at = format_utc_millis(now);
        insert_one!(
            self.transaction,
            "src/queries/insert_person_actor_identity.sql",
            actor.as_str().to_owned(),
            written_at.clone(),
            written_at.clone(),
        );
        insert_one!(
            self.transaction,
            "src/queries/insert_actor_person.sql",
            actor.as_str().to_owned(),
            admission.person().as_str().to_owned(),
            written_at.clone(),
            written_at,
        );

        Ok(actor)
    }
}

fn store_error(error: sqlx_core::error::Error) -> StoreFailure {
    match error {
        sqlx_core::error::Error::ColumnNotFound(column)
        | sqlx_core::error::Error::ColumnDecode { index: column, .. } => corrupt(&column),
        _ => StoreFailure::Unavailable,
    }
}
