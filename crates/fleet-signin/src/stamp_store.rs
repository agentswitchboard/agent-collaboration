//! Local checked statements for the owning Central store.
use crate::{
    organization_authority::{StampMutation, StoredOrganizationStamp, StoredStampRead},
    organization_change::OrganizationChangeRecord,
};
use central_storage::{BatchOutcome, StoreFailure};
use fleet_identity_protocol::OrganizationId;
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use futures_util::future::BoxFuture;
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Type};

/// The host applies a checked stamp and recomputes its alarm on the same transaction.
pub trait StampStore: Send + Sync {
    fn read_stamp(&self) -> BoxFuture<'_, Result<StoredStampRead, StoreFailure>>;
    fn write_stamp(&self, write: StampWrite) -> BoxFuture<'_, Result<BatchOutcome, StoreFailure>>;
}

const READ_STAMP_SQL: &str = include_str!("queries/read_stamp.sql");

#[derive(sqlx::FromRow)]
struct StampRow {
    id: Option<String>,
    organization_statement: String,
}

pub async fn read_stamp<'connection, DB>(
    executor: impl Executor<'connection, Database = DB>,
) -> Result<StoredStampRead, StoreFailure>
where
    DB: Database,
    DB::Arguments: IntoArguments<DB>,
    String: Type<DB> + for<'row> Decode<'row, DB>,
    Option<String>: Type<DB> + for<'row> Decode<'row, DB>,
    for<'row> &'row str: ColumnIndex<DB::Row>,
{
    #[cfg(all(test, not(target_arch = "wasm32")))]
    let _ = sqlx::query_file_as!(StampRow, "src/queries/read_stamp.sql");

    let rows = sqlx::query_as::<DB, StampRow>(READ_STAMP_SQL)
        .fetch_all(executor)
        .await
        .map_err(|error| match error {
            sqlx::Error::ColumnDecode { index, .. } if index.contains("organization_statement") => {
                corrupt("signin_organization_identity.organization_statement")
            }
            sqlx::Error::ColumnDecode { .. } => corrupt("signin_organization_identity.id"),
            _ => StoreFailure::Unavailable,
        })?;
    let row = match rows.as_slice() {
        [] => return Ok(StoredStampRead::Unprovisioned),
        [row] => row,
        _ => return Err(corrupt("signin_organization_identity.id")),
    };
    let organization: OrganizationId = row
        .id
        .clone()
        .ok_or_else(|| corrupt("signin_organization_identity.id"))?
        .try_into()
        .map_err(|_| corrupt("signin_organization_identity.id"))?;
    if row.organization_statement.is_empty() {
        return Err(corrupt(
            "signin_organization_identity.organization_statement",
        ));
    }
    Ok(StoredStampRead::Stamped(StoredOrganizationStamp::new(
        organization,
        row.organization_statement.clone(),
    )))
}

pub struct StampWrite {
    pub(crate) mutation: StampMutation,
    pub(crate) now_ms: i64,
    pub(crate) issued_seconds: i64,
    pub(crate) expires_seconds: i64,
    pub(crate) audit: OrganizationChangeRecord,
}

#[derive(Debug, thiserror::Error)]
pub enum StampWriteStop {
    #[error("stamp statement {index} affected {rows_affected} rows, expected one")]
    Guard { index: u32, rows_affected: u64 },
    #[error("stamp statement {index} failed: {source}")]
    Sql {
        index: u32,
        #[source]
        source: sqlx::Error,
    },
    #[error(transparent)]
    Store(#[from] StoreFailure),
}

struct StampTiming {
    now_ms: i64,
    now_seconds: i64,
    issued: i64,
    expires: i64,
}
fn timing(write: &StampWrite) -> Result<StampTiming, StoreFailure> {
    let exact = |value| {
        central_storage::SafeInteger::new(value)
            .map(central_storage::SafeInteger::get)
            .map_err(|_| corrupt("signin_organization_identity.updated_at"))
    };
    Ok(StampTiming {
        now_ms: exact(write.now_ms)?,
        now_seconds: exact(write.now_ms.div_euclid(1000))?,
        issued: exact(write.issued_seconds)?,
        expires: exact(write.expires_seconds)?,
    })
}

fn exact_one(index: u32, rows_affected: u64) -> Result<(), StampWriteStop> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(StampWriteStop::Guard {
            index,
            rows_affected,
        })
    }
}

impl StampWrite {
    pub async fn apply<T: CentralTransaction>(self, tx: &mut T) -> Result<[u64; 2], StampWriteStop>
    where
        <T::Database as Database>::Arguments: IntoArguments<T::Database>,
        String: Type<T::Database> + for<'q> Encode<'q, T::Database>,
        Option<String>: Type<T::Database> + for<'q> Encode<'q, T::Database>,
        i64: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    {
        let time = timing(&self)?;
        let body = self.mutation.accepted().statement().to_owned();
        let audit = self.audit.insert_values()?;
        let audit_timestamp = audit.timestamp.get();
        let result = match self.mutation {
            StampMutation::Initialize(accepted) => {
                let id = accepted.organization_id().as_str().to_owned();
                #[cfg(all(test, not(target_arch = "wasm32")))]
                let _ = sqlx::query_file!(
                    "src/queries/initialize_stamp.sql",
                    &id,
                    &body,
                    time.now_ms,
                    time.now_ms,
                    time.now_seconds,
                    time.issued,
                    time.now_seconds,
                    time.expires
                );
                sqlx::query::<T::Database>(include_str!("queries/initialize_stamp.sql"))
                    .bind(id)
                    .bind(body)
                    .bind(time.now_ms)
                    .bind(time.now_ms)
                    .bind(time.now_seconds)
                    .bind(time.issued)
                    .bind(time.now_seconds)
                    .bind(time.expires)
                    .execute(tx.executor())
                    .await
            }
            StampMutation::Replace {
                predecessor: prior, ..
            } => {
                let stored_org = prior.organization_id().as_str().to_owned();
                let prior_body = prior.statement().to_owned();
                #[cfg(all(test, not(target_arch = "wasm32")))]
                let _ = sqlx::query_file!(
                    "src/queries/replace_stamp.sql",
                    &body,
                    time.now_ms,
                    &stored_org,
                    &prior_body,
                    time.now_seconds,
                    time.issued,
                    time.now_seconds,
                    time.expires
                );
                sqlx::query::<T::Database>(include_str!("queries/replace_stamp.sql"))
                    .bind(body)
                    .bind(time.now_ms)
                    .bind(stored_org)
                    .bind(prior_body)
                    .bind(time.now_seconds)
                    .bind(time.issued)
                    .bind(time.now_seconds)
                    .bind(time.expires)
                    .execute(tx.executor())
                    .await
            }
        }
        .map_err(|source| StampWriteStop::Sql { index: 0, source })?;
        let affected = T::rows_affected(result);
        exact_one(0, affected)?;

        #[cfg(all(test, not(target_arch = "wasm32")))]
        let _ = sqlx::query_file!(
            "src/queries/insert_change_record.sql",
            &audit.id,
            &audit.request,
            &audit.kind,
            &audit.origin,
            &audit.person,
            &audit.permission,
            &audit.target,
            audit_timestamp,
            audit_timestamp
        );
        let result = sqlx::query::<T::Database>(include_str!("queries/insert_change_record.sql"))
            .bind(audit.id)
            .bind(audit.request.to_owned())
            .bind(audit.kind.to_owned())
            .bind(audit.origin.to_owned())
            .bind(audit.person.map(str::to_owned))
            .bind(audit.permission.map(str::to_owned))
            .bind(audit.target.to_owned())
            .bind(audit_timestamp)
            .bind(audit_timestamp)
            .execute(tx.executor())
            .await
            .map_err(|source| StampWriteStop::Sql { index: 1, source })?;
        let recorded = T::rows_affected(result);
        exact_one(1, recorded)?;
        Ok([affected, recorded])
    }
}
