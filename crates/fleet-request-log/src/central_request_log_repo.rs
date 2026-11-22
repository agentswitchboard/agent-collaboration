//! The checked organization request log, on the caller's owning executor.
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, from_row::FromRow, query::query, query_as::query_as, row::Row, types::Type,
};
use workspace_model::{ActorIdentityId, RequestId, Timestamp};

use central_storage::{GuardedStatement, RowExpectation, SqlValue, Statement, StoreFailure};
use fleet_engine::CentralTransaction;
use fleet_runtime::workspace_request_log::{RetainRequest, RetainedRequest};
struct StoredRequestRow {
    caller: String,
    content_digest: String,
    answer: String,
}

impl<'row, R: Row> FromRow<'row, R> for StoredRequestRow
where
    &'static str: ColumnIndex<R>,
    String: Type<R::Database> + Decode<'row, R::Database>,
{
    fn from_row(row: &'row R) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            caller: row.try_get("caller")?,
            content_digest: row.try_get("content_digest")?,
            answer: row.try_get("answer")?,
        })
    }
}

macro_rules! read_binds {
    ($emit:ident, $key:expr, $cutoff:expr) => {
        $emit!(
            "src/queries/read_central_request.sql",
            $key.to_string(),
            $cutoff.to_string()
        )
    };
}
macro_rules! retain_binds {
    ($emit:ident, $request:expr, $caller:expr, $digest:expr, $answer:expr, $timestamp:expr) => {
        $emit!(
            "src/queries/retain_central_request.sql",
            $request.to_string(),
            $caller.to_string(),
            $digest.to_owned(),
            $answer,
            $timestamp.to_owned(),
            $timestamp.to_owned(),
        )
    };
}
macro_rules! prune_binds {
    ($emit:ident, $cutoff:expr) => {
        $emit!("src/queries/prune_central_requests.sql", $cutoff.to_owned())
    };
}
macro_rules! plain {
    ($file:literal, $($arg:expr),+ $(,)?) => {
        query::<T::Database>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
    };
}

pub async fn read_retained_organization_request<T: CentralTransaction>(
    tx: &mut T,
    request: &RequestId,
    cutoff: Timestamp,
) -> Result<Option<RetainedRequest>, central_storage::StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    &'static str: ColumnIndex<<T::Database as Database>::Row>,
{
    macro_rules! typed {
        ($file:literal, $($arg:expr),+) => {
            query_as::<T::Database, StoredRequestRow>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
        };
    }
    let row = read_binds!(typed, request, cutoff.to_rfc3339())
        .fetch_optional(tx.executor())
        .await
        .map_err(|_| central_storage::StoreFailure::Unavailable)?;
    row.map(|row| {
        Ok(RetainedRequest {
            caller: row.caller.parse().map_err(|_| {
                fleet_runtime_ports::workspace_decode_error::corrupt("runtime_request_log.caller")
            })?,
            content_digest: row.content_digest,
            answer: row.answer,
        })
    })
    .transpose()
}

pub async fn upsert_organization_request_answer<T: CentralTransaction>(
    tx: &mut T,
    entry: RetainRequest<'_>,
) -> Result<u64, sqlx_core::Error>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'value> Encode<'value, T::Database>,
{
    let timestamp = entry.at.to_rfc3339();
    retain_binds!(
        plain,
        entry.request_id,
        entry.caller,
        entry.digest,
        entry.answer,
        timestamp
    )
    .execute(tx.executor())
    .await
    .map(T::rows_affected)
}

pub async fn prune_expired_organization_requests<T: CentralTransaction>(
    tx: &mut T,
    cutoff: Timestamp,
) -> Result<u64, sqlx_core::Error>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'value> Encode<'value, T::Database>,
{
    prune_binds!(plain, cutoff.to_rfc3339())
        .execute(tx.executor())
        .await
        .map(T::rows_affected)
}

// These existing Core callers still compose a guarded statement batch.
// Keep their storage policy on the same checked files as the typed mail leaf.
pub fn lookup_organization_request(request: &RequestId, cutoff: &str) -> Statement {
    macro_rules! legacy {
         ($file:literal, $($arg:expr),+) => {
             Statement { sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)), args: vec![$(SqlValue::Text($arg)),+] }
         };
     }
    read_binds!(legacy, request, cutoff)
}

pub fn retain_organization_request(
    request: &RequestId,
    caller: &ActorIdentityId,
    digest: &str,
    answer: &impl serde::Serialize,
    now: &str,
    cutoff: &str,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let answer = serde_json::to_string(answer).map_err(|_| {
        fleet_runtime_ports::workspace_decode_error::corrupt("runtime_request_log.answer")
    })?;
    macro_rules! legacy {
         ($file:literal, $($arg:expr),+ $(,)?) => {
             Statement { sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)), args: vec![$(SqlValue::Text($arg)),+] }
         };
     }
    Ok(vec![
        GuardedStatement {
            statement: retain_binds!(legacy, request, caller, digest, answer, now),
            expect: RowExpectation::Exactly(1),
        },
        GuardedStatement {
            statement: prune_binds!(legacy, cutoff),
            expect: RowExpectation::Any,
        },
    ])
}

#[cfg(test)]
#[path = "central_request_log_repo_test.rs"]
mod tests;
