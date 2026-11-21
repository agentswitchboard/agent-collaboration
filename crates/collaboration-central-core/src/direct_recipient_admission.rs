//! Admission for the destination of a direct send.
use central_storage::StoreFailure;
use fleet_runtime_ports::CentralStore;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, executor::Executor, from_row::FromRow, query_as::query_as, row::Row,
    types::Type,
};
use workspace_ids::MachineId;

/// The transient rule of messages/D6 accepts an enrolled receiving machine.
/// Once Central creates sessions, replace this with lookup of that session.
pub(crate) async fn admit_direct_recipient(
    host: &impl CentralStore,
    machine: &MachineId,
) -> Result<bool, StoreFailure> {
    host.enrolled_direct_recipient(machine).await
}

struct StoredEnrolledRecipient {
    machine_id: String,
}

impl<'row, R: Row> FromRow<'row, R> for StoredEnrolledRecipient
where
    &'static str: ColumnIndex<R>,
    String: Type<R::Database> + Decode<'row, R::Database>,
{
    fn from_row(row: &'row R) -> Result<Self, sqlx_core::Error> {
        Ok(Self {
            machine_id: row.try_get("machine_id!: String")?,
        })
    }
}

macro_rules! enrolled_direct_recipient_query {
    ($query:ident, $machine:expr) => {
        $query!(
            "src/queries/read_enrolled_direct_recipient.sql",
            $machine.to_string()
        )
    };
}

/// Reads the enrollment on the owning Central store's executor.
pub async fn read_enrolled_direct_recipient<'query, DB: Database>(
    executor: impl Executor<'query, Database = DB>,
    machine: &MachineId,
) -> Result<bool, StoreFailure>
where
    DB::Arguments: IntoArguments<DB>,
    String: Type<DB> + for<'value> Encode<'value, DB> + for<'row> Decode<'row, DB>,
    &'static str: ColumnIndex<DB::Row>,
{
    macro_rules! runtime_query {
        ($file:literal, $machine:expr) => {
            query_as::<DB, StoredEnrolledRecipient>(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/",
                $file,
            )))
            .bind($machine)
        };
    }
    let row = enrolled_direct_recipient_query!(runtime_query, machine)
        .fetch_optional(executor)
        .await
        .map_err(|_| StoreFailure::Unavailable)?;
    match row {
        Some(row) if row.machine_id == machine.as_str() => Ok(true),
        Some(_) => Err(corrupt("login_machines.machine_id")),
        None => Ok(false),
    }
}

#[cfg(test)]
#[path = "direct_recipient_admission_test.rs"]
mod tests;
