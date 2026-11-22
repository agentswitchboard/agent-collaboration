//! Admission for the destination of a direct send.
use central_storage::StoreFailure;
use fleet_engine::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, from_row::FromRow, query_as::query_as, row::Row, types::Type,
};
use workspace_ids::MachineId;

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

/// The transient messages/D6 rule accepts an enrolled receiving machine.
/// Central session creation replaces this with a lookup of that session.
pub(super) async fn admit_enrolled_direct_recipient<T: CentralTransaction>(
    transaction: &mut T,
    machine: &MachineId,
) -> Result<bool, StoreFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    &'static str: ColumnIndex<<T::Database as Database>::Row>,
{
    macro_rules! bind_enrolled_recipient_read {
        ($file:literal, $machine:expr) => {
            query_as::<T::Database, StoredEnrolledRecipient>(include_str!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/",
                $file,
            )))
            .bind($machine)
        };
    }
    let row = enrolled_direct_recipient_query!(bind_enrolled_recipient_read, machine)
        .fetch_optional(transaction.executor())
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
