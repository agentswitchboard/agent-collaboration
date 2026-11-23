//! Read the sender's owned message after applying any deadline it has reached.
use crate::{
    AppliedDirectStatus, OwnedDirectStatus,
    direct_message::{DirectLifecycle, DirectMessageEvent},
    direct_store_reads::read_owned_direct_state,
    direct_store_writes::{
        AppliedTransition, DirectApplyFailure, DirectWriteOutcome, apply_direct_transition,
    },
};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, types::Type,
};
use workspace_model::{Timestamp, central_mail::MessageStatus};

pub async fn settle_and_read_status<Transaction: CentralTransaction>(
    transaction: &mut Transaction,
    command: OwnedDirectStatus,
) -> Result<DirectWriteOutcome<AppliedDirectStatus>, DirectApplyFailure>
where
    <Transaction::Database as Database>::Arguments: IntoArguments<Transaction::Database>,
    i64: Type<Transaction::Database> + for<'value> Encode<'value, Transaction::Database>,
    String: Type<Transaction::Database>
        + for<'value> Encode<'value, Transaction::Database>
        + for<'row> Decode<'row, Transaction::Database>,
    Option<String>: Type<Transaction::Database>
        + for<'value> Encode<'value, Transaction::Database>
        + for<'row> Decode<'row, Transaction::Database>,
    for<'row> &'row str: ColumnIndex<<Transaction::Database as Database>::Row>,
{
    let Some(stored) = read_owned_direct_state(transaction, &command).await? else {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            AppliedDirectStatus {
                status: MessageStatus::NotFound {},
                ready: None,
            },
        ));
    };
    let row = stored.decode_direct_message()?;
    let now = Timestamp::from_timestamp_millis(command.clock.now().unix_millis())
        .ok_or_else(|| corrupt("clock"))?;
    let status = row.lifecycle.project_message_status(now >= row.cutoff);
    if row.deadline_settled_at.is_some()
        || now < row.mark
        || matches!(row.lifecycle, DirectLifecycle::Final { .. })
    {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            AppliedDirectStatus {
                status,
                ready: None,
            },
        ));
    }

    let transition = apply_direct_transition(
        transaction,
        &stored,
        None,
        DirectMessageEvent::Due {
            occurrence: row.mark,
        },
        now,
        command.ids.as_ref(),
    )
    .await?;
    match transition {
        AppliedTransition::Changed(ready) => Ok(DirectWriteOutcome::with_alarm_recompute(
            AppliedDirectStatus { status, ready },
        )),
        AppliedTransition::Missing | AppliedTransition::Covered | AppliedTransition::Settled => {
            Err(DirectApplyFailure::Conflict)
        }
    }
}
