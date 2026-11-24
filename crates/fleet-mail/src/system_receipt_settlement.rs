//! Checked reads and writes for the existing system-mail receipt entry.
use crate::{
    direct_message_rows::enum_text,
    direct_store_port::ApplyReceipt,
    mail_write_failure::{DirectApplyFailure, require_one_affected_row},
    message_deadline::EXPIRED_24_HOURS,
};
use central_storage::StoreFailure;
use fleet_engine::{DueItemKey, DueKind, DueTableChange, apply_due_table_change};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query::query, query_as::query_as, types::Type,
};
use workspace_ids::{MachineId, MessageId};
use workspace_model::{
    Timestamp,
    central_mail::{MessageRefusal, ReceiptKind, ReceiptResult},
};

#[derive(sqlx::FromRow)]
struct StoredSystemState {
    state: String,
    failure_reason: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) enum SystemReceiptKind {
    Notice,
    DeliveryOutcome,
    Wake,
    Automation,
}

impl SystemReceiptKind {
    pub(super) fn decode_storage_tag(tag: &str) -> Result<Self, StoreFailure> {
        match tag {
            "notice" => Ok(Self::Notice),
            "deliveryOutcome" => Ok(Self::DeliveryOutcome),
            "wake" => Ok(Self::Wake),
            "automation" => Ok(Self::Automation),
            _ => Err(corrupt("mail_messages.message_kind")),
        }
    }

    fn storage_tag(self) -> &'static str {
        match self {
            Self::Notice => "notice",
            Self::DeliveryOutcome => "deliveryOutcome",
            Self::Wake => "wake",
            Self::Automation => "automation",
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SystemSettlement {
    Queued,
    Delivered,
    Refused(MessageRefusal),
    Cancelled,
}

impl StoredSystemState {
    fn decode_settlement(self, kind: SystemReceiptKind) -> Result<SystemSettlement, StoreFailure> {
        match (self.state.as_str(), self.failure_reason) {
            ("queued", None) => Ok(SystemSettlement::Queued),
            ("delivered", None) => Ok(SystemSettlement::Delivered),
            ("failed", Some(reason)) => serde_json::from_value(serde_json::Value::String(reason))
                .map(SystemSettlement::Refused)
                .map_err(|_| corrupt("mail_messages.failure_reason")),
            ("cancelled", None) => Ok(SystemSettlement::Cancelled),
            ("cancelled", Some(reason))
                if matches!(
                    kind,
                    SystemReceiptKind::Notice | SystemReceiptKind::DeliveryOutcome
                ) && reason == EXPIRED_24_HOURS =>
            {
                Ok(SystemSettlement::Cancelled)
            }
            _ => Err(corrupt("mail_messages.state")),
        }
    }
}

macro_rules! system_state_query {
    ($emit:ident,$message:expr,$target:expr,$kind:expr) => {
        $emit!(
            "src/queries/read_system_receipt_state.sql",
            $message.to_string(),
            $target.to_string(),
            $kind.to_owned()
        )
    };
}
macro_rules! system_update_query {
    ($emit:ident,$message:expr,$target:expr,$kind:expr,$state:expr,$failure:expr,$now:expr) => {
        $emit!(
            "src/queries/apply_system_receipt.sql",
            $state.to_owned(),
            $failure,
            format_utc_millis($now),
            $message.to_string(),
            $target.to_string(),
            $kind.to_owned()
        )
    };
}
pub(super) async fn settle_system_receipt<T: CentralTransaction>(
    transaction: &mut T,
    message: &MessageId,
    target: &MachineId,
    kind: SystemReceiptKind,
    receipt: ReceiptKind,
    now: Timestamp,
) -> Result<ApplyReceipt, DirectApplyFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let (incoming, next_tag, refusal) = match receipt {
        ReceiptKind::Delivered { .. } => (SystemSettlement::Delivered, "delivered", None),
        ReceiptKind::Refused { reason } => (
            SystemSettlement::Refused(reason),
            "failed",
            Some(enum_text(reason)?),
        ),
        ReceiptKind::Seen { .. } => {
            return Ok(ApplyReceipt::Stop(ReceiptResult::NotApplicable));
        }
    };
    macro_rules! bind_system_settlement_read {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            query_as::<T::Database, StoredSystemState>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
        };
    }
    let stored: Option<StoredSystemState> = system_state_query!(
        bind_system_settlement_read,
        message,
        target,
        kind.storage_tag()
    )
    .fetch_optional(transaction.executor())
    .await?;
    let Some(stored) = stored else {
        return Ok(ApplyReceipt::Stop(ReceiptResult::NoSuchMessage));
    };
    let settlement = stored.decode_settlement(kind)?;
    if settlement != SystemSettlement::Queued {
        return Ok(ApplyReceipt::Stop(if settlement == incoming {
            ReceiptResult::Recorded
        } else {
            ReceiptResult::AlreadySettled
        }));
    }

    macro_rules! bind_system_settlement_write {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            query::<T::Database>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
        };
    }
    let written = system_update_query!(
        bind_system_settlement_write,
        message,
        target,
        kind.storage_tag(),
        next_tag,
        refusal,
        now
    )
    .execute(transaction.executor())
    .await?;
    require_one_affected_row::<T>(written)?;
    apply_due_table_change(
        transaction,
        &DueTableChange::RemoveItem(DueItemKey {
            kind: DueKind::SystemMessageExpiry,
            subject: central_engine_model::InstanceKey(message.to_string()),
        }),
        now,
    )
    .await?;
    // System writers continue to rely on the post-event completion for publication.
    Ok(ApplyReceipt::Applied {
        ready: None,
        retired_recipient: None,
    })
}

#[cfg(test)]
#[path = "system_receipt_settlement_test.rs"]
mod tests;
