//! Checked direct-message reads on the existing owning executor.
pub(crate) use crate::direct_message_rows::StoredDirectState;
use crate::direct_store_port::{
    DirectOffer, DirectOfferLookup, DirectReadFailure, DirectSendLookup, OfferPageLookup,
    OwnedDirectStatus, RetainedDirectSend, RetainedDirectSendRead,
};
use central_storage::{CorruptReason, StoreFailure};
use fleet_lent_transaction::CentralTransaction;
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query_as::query_as, types::Type,
};
use workspace_ids::MessageId;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailRefusal, CentralMailResult, MessageSender,
};

impl From<DirectReadFailure> for StoreFailure {
    fn from(failure: DirectReadFailure) -> Self {
        match failure {
            DirectReadFailure::Unavailable => Self::Unavailable,
            DirectReadFailure::Corrupt { reason } => Self::Corrupt { reason },
        }
    }
}

impl DirectReadFailure {
    pub fn from_store(error: StoreFailure) -> Self {
        match error {
            StoreFailure::Corrupt { reason } => Self::Corrupt { reason },
            StoreFailure::Unavailable
            | StoreFailure::AnswerLost
            | StoreFailure::AlarmUnpublishable { .. } => Self::Unavailable,
        }
    }
}

fn classify_direct_read_failure(error: sqlx_core::Error) -> DirectReadFailure {
    match error {
        sqlx_core::Error::ColumnDecode { index, .. } => DirectReadFailure::Corrupt {
            reason: CorruptReason::BadValue { column: index },
        },
        _ => DirectReadFailure::Unavailable,
    }
}

pub async fn read_retained_send<T: CentralTransaction>(
    tx: &mut T,
    lookup: DirectSendLookup,
) -> Result<RetainedDirectSendRead, DirectReadFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    &'static str: ColumnIndex<<T::Database as Database>::Row>,
{
    let record = fleet_request_log::read_retained_organization_request(
        tx,
        &lookup.request.request_id,
        lookup.request.cutoff,
    )
    .await
    .map_err(DirectReadFailure::from_store)?;
    let Some(record) = record else {
        return Ok(RetainedDirectSendRead::Missing);
    };
    if lookup.caller.as_ref() != Some(&record.caller)
        || record.content_digest != lookup.request.digest
    {
        return Ok(RetainedDirectSendRead::RequestIdReused);
    }
    let corrupt_answer = || DirectReadFailure::from_store(corrupt("runtime_request_log.answer"));
    let answer = match serde_json::from_str(&record.answer).map_err(|_| corrupt_answer())? {
        CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) => {
            RetainedDirectSend::Queued { message_id }
        }
        CentralMailResult::Refused(CentralMailRefusal::UnknownSession { target }) => {
            RetainedDirectSend::RefusedUnknownTarget { target }
        }
        _ => return Err(corrupt_answer()),
    };
    Ok(RetainedDirectSendRead::Found(answer))
}

#[derive(sqlx::FromRow)]
struct StoredOffer {
    #[sqlx(rename = "message_id!: String")]
    message_id: String,
    sender: String,
    target_machine_id: String,
    target_session: String,
    message_kind: String,
    body: String,
    state: String,
    failure_reason: Option<String>,
    source_change_id: Option<String>,
    recipient_actor: Option<String>,
    source_workspace_id: Option<String>,
    source_thread_id: Option<String>,
    created_at: String,
    updated_at: String,
    delivery_window_ends_at: Option<String>,
    cancel_due_at: Option<String>,
    deadline_settled_at: Option<String>,
}

impl StoredOffer {
    fn decode_offered_message(
        self,
    ) -> Result<fleet_contracts::mail_link::WindowedDelivery, DirectReadFailure> {
        use workspace_model::central_mail::{
            MessageBody, MessageRecord, MessageState, NoticeSource,
        };
        let column = |name: &str| DirectReadFailure::from_store(corrupt(name));
        let cutoff = self
            .delivery_window_ends_at
            .as_ref()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| column("mail_messages.delivery_window_ends_at"))?;
        let body: MessageBody =
            serde_json::from_str(&self.body).map_err(|_| column("mail_messages.body"))?;
        if self.message_kind != crate::message_insert::stored_kind(&body)
            || self.state != "queued"
            || self.failure_reason.is_some()
        {
            return Err(column("mail_messages.state"));
        }
        if self.message_kind == "direct" {
            let created_at = self
                .created_at
                .parse()
                .map_err(|_| column("mail_messages.created_at"))?;
            let deadline = crate::direct_message_rows::decode_direct_deadline(
                created_at,
                self.delivery_window_ends_at.as_deref(),
                self.cancel_due_at.as_deref(),
                self.deadline_settled_at.as_deref(),
            )
            .map_err(DirectReadFailure::from_store)?;
            if deadline.settled_at.is_some() {
                return Err(column("mail_messages.deadline_settled_at"));
            }
        }
        let target: workspace_ids::SessionRef = serde_json::from_str(&self.target_session)
            .map_err(|_| column("mail_messages.target_session"))?;
        if target.machine_id.as_str() != self.target_machine_id {
            return Err(column("mail_messages.target_machine_id"));
        }
        let source = match (
            &body,
            self.source_change_id,
            self.recipient_actor,
            self.source_workspace_id,
            self.source_thread_id,
        ) {
            (
                MessageBody::Notice { items },
                Some(change_id),
                Some(actor),
                Some(workspace),
                thread,
            ) => {
                let source = NoticeSource {
                    change_id: change_id
                        .parse()
                        .map_err(|_| column("mail_messages.source_change_id"))?,
                    recipient_actor: actor
                        .parse()
                        .map_err(|_| column("mail_messages.recipient_actor"))?,
                    workspace_id: workspace
                        .parse()
                        .map_err(|_| column("mail_messages.source_workspace_id"))?,
                    thread_id: thread
                        .map(|id| id.parse())
                        .transpose()
                        .map_err(|_| column("mail_messages.source_thread_id"))?,
                };
                let first = items.first().ok_or_else(|| column("mail_messages.body"))?;
                if first.change_id != source.change_id
                    || first.workspace_id != source.workspace_id
                    || first.thread_id != source.thread_id
                {
                    return Err(column("mail_messages.source_change_id"));
                }
                Some(source)
            }
            (
                MessageBody::Direct { .. } | MessageBody::DeliveryOutcome { .. },
                None,
                None,
                None,
                None,
            ) => None,
            _ => return Err(column("mail_messages.source_change_id")),
        };
        let message = MessageRecord {
            message_id: self
                .message_id
                .parse()
                .map_err(|_| column("mail_messages.message_id"))?,
            sender: serde_json::from_str(&self.sender)
                .map_err(|_| column("mail_messages.sender"))?,
            target,
            body,
            state: MessageState::Queued,
            source,
            created_at: self
                .created_at
                .parse()
                .map_err(|_| column("mail_messages.created_at"))?,
            updated_at: self
                .updated_at
                .parse()
                .map_err(|_| column("mail_messages.updated_at"))?,
        };
        Ok(fleet_contracts::mail_link::WindowedDelivery {
            message,
            expires_at: cutoff,
        })
    }
}

macro_rules! offer_page_query {
    ($emit:ident, $lookup:ident, $limit:expr) => {
        $emit!(
            "src/queries/read_windowed_page.sql",
            $lookup.recipient.to_string(),
            fleet_runtime_ports::stored_values::format_utc_millis($lookup.now),
            $lookup.after.as_ref().map(|cursor| {
                fleet_runtime_ports::stored_values::format_utc_millis(cursor.created_at)
            }),
            $lookup
                .after
                .as_ref()
                .map(|cursor| cursor.message_id.to_string()),
            $limit,
        )
    };
}

macro_rules! status_query {
    ($emit:ident, $message:expr, $sender:expr) => {
        $emit!(
            "src/queries/read_owned_direct_status.sql",
            $message.to_string(),
            $sender
        )
    };
}
macro_rules! offer_query {
    ($emit:ident, $lookup:ident) => {
        $emit!(
            "src/queries/read_direct_offer.sql",
            $lookup.message_id.to_string(),
            $lookup.recipient.to_string(),
            fleet_runtime_ports::stored_values::format_utc_millis($lookup.now)
        )
    };
}
macro_rules! state_query {
    ($emit:ident, $message:expr) => {
        $emit!("src/queries/read_direct_state.sql", $message.to_string())
    };
}
macro_rules! bind_direct_message_read {
    ($file:literal, $($arg:expr),+ $(,)?) => {
        query_as::<T::Database, _>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
    };
}

pub(super) async fn read_owned_direct_state<T: CentralTransaction>(
    tx: &mut T,
    status: &OwnedDirectStatus,
) -> Result<Option<StoredDirectState>, DirectReadFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    Option<String>: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    let sender = serde_json::to_string(&MessageSender::Actor(status.sender.clone()))
        .map_err(|_| DirectReadFailure::from_store(corrupt("mail_messages.sender")))?;
    status_query!(bind_direct_message_read, status.message_id, sender)
        .fetch_optional(tx.executor())
        .await
        .map_err(classify_direct_read_failure)
}

pub async fn read_offer<T: CentralTransaction>(
    tx: &mut T,
    lookup: DirectOfferLookup,
) -> Result<Option<DirectOffer>, DirectReadFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    Option<String>: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    let row: Option<StoredOffer> = offer_query!(bind_direct_message_read, lookup)
        .fetch_optional(tx.executor())
        .await
        .map_err(classify_direct_read_failure)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let offer = row.decode_offered_message()?;
    let sender = crate::direct_store_sender::resolve_offer_sender(tx, &offer.message).await?;
    Ok(Some(DirectOffer {
        message: offer.message,
        sender,
        cutoff: offer.expires_at,
    }))
}

pub async fn read_offer_page<T: CentralTransaction>(
    tx: &mut T,
    lookup: OfferPageLookup,
) -> Result<Vec<fleet_contracts::mail_link::WindowedDelivery>, DirectReadFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    Option<String>: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    i64: Type<T::Database> + for<'value> Encode<'value, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    let limit = i64::try_from(fleet_contracts::mail_link::WELCOME_PAGE_SIZE)
        .map_err(|_| DirectReadFailure::from_store(corrupt("mail page size")))?;
    let rows: Vec<StoredOffer> = offer_page_query!(bind_direct_message_read, lookup, limit)
        .fetch_all(tx.executor())
        .await
        .map_err(classify_direct_read_failure)?;
    rows.into_iter()
        .map(StoredOffer::decode_offered_message)
        .collect()
}

pub(crate) async fn read_stored_direct_message<T: CentralTransaction>(
    tx: &mut T,
    message: &MessageId,
) -> Result<Option<StoredDirectState>, DirectReadFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database>
        + for<'value> Encode<'value, T::Database>
        + for<'row> Decode<'row, T::Database>,
    Option<String>: Type<T::Database> + for<'row> Decode<'row, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
{
    state_query!(bind_direct_message_read, message)
        .fetch_optional(tx.executor())
        .await
        .map_err(classify_direct_read_failure)
}

#[cfg(test)]
#[path = "direct_store_reads_test.rs"]
mod tests;
