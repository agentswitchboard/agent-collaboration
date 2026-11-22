//! Plan sourced notices for their applying batches.
use crate::message_insert::insert_message;
use central_storage::{GuardedStatement, StoreFailure};
use fleet_contracts::MailDeliveryReady;
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    Timestamp,
    central_mail::{
        MessageBody, MessageRecord, MessageSender, MessageState, NoticeItem, NoticeSource,
    },
};

fn prepare_message_record(
    sender: MessageSender,
    target: SessionRef,
    body: MessageBody,
    source: Option<NoticeSource>,
    now: Timestamp,
    mint: impl FnOnce() -> Result<MessageId, StoreFailure>,
) -> Result<MessageRecord, StoreFailure> {
    Ok(MessageRecord {
        message_id: mint()?,
        sender,
        target,
        body,
        state: MessageState::Queued,
        source,
        created_at: now,
        updated_at: now,
    })
}

/// Plan a sourced notice and a readiness hint for the same record.
///
/// Offer the hint only after the batch applies. On a source conflict the
/// INSERT leaves the existing row in place, and the hint names the new ID
/// whose row was never inserted. Delivery rechecks that ID.
pub fn plan_sourced_notice(
    target: SessionRef,
    items: Vec<NoticeItem>,
    source: NoticeSource,
    now: Timestamp,
    mint: impl FnOnce() -> Result<MessageId, StoreFailure>,
) -> Result<(GuardedStatement, MailDeliveryReady), StoreFailure> {
    let message = prepare_message_record(
        MessageSender::Central,
        target,
        MessageBody::Notice { items },
        Some(source),
        now,
        mint,
    )?;
    let insert = insert_message(&message)?;
    Ok((insert, (&message).into()))
}

#[cfg(test)]
#[path = "request_plan_test.rs"]
mod tests;
