//! The canonical mailbox insert and its stored delivery terms.
use central_storage::{GuardedStatement, SqlValue, StoreFailure};
use fleet_runtime_ports::stored_values::{encode_json, format_utc_millis};
use fleet_runtime_ports::{
    guarded_statements::{any, text},
    workspace_decode_error::corrupt,
};
use workspace_model::central_mail as mail;

/// Central-authored notices, outcomes, wakes and automation messages.
pub fn insert_message(message: &mail::MessageRecord) -> Result<GuardedStatement, StoreFailure> {
    let kind = stored_kind(&message.body);
    let (state, failure) = match &message.state {
        mail::MessageState::Queued => ("queued", SqlValue::Null),
        mail::MessageState::Delivered => ("delivered", SqlValue::Null),
        mail::MessageState::Cancelled => ("cancelled", SqlValue::Null),
        mail::MessageState::Failed { reason } => ("failed", text(reason)),
    };
    let source = message.source.as_ref();
    let delivery_window = match &message.body {
        mail::MessageBody::Direct { .. } => return Err(corrupt("mail_messages.message_kind")),
        mail::MessageBody::Notice { .. } | mail::MessageBody::DeliveryOutcome { .. } => Some(
            crate::message_deadline::system_message_deadline(message.created_at)
                .ok_or_else(|| corrupt("mail_messages.delivery_window_ends_at"))?,
        ),
        mail::MessageBody::Wake { .. } | mail::MessageBody::Automation { .. } => None,
    };
    Ok(any(
        "INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,failure_reason,source_change_id,recipient_actor,source_workspace_id,source_thread_id,created_at,updated_at,cancel_due_at,sender_request_id,delivery_window_ends_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_change_id,recipient_actor) WHERE source_change_id IS NOT NULL DO NOTHING",
        vec![
            text(&message.message_id),
            encode_json(&message.sender, "mail JSON")?,
            text(&message.target.machine_id),
            encode_json(&message.target, "mail JSON")?,
            text(kind),
            encode_json(&message.body, "mail JSON")?,
            text(state),
            failure,
            source.map_or(SqlValue::Null, |source| text(&source.change_id)),
            source.map_or(SqlValue::Null, |source| text(&source.recipient_actor)),
            source.map_or(SqlValue::Null, |source| text(&source.workspace_id)),
            source
                .and_then(|source| source.thread_id.as_ref())
                .map_or(SqlValue::Null, text),
            text(format_utc_millis(message.created_at)),
            text(format_utc_millis(message.updated_at)),
            SqlValue::Null,
            SqlValue::Null,
            delivery_window.map_or(SqlValue::Null, |due| text(format_utc_millis(due))),
        ],
    ))
}

/// The `mail_messages.message_kind` text for a body; stored rows must match it.
pub(crate) fn stored_kind(body: &mail::MessageBody) -> &'static str {
    match body {
        mail::MessageBody::Direct { .. } => "direct",
        mail::MessageBody::Notice { .. } => "notice",
        mail::MessageBody::Wake { .. } => "wake",
        mail::MessageBody::Automation { .. } => "automation",
        mail::MessageBody::DeliveryOutcome { .. } => "deliveryOutcome",
    }
}
