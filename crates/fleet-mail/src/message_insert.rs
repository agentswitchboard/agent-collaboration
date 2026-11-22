//! The canonical mailbox insert and its stored delivery terms.
use central_storage::{GuardedStatement, SqlValue, StoreFailure};
use fleet_runtime_ports::{
    guarded_statements::any, stored_values::format_utc_millis, workspace_decode_error::corrupt,
};
use workspace_model::central_mail as mail;

fn encode_system_mail_json(value: &impl serde::Serialize) -> Result<String, StoreFailure> {
    serde_json::to_string(value).map_err(|_| corrupt("mail JSON"))
}

fn system_mail_delivery_window(
    message: &mail::MessageRecord,
) -> Result<Option<String>, StoreFailure> {
    match &message.body {
        mail::MessageBody::Direct { .. } => Err(corrupt("mail_messages.message_kind")),
        mail::MessageBody::Notice { .. } | mail::MessageBody::DeliveryOutcome { .. } => {
            crate::message_deadline::system_message_deadline(message.created_at)
                .map(format_utc_millis)
                .map(Some)
                .ok_or_else(|| corrupt("mail_messages.delivery_window_ends_at"))
        }
        mail::MessageBody::Wake { .. } | mail::MessageBody::Automation { .. } => Ok(None),
    }
}

// Both the guarded batch and the schema witness consume these exact typed binds.
macro_rules! system_mail_insert {
    ($emit:ident, $message:expr) => {{
        let message: &mail::MessageRecord = $message;
        let (state, failure) = match &message.state {
            mail::MessageState::Queued => ("queued", None),
            mail::MessageState::Delivered => ("delivered", None),
            mail::MessageState::Cancelled => ("cancelled", None),
            mail::MessageState::Failed { reason } => ("failed", Some(reason.to_string())),
        };
        let source = message.source.as_ref();
        let delivery_window = system_mail_delivery_window(message)?;
        $emit!(
            "src/queries/insert_system_mail.sql",
            message.message_id.to_string(),
            encode_system_mail_json(&message.sender)?,
            message.target.machine_id.to_string(),
            encode_system_mail_json(&message.target)?,
            stored_kind(&message.body).to_owned(),
            encode_system_mail_json(&message.body)?,
            state.to_owned(),
            failure,
            source.map(|source| source.change_id.to_string()),
            source.map(|source| source.recipient_actor.to_string()),
            source.map(|source| source.workspace_id.to_string()),
            source
                .and_then(|source| source.thread_id.as_ref())
                .map(ToString::to_string),
            format_utc_millis(message.created_at),
            format_utc_millis(message.updated_at),
            None::<String>,
            None::<String>,
            delivery_window,
        )
    }};
}

/// Central-authored notices, outcomes, wakes and automation messages.
pub fn insert_message(message: &mail::MessageRecord) -> Result<GuardedStatement, StoreFailure> {
    fn text_or_null(value: impl Into<Option<String>>) -> SqlValue {
        value.into().map_or(SqlValue::Null, SqlValue::Text)
    }

    macro_rules! guarded_insert {
        ($file:literal, $($arg:expr),+ $(,)?) => {
            any(
                include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
                vec![$(text_or_null($arg)),+],
            )
        };
    }

    Ok(system_mail_insert!(guarded_insert, message))
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

#[cfg(test)]
#[path = "message_insert_test.rs"]
mod tests;
