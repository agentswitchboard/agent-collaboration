//! Validate persisted receipt evidence as one direct lifecycle.
use crate::direct_message::{DirectLifecycle, FinalReceipt};
use central_storage::StoreFailure;
use fleet_runtime_ports::{stored_values::format_utc_millis, workspace_decode_error::corrupt};
use workspace_ids::{MachineId, MessageId, RequestId};
use workspace_model::{Timestamp, central_mail as mail};

pub(crate) struct DirectMessageRow {
    pub id: MessageId,
    pub sender: workspace_model::ActorIdentityId,
    pub sender_request_id: RequestId,
    pub recipient: MachineId,
    pub cutoff: Timestamp,
    pub mark: Timestamp,
    pub lifecycle: DirectLifecycle,
    pub deadline_settled_at: Option<Timestamp>,
}

#[derive(Eq, PartialEq, sqlx::FromRow)]
pub(crate) struct StoredDirectState {
    #[sqlx(rename = "message_id!: String")]
    pub(crate) message_id: String,
    pub(crate) sender: String,
    pub(crate) message_kind: String,
    pub(crate) state: String,
    pub(crate) failure_reason: Option<String>,
    pub(crate) cancel_due_at: Option<String>,
    pub(crate) sender_request_id: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) target_machine_id: String,
    pub(crate) delivery_window_ends_at: Option<String>,
    pub(crate) delivered_arrived_at: Option<String>,
    pub(crate) seen_completed_at: Option<String>,
    pub(crate) deadline_settled_at: Option<String>,
}

fn parse<T: std::str::FromStr>(value: &str, field: &str) -> Result<T, StoreFailure> {
    value.parse().map_err(|_| corrupt(field))
}
fn required<T: std::str::FromStr>(value: Option<&str>, field: &str) -> Result<T, StoreFailure> {
    parse(value.ok_or_else(|| corrupt(field))?, field)
}
fn enum_value<T: serde::de::DeserializeOwned>(value: &str, field: &str) -> Result<T, StoreFailure> {
    serde_json::from_value(serde_json::Value::String(value.to_owned())).map_err(|_| corrupt(field))
}

pub(super) struct DirectDeadlineEvidence {
    pub(super) cutoff: Timestamp,
    pub(super) mark: Timestamp,
    pub(super) settled_at: Option<Timestamp>,
}

pub(super) fn decode_direct_deadline(
    created_at: Timestamp,
    stored_cutoff: Option<&str>,
    stored_mark: Option<&str>,
    stored_settlement: Option<&str>,
) -> Result<DirectDeadlineEvidence, StoreFailure> {
    let cutoff_column = "mail_messages.delivery_window_ends_at";
    let mark_column = "mail_messages.cancel_due_at";
    let cutoff: Timestamp = required(stored_cutoff, cutoff_column)?;
    if cutoff < created_at || stored_cutoff != Some(format_utc_millis(cutoff).as_str()) {
        return Err(corrupt(cutoff_column));
    }
    let mark: Timestamp = required(stored_mark, mark_column)?;
    if crate::message_deadline::direct_deadline_occurrence(cutoff) != Some(mark)
        || stored_mark != Some(format_utc_millis(mark).as_str())
    {
        return Err(corrupt(mark_column));
    }
    let settled_at = stored_settlement
        .map(|stored_time| {
            let column = "mail_messages.deadline_settled_at";
            let settled_at: Timestamp = parse(stored_time, column)?;
            if settled_at < mark
                || !central_engine_model::supported_time(settled_at)
                || format_utc_millis(settled_at) != stored_time
            {
                return Err(corrupt(column));
            }
            Ok(settled_at)
        })
        .transpose()?;
    Ok(DirectDeadlineEvidence {
        cutoff,
        mark,
        settled_at,
    })
}

impl StoredDirectState {
    pub(crate) fn decode_direct_message(&self) -> Result<DirectMessageRow, StoreFailure> {
        if self.message_kind != "direct" {
            return Err(corrupt("mail_messages.message_kind"));
        }
        let mail::MessageSender::Actor(sender) =
            serde_json::from_str(&self.sender).map_err(|_| corrupt("mail_messages.sender"))?
        else {
            return Err(corrupt("mail_messages.sender"));
        };
        let created_at: Timestamp = parse(&self.created_at, "mail_messages.created_at")?;
        let _: Timestamp = parse(&self.updated_at, "mail_messages.updated_at")?;
        let deadline = decode_direct_deadline(
            created_at,
            self.delivery_window_ends_at.as_deref(),
            self.cancel_due_at.as_deref(),
            self.deadline_settled_at.as_deref(),
        )?;
        Ok(DirectMessageRow {
            id: parse(&self.message_id, "mail_messages.message_id")?,
            sender,
            sender_request_id: required(
                self.sender_request_id.as_deref(),
                "mail_messages.sender_request_id",
            )?,
            recipient: parse(&self.target_machine_id, "mail_messages.target_machine_id")?,
            cutoff: deadline.cutoff,
            mark: deadline.mark,
            lifecycle: self.decode_lifecycle()?,
            deadline_settled_at: deadline.settled_at,
        })
    }

    fn decode_lifecycle(&self) -> Result<DirectLifecycle, StoreFailure> {
        let arrived = self
            .delivered_arrived_at
            .as_deref()
            .map(|text| parse(text, "mail_messages.delivered_arrived_at"))
            .transpose()?;
        let seen = self
            .seen_completed_at
            .as_deref()
            .map(|text| parse(text, "mail_messages.seen_completed_at"))
            .transpose()?;
        let refused = self
            .failure_reason
            .as_deref()
            .map(|text| enum_value(text, "mail_messages.failure_reason"))
            .transpose()?;
        match (self.state.as_str(), arrived, seen, refused) {
            ("queued", None, None, None) => Ok(DirectLifecycle::Queued),
            ("delivered", Some(arrived_at), None, None) => {
                Ok(DirectLifecycle::Delivered { arrived_at })
            }
            ("seen", arrived_at, Some(completed_at), None) => Ok(DirectLifecycle::Final {
                receipt: FinalReceipt::Seen { completed_at },
                arrived_at,
            }),
            ("refused", arrived_at, None, Some(reason)) => Ok(DirectLifecycle::Final {
                receipt: FinalReceipt::Refused { reason },
                arrived_at,
            }),
            _ => Err(corrupt("mail_messages.state")),
        }
    }
}

pub(crate) fn enum_text(value: impl serde::Serialize) -> Result<String, StoreFailure> {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(text)) => Ok(text),
        _ => Err(corrupt("mail_messages.receipt")),
    }
}

#[cfg(test)]
#[path = "direct_message_rows_test.rs"]
mod tests;
