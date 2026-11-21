//! Validate persisted receipt evidence as one direct lifecycle.
use crate::direct_message::{DirectLifecycle, FinalReceipt, SavedDelivery};
use central_storage::StoreFailure;
use fleet_runtime_ports::workspace_decode_error::corrupt;
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
    pub(crate) delivered_waiting_reason: Option<String>,
    pub(crate) seen_completed_at: Option<String>,
    pub(crate) not_seen_waiting_reason: Option<String>,
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

impl StoredDirectState {
    pub(crate) fn decode(&self) -> Result<DirectMessageRow, StoreFailure> {
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
        let cutoff: Timestamp = required(
            self.delivery_window_ends_at.as_deref(),
            "mail_messages.delivery_window_ends_at",
        )?;
        let mark: Timestamp =
            required(self.cancel_due_at.as_deref(), "mail_messages.cancel_due_at")?;
        if cutoff < created_at
            || mark <= cutoff
            || !central_engine_model::supported_time(mark)
            || !central_engine_model::on_five_minute_grid(mark)
        {
            return Err(corrupt("mail_messages.cancel_due_at"));
        }
        Ok(DirectMessageRow {
            id: parse(&self.message_id, "mail_messages.message_id")?,
            sender,
            sender_request_id: required(
                self.sender_request_id.as_deref(),
                "mail_messages.sender_request_id",
            )?,
            recipient: parse(&self.target_machine_id, "mail_messages.target_machine_id")?,
            cutoff,
            mark,
            lifecycle: self.lifecycle()?,
        })
    }

    fn lifecycle(&self) -> Result<DirectLifecycle, StoreFailure> {
        let arrived = self
            .delivered_arrived_at
            .as_deref()
            .map(|text| parse(text, "mail_messages.delivered_arrived_at"))
            .transpose()?;
        let waiting = self
            .delivered_waiting_reason
            .as_deref()
            .map(|text| enum_value(text, "mail_messages.delivered_waiting_reason"))
            .transpose()?;
        let saved = match (arrived, waiting) {
            (Some(arrived_at), waiting) => Some(SavedDelivery {
                arrived_at,
                waiting,
            }),
            (None, None) => None,
            (None, Some(_)) => return Err(corrupt("mail_messages.delivered_arrived_at")),
        };
        let seen = self
            .seen_completed_at
            .as_deref()
            .map(|text| parse(text, "mail_messages.seen_completed_at"))
            .transpose()?;
        let not_seen = self
            .not_seen_waiting_reason
            .as_deref()
            .map(|text| enum_value(text, "mail_messages.not_seen_waiting_reason"))
            .transpose()?;
        let refused = self
            .failure_reason
            .as_deref()
            .map(|text| enum_value(text, "mail_messages.failure_reason"))
            .transpose()?;
        match (self.state.as_str(), saved, seen, not_seen, refused) {
            ("queued", None, None, None, None) => Ok(DirectLifecycle::Queued),
            ("delivered", Some(saved), None, None, None) => Ok(DirectLifecycle::Delivered(saved)),
            ("pastTheMark", saved, None, None, None) => Ok(DirectLifecycle::PastTheMark(saved)),
            ("seen", saved, Some(completed_at), None, None) => Ok(DirectLifecycle::Final {
                receipt: FinalReceipt::Seen { completed_at },
                saved,
            }),
            ("notSeen", saved, None, Some(waiting), None) => Ok(DirectLifecycle::Final {
                receipt: FinalReceipt::NotSeen { waiting },
                saved,
            }),
            ("refused", saved, None, None, Some(reason)) => Ok(DirectLifecycle::Final {
                receipt: FinalReceipt::Refused { reason },
                saved,
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
