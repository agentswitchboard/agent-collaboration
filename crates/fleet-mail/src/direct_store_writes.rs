//! Checked direct writes. Each operation is evaluated within the lent transaction.
use crate::{
    direct_message::{
        DirectLifecycle, DirectMessage, DirectMessageEffect, DirectMessageEvent,
        DirectMessageRefusal, DirectMessageSubject, DirectReducerState, FinalReceipt,
        apply_message_event,
    },
    direct_message_rows::enum_text,
    direct_recipient_admission::admit_enrolled_direct_recipient,
    direct_store_port::{AdmittedDirectSend, AdmittedReceipt, ApplyReceipt, DirectSendResult},
    direct_store_reads::{StoredDirectState, read_stored_direct_message},
    mail_write_failure::require_one_affected_row,
    message_deadline::{
        delivery_window_ends_at, direct_deadline_occurrence, system_message_deadline,
    },
    system_receipt_settlement::{SystemReceiptKind, settle_system_receipt},
};
use central_engine_model::{Context, Machine, TransitionError};
use central_storage::{EngineAlarmPlan, StoreFailure};
use fleet_contracts::MailDeliveryReady;
use fleet_engine::{DueItemKey, DueKind, DueOwnerTiming, DueTableChange, apply_due_table_change};
use fleet_lent_transaction::CentralTransaction;
use fleet_request_log::{
    RetainRequest, prune_expired_organization_requests, upsert_organization_request_answer,
};
use fleet_runtime_ports::{
    Clock, IdSource, stored_values::format_utc_millis, workspace_decode_error::corrupt,
};
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query::query, query_as::query_as, types::Type,
};
use workspace_ids::{MachineId, MessageId};
use workspace_model::{
    Timestamp,
    central_mail::{
        CentralMailAnswer, CentralMailRefusal, CentralMailResult, MessageBody, MessageRecord,
        MessageSender, MessageState, ReceiptResult,
    },
};

pub use crate::mail_write_failure::DirectApplyFailure;

/// Host plumbing consumes the owner's outcome and the existing typed alarm directive.
pub struct DirectWriteOutcome<A> {
    pub outcome: A,
    pub alarm: Option<EngineAlarmPlan>,
}

impl<A> DirectWriteOutcome<A> {
    pub(super) fn without_alarm_recompute(outcome: A) -> Self {
        Self {
            outcome,
            alarm: None,
        }
    }
    pub(super) fn with_alarm_recompute(outcome: A) -> Self {
        Self {
            outcome,
            alarm: Some(EngineAlarmPlan::Recompute),
        }
    }
}

fn encode_mail_json(value: impl serde::Serialize) -> Result<String, StoreFailure> {
    serde_json::to_string(&value).map_err(|_| corrupt("mail_messages.json"))
}

macro_rules! insert_send {
    ($emit:ident,$message:ident,$mark:expr,$request:expr,$cutoff:expr) => {
        $emit!(
            "src/queries/insert_direct_send.sql",
            $message.message_id.to_string(),
            encode_mail_json(&$message.sender)?,
            $message.target.machine_id.to_string(),
            encode_mail_json(&$message.target)?,
            encode_mail_json(&$message.body)?,
            format_utc_millis($message.created_at),
            format_utc_millis($message.updated_at),
            format_utc_millis($mark),
            $request.to_string(),
            format_utc_millis($cutoff)
        )
    };
}

macro_rules! insert_outcome {
    ($emit:ident,$message:ident,$cutoff:expr) => {
        $emit!(
            "src/queries/insert_direct_outcome.sql",
            $message.message_id.to_string(),
            encode_mail_json(&$message.sender)?,
            $message.target.machine_id.to_string(),
            encode_mail_json(&$message.target)?,
            encode_mail_json(&$message.body)?,
            format_utc_millis($message.created_at),
            format_utc_millis($message.updated_at),
            format_utc_millis($cutoff)
        )
    };
}
macro_rules! bind_direct_mail_write {
    ($file:literal, $($arg:expr),+ $(,)?) => {
        query::<T::Database>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
    };
}
macro_rules! bind_receipt_projection {
    ($file:literal, $($arg:expr),+ $(,)?) => {
        query_as::<T::Database, _>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
    };
}
pub async fn apply_direct_send<T: CentralTransaction>(
    tx: &mut T,
    command: AdmittedDirectSend,
) -> Result<DirectWriteOutcome<DirectSendResult>, DirectApplyFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    &'static str: ColumnIndex<<T::Database as Database>::Row>,
{
    let target = command.send.to;
    let (reply, decision) = if admit_enrolled_direct_recipient(tx, &target.machine_id).await? {
        let message_id: MessageId = command
            .ids
            .new_id()
            .to_string()
            .try_into()
            .map_err(|_| corrupt("minted id"))?;
        let message = MessageRecord {
            message_id: message_id.clone(),
            sender: MessageSender::Actor(command.caller.clone()),
            target,
            body: MessageBody::Direct {
                body: command.send.body,
                delivery: command.send.delivery,
                reply_to: command.send.reply_to,
            },
            state: MessageState::Queued,
            source: None,
            created_at: command.accepted_at,
            updated_at: command.accepted_at,
        };
        let cutoff = delivery_window_ends_at(command.sent_at, command.accepted_at)
            .ok_or_else(|| corrupt("mail_messages.delivery_window_ends_at"))?;
        let mark = direct_deadline_occurrence(cutoff)
            .ok_or_else(|| corrupt("mail_messages.cancel_due_at"))?;
        let timing = DueOwnerTiming::at(due_key(DueKind::DirectMessage, &message_id), mark)
            .map_err(|_| corrupt("mail_messages.cancel_due_at"))?;
        let result = insert_send!(
            bind_direct_mail_write,
            message,
            mark,
            command.request.request_id,
            cutoff
        )
        .execute(tx.executor())
        .await?;
        require_one_affected_row::<T>(result)?;
        apply_due_table_change(
            tx,
            &DueTableChange::PublishItem(timing),
            command.accepted_at,
        )
        .await?;
        (
            CentralMailResult::Done(CentralMailAnswer::Queued {
                message_id: message_id.clone(),
            }),
            DirectSendResult::Committed {
                message_id,
                ready: (&message).into(),
            },
        )
    } else {
        (
            CentralMailResult::Refused(CentralMailRefusal::UnknownSession {
                target: target.clone(),
            }),
            DirectSendResult::RetainedRefusal { target },
        )
    };
    let result = upsert_organization_request_answer(
        tx,
        RetainRequest {
            request_id: &command.request.request_id,
            caller: &command.caller,
            digest: &command.request.digest,
            answer: encode_mail_json(reply)?,
            at: command.accepted_at,
        },
    )
    .await?;
    if result != 1 {
        return Err(DirectApplyFailure::Conflict);
    }
    prune_expired_organization_requests(tx, command.request.cutoff).await?;
    Ok(DirectWriteOutcome::with_alarm_recompute(decision))
}

#[derive(sqlx::FromRow)]
struct StoredReceiptRoute {
    target_machine_id: String,
}
#[derive(sqlx::FromRow)]
struct StoredReceiptKind {
    message_kind: String,
}
macro_rules! route_query {
    ($emit:ident,$message:expr) => {
        $emit!("src/queries/read_receipt_target.sql", $message.to_string())
    };
}
macro_rules! owned_kind_query {
    ($emit:ident,$message:expr,$target:expr) => {
        $emit!(
            "src/queries/read_owned_receipt_kind.sql",
            $message.to_string(),
            $target.to_string()
        )
    };
}
macro_rules! clear_attempt {
    ($emit:ident,$message:expr) => {
        $emit!("src/queries/clear_direct_attempt.sql", $message.to_string())
    };
}

/// Apply the admitted command with the owning host’s clock.
pub async fn apply_receipt<T: CentralTransaction>(
    tx: &mut T,
    command: AdmittedReceipt,
    clock: &dyn Clock,
) -> Result<DirectWriteOutcome<ApplyReceipt>, DirectApplyFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    i64: Type<T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let authority = command.authority;
    let target = authority.machine_id();
    // Sample after the authority reads, while the applying transaction is held.
    let (applying_time, lapse) = fleet_machine_login::link_authority::recheck_lent_link_authority(
        tx,
        authority.attachment_ref(),
        clock,
    )
    .await?;
    if let Some(cause) = lapse {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            ApplyReceipt::AuthorityLapsed(cause),
        ));
    }
    let applying_time = Timestamp::from_timestamp_millis(applying_time.unix_millis())
        .ok_or_else(|| corrupt("clock"))?;
    let row: Option<StoredReceiptRoute> = route_query!(bind_receipt_projection, command.message_id)
        .fetch_optional(tx.executor())
        .await?;
    let Some(row) = row else {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            ApplyReceipt::Stop(ReceiptResult::NoSuchMessage),
        ));
    };
    let recipient: MachineId = row
        .target_machine_id
        .parse()
        .map_err(|_| corrupt("mail_messages.target_machine_id"))?;
    if &recipient != target {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            ApplyReceipt::Stop(ReceiptResult::NoSuchMessage),
        ));
    }
    let kind: Option<StoredReceiptKind> =
        owned_kind_query!(bind_receipt_projection, command.message_id, target)
            .fetch_optional(tx.executor())
            .await?;
    let Some(kind) = kind else {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            ApplyReceipt::Stop(ReceiptResult::NoSuchMessage),
        ));
    };
    match kind.message_kind.as_str() {
        "direct" => {
            let Some(stored) = read_stored_direct_message(tx, &command.message_id).await? else {
                return Ok(DirectWriteOutcome::without_alarm_recompute(
                    ApplyReceipt::Stop(ReceiptResult::NoSuchMessage),
                ));
            };
            match apply_direct_transition(
                tx,
                &stored,
                Some(target),
                DirectMessageEvent::Receipt(command.receipt),
                applying_time,
                command.ids.as_ref(),
            )
            .await?
            {
                AppliedTransition::Changed(ready) => Ok(DirectWriteOutcome::with_alarm_recompute(
                    ApplyReceipt::Applied { ready },
                )),
                AppliedTransition::Missing => Ok(DirectWriteOutcome::without_alarm_recompute(
                    ApplyReceipt::Stop(ReceiptResult::NoSuchMessage),
                )),
                AppliedTransition::Covered => Ok(DirectWriteOutcome::without_alarm_recompute(
                    ApplyReceipt::Stop(ReceiptResult::Recorded),
                )),
                AppliedTransition::Settled => Ok(DirectWriteOutcome::without_alarm_recompute(
                    ApplyReceipt::Stop(ReceiptResult::AlreadySettled),
                )),
            }
        }
        stored_kind => {
            let kind = SystemReceiptKind::decode_storage_tag(stored_kind)?;
            settle_system_receipt(
                tx,
                &command.message_id,
                target,
                kind,
                command.receipt,
                applying_time,
            )
            .await
            .map(DirectWriteOutcome::without_alarm_recompute)
        }
    }
}

struct ReducerIds<'a>(&'a dyn IdSource);
impl central_engine_model::IdSource for ReducerIds<'_> {
    fn next_id(&mut self) -> uuid::Uuid {
        self.0.new_id()
    }
}

pub(super) enum AppliedTransition {
    Changed(Option<MailDeliveryReady>),
    Missing,
    Covered,
    Settled,
}

macro_rules! lifecycle_update {
    ($emit:ident,$row:ident,$prior:expr,$now:expr) => {
        $emit!(
            "src/queries/update_direct_lifecycle.sql",
            $row.tag.to_owned(),
            $row.failure,
            format_utc_millis($now),
            $row.arrived,
            $row.seen,
            $row.deadline_settled_at,
            ($prior).subject.id.to_string(),
            ($prior).subject.recipient.to_string(),
            ($prior).lifecycle.storage_tag().to_owned(),
            ($prior).deadline_settled_at.map(format_utc_millis)
        )
    };
}

pub(super) async fn apply_direct_transition<T: CentralTransaction>(
    tx: &mut T,
    stored: &StoredDirectState,
    recipient: Option<&MachineId>,
    event: DirectMessageEvent,
    now: Timestamp,
    ids: &dyn IdSource,
) -> Result<AppliedTransition, DirectApplyFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let row = stored
        .decode_direct_message()
        .map_err(DirectApplyFailure::CorruptObserved)?;
    if recipient.is_some_and(|recipient| recipient != &row.recipient) {
        return Ok(AppliedTransition::Missing);
    }
    let sender = crate::direct_store_sender::read_sender_identity(tx, &row.sender)
        .await
        .map_err(DirectApplyFailure::CorruptObserved)?
        .ok_or_else(|| DirectApplyFailure::CorruptObserved(corrupt("mail_messages.sender")))?;
    let state = DirectReducerState {
        subject: DirectMessageSubject {
            id: row.id,
            sender_session: match sender {
                workspace_model::Identity::AgentSession { session } => Some(session),
                workspace_model::Identity::RouterComponent { .. }
                | workspace_model::Identity::Person { .. } => None,
            },
            sender_request_id: row.sender_request_id,
            recipient: row.recipient,
            mark: row.mark,
        },
        lifecycle: row.lifecycle,
        deadline_settled_at: row.deadline_settled_at,
    };
    let (next, effects) = match apply_message_event(
        &state,
        event,
        &mut Context {
            now,
            ids: &mut ReducerIds(ids),
        },
    ) {
        Ok(next) => next,
        Err(
            TransitionError::Stale | TransitionError::Domain(DirectMessageRefusal::AlreadySettled),
        ) => return Ok(AppliedTransition::Settled),
        Err(TransitionError::Domain(DirectMessageRefusal::AlreadyRecorded)) => {
            return Ok(AppliedTransition::Covered);
        }
        Err(
            TransitionError::NotAllowed { .. }
            | TransitionError::Domain(DirectMessageRefusal::InvalidIdentifier),
        ) => return Err(corrupt("mail_messages.transition").into()),
    };
    let encoding = DirectStateUpdate::new(&next)?;
    let result = lifecycle_update!(bind_direct_mail_write, encoding, &state, now)
        .execute(tx.executor())
        .await?;
    require_one_affected_row::<T>(result)?;
    let mut ready = None;
    for DirectMessageEffect::TellSender(message) in effects {
        let cutoff = system_message_deadline(message.created_at)
            .ok_or_else(|| corrupt("mail_messages.delivery_window_ends_at"))?;
        let result = insert_outcome!(bind_direct_mail_write, message, cutoff)
            .execute(tx.executor())
            .await?;
        require_one_affected_row::<T>(result)?;
        apply_due_table_change(
            tx,
            &DueTableChange::PublishItem(DueOwnerTiming::at(
                due_key(DueKind::SystemMessageExpiry, &message.message_id),
                cutoff,
            )?),
            now,
        )
        .await?;
        ready = Some((&message).into());
    }
    clear_attempt!(bind_direct_mail_write, state.subject.id)
        .execute(tx.executor())
        .await?;
    let key = due_key(DueKind::DirectMessage, &state.subject.id);
    let due = match DirectMessage::next_due(&next) {
        Some(mark) => DueTableChange::PublishItem(DueOwnerTiming::at(key, mark)?),
        None => DueTableChange::RemoveItem(key),
    };
    apply_due_table_change(tx, &due, now).await?;
    Ok(AppliedTransition::Changed(ready))
}

fn due_key(kind: DueKind, id: &MessageId) -> DueItemKey {
    DueItemKey {
        kind,
        subject: central_engine_model::InstanceKey(id.to_string()),
    }
}

struct DirectStateUpdate {
    tag: &'static str,
    failure: Option<String>,
    arrived: Option<String>,
    seen: Option<String>,
    deadline_settled_at: Option<String>,
}
impl DirectStateUpdate {
    fn new(next: &DirectReducerState) -> Result<Self, StoreFailure> {
        let (seen, failure) = match next.lifecycle {
            DirectLifecycle::Queued | DirectLifecycle::Delivered { .. } => (None, None),
            DirectLifecycle::Final { receipt, .. } => match receipt {
                FinalReceipt::Seen { completed_at } => (Some(completed_at.to_rfc3339()), None),
                FinalReceipt::Refused { reason } => (None, Some(enum_text(reason)?)),
            },
        };
        Ok(Self {
            tag: next.lifecycle.storage_tag(),
            failure,
            arrived: next
                .lifecycle
                .delivered_arrival()
                .map(|arrived_at| arrived_at.to_rfc3339()),
            seen,
            deadline_settled_at: next.deadline_settled_at.map(format_utc_millis),
        })
    }
}

#[cfg(test)]
#[path = "direct_store_writes_test.rs"]
mod tests;
