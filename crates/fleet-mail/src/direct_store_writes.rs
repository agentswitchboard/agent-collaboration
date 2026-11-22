//! Checked direct writes. Each operation is evaluated within the lent transaction.
use crate::{
    direct_message::{
        DirectLifecycle, DirectMessage, DirectMessageEffect, DirectMessageEvent,
        DirectMessageRefusal, DirectMessageSubject, DirectReducerState, FinalReceipt,
    },
    direct_message_rows::enum_text,
    direct_recipient_admission::admit_enrolled_direct_recipient,
    direct_store_port::{AdmittedDirectSend, AdmittedReceipt, ApplyReceipt, DirectSendResult},
    direct_store_reads::{StoredDirectState, read_stored_direct_message},
    mail_write_failure::require_one_affected_row,
    message_deadline::{cancel_due_at, delivery_window_ends_at, system_message_deadline},
    system_receipt_settlement::{SystemReceiptKind, settle_system_receipt},
};
use central_engine_model::{Context, Machine, TransitionError};
use central_storage::{EngineAlarmPlan, StoreFailure};
use fleet_contracts::MailDeliveryReady;
use fleet_engine::CentralTransaction;
use fleet_request_log::{
    RetainRequest, prune_expired_organization_requests, upsert_organization_request_answer,
};
use fleet_runtime_ports::{
    IdSource, stored_values::format_utc_millis, workspace_decode_error::corrupt,
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
    fn with_alarm_recompute(outcome: A) -> Self {
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
macro_rules! execute {
    ($file:literal, $($arg:expr),+ $(,)?) => {
        query::<T::Database>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
    };
}
macro_rules! select {
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
        let result = insert_send!(
            execute,
            message,
            cancel_due_at(command.sent_at, command.accepted_at),
            command.request.request_id,
            delivery_window_ends_at(command.sent_at, command.accepted_at)
        )
        .execute(tx.executor())
        .await?;
        require_one_affected_row::<T>(result)?;
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
struct StoredReceiptEnrollment {
    #[sqlx(rename = "machine_id!: String")]
    machine_id: String,
}
#[derive(sqlx::FromRow)]
struct StoredReceiptRoute {
    target_machine_id: String,
}
#[derive(sqlx::FromRow)]
struct StoredReceiptKind {
    message_kind: String,
}
macro_rules! enrollment_query {
    ($emit:ident,$auth:expr) => {
        $emit!(
            "src/queries/read_receipt_enrollment.sql",
            $auth.machine_id().to_string(),
            $auth.enrollment_id().as_str().to_owned(),
            $auth.client_id().as_str().to_owned()
        )
    };
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

pub async fn apply_receipt<T: CentralTransaction>(
    tx: &mut T,
    command: AdmittedReceipt,
) -> Result<DirectWriteOutcome<ApplyReceipt>, DirectApplyFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let authority = command.authority;
    let target = authority.machine_id();
    let enrollment: Option<StoredReceiptEnrollment> = enrollment_query!(select, authority)
        .fetch_optional(tx.executor())
        .await?;
    // Sample after the owner has lent its write transaction and re-read enrollment.
    let applying_time = Timestamp::from_timestamp_millis(command.clock.now().unix_millis())
        .ok_or_else(|| corrupt("clock"))?;
    if applying_time.timestamp() >= authority.effective_expires_at().0
        || enrollment.is_none_or(|row| row.machine_id != target.as_str())
    {
        return Ok(DirectWriteOutcome::without_alarm_recompute(
            ApplyReceipt::AuthorityLapsed,
        ));
    }
    let row: Option<StoredReceiptRoute> = route_query!(select, command.message_id)
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
    let kind: Option<StoredReceiptKind> = owned_kind_query!(select, command.message_id, target)
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
            $row.waiting,
            $row.seen,
            $row.not_seen,
            ($prior).subject.id.to_string(),
            ($prior).subject.recipient.to_string(),
            ($prior).lifecycle.tag().to_owned()
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
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
{
    let row = stored
        .decode()
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
            cutoff: row.cutoff,
            mark: row.mark,
        },
        lifecycle: row.lifecycle,
    };
    let (next, effects) = match DirectMessage::transition(
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
    let encoding = DirectStateUpdate::new(next.lifecycle)?;
    let result = lifecycle_update!(execute, encoding, &state, now)
        .execute(tx.executor())
        .await?;
    require_one_affected_row::<T>(result)?;
    let mut ready = None;
    for DirectMessageEffect::TellSender(message) in effects {
        let cutoff = system_message_deadline(message.created_at)
            .ok_or_else(|| corrupt("mail_messages.delivery_window_ends_at"))?;
        let result = insert_outcome!(execute, message, cutoff)
            .execute(tx.executor())
            .await?;
        require_one_affected_row::<T>(result)?;
        ready = Some((&message).into());
    }
    clear_attempt!(execute, state.subject.id)
        .execute(tx.executor())
        .await?;
    Ok(AppliedTransition::Changed(ready))
}

struct DirectStateUpdate {
    tag: &'static str,
    failure: Option<String>,
    arrived: Option<String>,
    waiting: Option<String>,
    seen: Option<String>,
    not_seen: Option<String>,
}
impl DirectStateUpdate {
    fn new(lifecycle: DirectLifecycle) -> Result<Self, StoreFailure> {
        let mut encoded = Self {
            tag: lifecycle.tag(),
            failure: None,
            arrived: None,
            waiting: None,
            seen: None,
            not_seen: None,
        };
        if let Some(saved) = lifecycle.saved() {
            encoded.arrived = Some(saved.arrived_at.to_rfc3339());
            encoded.waiting = saved.waiting.map(enum_text).transpose()?;
        }
        if let DirectLifecycle::Final { receipt, .. } = lifecycle {
            match receipt {
                FinalReceipt::Seen { completed_at } => {
                    encoded.seen = Some(completed_at.to_rfc3339())
                }
                FinalReceipt::NotSeen { waiting } => encoded.not_seen = Some(enum_text(waiting)?),
                FinalReceipt::Refused { reason } => encoded.failure = Some(enum_text(reason)?),
            }
        }
        Ok(encoded)
    }
}

#[cfg(test)]
#[path = "direct_store_writes_test.rs"]
mod tests;
