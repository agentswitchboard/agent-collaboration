//! Apply an eligible direct mark or charge a previously rolled-back rejection.
use crate::{
    direct_message::{DirectMessage, DirectMessageEvent},
    direct_store_port::{ApplyMark, DirectMark, DirectReadFailure, RejectedDirectMark},
    direct_store_reads::read_stored_direct_message,
    direct_store_writes::{AppliedTransition, DirectApplyFailure, apply_direct_transition},
};
use central_engine_model::{FailureRecord, InstanceKey, ItemFailureClass, Machine};
use central_storage::{CorruptReason, StoreFailure};
use fleet_contracts::EngineSubject;
use fleet_engine::{
    CentralTransaction, CheckedAttempt, CheckedDueFailure, checked_statement_rejection,
    observe_checked_attempt, record_checked_attempt,
};
use fleet_runtime_ports::workspace_decode_error::corrupt;
use sqlx_core::{
    arguments::IntoArguments, column::ColumnIndex, database::Database, decode::Decode,
    encode::Encode, query_as::query_as, types::Type,
};

#[derive(sqlx::FromRow)]
pub(super) struct StoredMarkGuard {
    #[sqlx(rename = "occurrence_ms!: i64")]
    occurrence_ms: i64,
    #[sqlx(rename = "effective_due_ms!: i64")]
    effective_due_ms: i64,
}

macro_rules! mark_guard {
    ($emit:ident,$message:expr) => {
        $emit!("src/queries/read_due_mark_guard.sql", $message.to_string())
    };
}

fn direct_mark_subject(mark: &DirectMark) -> EngineSubject {
    EngineSubject {
        kind: DirectMessage::KIND,
        key: InstanceKey(mark.message_id.to_string()),
    }
}

async fn is_eligible<T: CentralTransaction>(
    transaction: &mut T,
    mark: &DirectMark,
) -> Result<bool, CheckedDueFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'value> Encode<'value, T::Database>,
    for<'row> &'row str: ColumnIndex<<T::Database as Database>::Row>,
    i64: Type<T::Database> + for<'row> Decode<'row, T::Database>,
{
    let horizon = mark
        .now
        .checked_add_signed(central_engine_model::DueBatchPolicy::DEFAULT.look_ahead)
        .ok_or_else(|| CheckedDueFailure::from_store(corrupt("mail_messages.cancel_due_at")))?;
    macro_rules! bind_due_mark_guard {
        ($file:literal, $($arg:expr),+) => {
            query_as::<T::Database, StoredMarkGuard>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)))$(.bind($arg))+
        };
    }
    let due = mark_guard!(bind_due_mark_guard, mark.message_id)
        .fetch_optional(transaction.executor())
        .await
        .map_err(|_| CheckedDueFailure::Unavailable)?;
    Ok(due.is_some_and(|due| {
        due.occurrence_ms == mark.occurrence.timestamp_millis()
            && due.effective_due_ms <= horizon.timestamp_millis()
    }))
}

/// The owning host must roll back before returning this result to the Engine.
pub enum MarkRollback {
    Superseded,
    Rejected(Box<RejectedDirectMark>),
    Failed(CheckedDueFailure),
}
impl MarkRollback {
    pub fn after_rollback(self) -> Result<ApplyMark, CheckedDueFailure> {
        match self {
            Self::Superseded => Ok(ApplyMark::Stale),
            Self::Rejected(rejected) => Ok(ApplyMark::Rejected(rejected)),
            Self::Failed(failure) => Err(failure),
        }
    }
}
impl From<CheckedDueFailure> for MarkRollback {
    fn from(failure: CheckedDueFailure) -> Self {
        Self::Failed(failure)
    }
}

fn corrupt_class(reason: CorruptReason) -> ItemFailureClass {
    let column = match reason {
        CorruptReason::BadValue { column }
        | CorruptReason::UnknownKind { column }
        | CorruptReason::MissingColumn { column } => column,
    };
    ItemFailureClass::CorruptRow {
        column,
        raw_value: None,
    }
}

pub async fn apply_mark<T: CentralTransaction>(
    transaction: &mut T,
    command: DirectMark,
) -> Result<ApplyMark, MarkRollback>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
    i64: Type<T::Database> + for<'r> Decode<'r, T::Database> + for<'q> Encode<'q, T::Database>,
{
    if !is_eligible(transaction, &command).await? {
        return Ok(ApplyMark::Stale);
    }
    let subject = direct_mark_subject(&command);
    let observed = observe_checked_attempt(transaction, &subject, command.now)
        .await
        .map_err(CheckedDueFailure::from_store)?;
    let previous = match observed {
        CheckedAttempt::Deferred => return Ok(ApplyMark::Stale),
        CheckedAttempt::Ready(previous) => previous,
        CheckedAttempt::Corrupt(class) => {
            return record_checked_attempt(
                transaction,
                &subject,
                None,
                class,
                command.now,
                command.ids.as_ref(),
            )
            .await
            .map(|saved| saved.map_or(ApplyMark::Stale, ApplyMark::Failed))
            .map_err(|failure| CheckedDueFailure::from_store(failure).into());
        }
    };
    let stored = match read_stored_direct_message(transaction, &command.message_id).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return Ok(ApplyMark::Stale),
        Err(DirectReadFailure::Unavailable) => return Err(CheckedDueFailure::Unavailable.into()),
        Err(DirectReadFailure::Corrupt { reason }) => {
            return record_corrupt_mark(transaction, &subject, previous.as_ref(), &command, reason)
                .await;
        }
    };
    let applied = apply_direct_transition(
        transaction,
        &stored,
        None,
        DirectMessageEvent::Due {
            occurrence: command.occurrence,
        },
        command.now,
        command.ids.as_ref(),
    )
    .await;
    let rejection = match applied {
        Ok(AppliedTransition::Changed(ready)) => return Ok(ApplyMark::Applied { ready }),
        Ok(
            AppliedTransition::Missing | AppliedTransition::Covered | AppliedTransition::Settled,
        ) => {
            return Ok(ApplyMark::Stale);
        }
        Err(DirectApplyFailure::CorruptObserved(StoreFailure::Corrupt { reason })) => {
            return record_corrupt_mark(transaction, &subject, previous.as_ref(), &command, reason)
                .await;
        }
        Err(DirectApplyFailure::Conflict) => return Err(MarkRollback::Superseded),
        Err(DirectApplyFailure::Store(StoreFailure::Corrupt { reason })) => corrupt_class(reason),
        Err(DirectApplyFailure::Sql(error)) => {
            checked_statement_rejection(&error).ok_or(CheckedDueFailure::Unavailable)?
        }
        Err(DirectApplyFailure::Store(error) | DirectApplyFailure::CorruptObserved(error)) => {
            return Err(CheckedDueFailure::from_store(error).into());
        }
    };
    Err(MarkRollback::Rejected(Box::new(RejectedDirectMark {
        command,
        row: stored,
        attempt: previous,
        class: rejection,
    })))
}

// The eligible subject is protected by the same transaction as this read failure.
async fn record_corrupt_mark<T: CentralTransaction>(
    transaction: &mut T,
    subject: &EngineSubject,
    previous: Option<&FailureRecord>,
    command: &DirectMark,
    reason: CorruptReason,
) -> Result<ApplyMark, MarkRollback>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    Option<String>: Type<T::Database> + for<'q> Encode<'q, T::Database>,
    i64: Type<T::Database> + for<'q> Encode<'q, T::Database>,
{
    record_checked_attempt(
        transaction,
        subject,
        previous,
        corrupt_class(reason),
        command.now,
        command.ids.as_ref(),
    )
    .await
    .map(|saved| saved.map_or(ApplyMark::Stale, ApplyMark::Failed))
    .map_err(|failure| CheckedDueFailure::from_store(failure).into())
}

pub async fn charge_rejected_mark<T: CentralTransaction>(
    transaction: &mut T,
    rejected: RejectedDirectMark,
    now: workspace_model::Timestamp,
) -> Result<Option<FailureRecord>, CheckedDueFailure>
where
    <T::Database as Database>::Arguments: IntoArguments<T::Database>,
    String: Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    Option<String>:
        Type<T::Database> + for<'q> Encode<'q, T::Database> + for<'r> Decode<'r, T::Database>,
    for<'r> &'r str: ColumnIndex<<T::Database as Database>::Row>,
    i64: Type<T::Database> + for<'r> Decode<'r, T::Database> + for<'q> Encode<'q, T::Database>,
{
    let command = DirectMark {
        now,
        ..rejected.command
    };
    if !is_eligible(transaction, &command).await? {
        return Ok(None);
    }
    let current = read_stored_direct_message(transaction, &command.message_id)
        .await
        .map_err(|failure| CheckedDueFailure::from_store(failure.into()))?;
    if current.as_ref() != Some(&rejected.row) {
        return Ok(None);
    }
    let subject = direct_mark_subject(&command);
    let observed = observe_checked_attempt(transaction, &subject, command.now)
        .await
        .map_err(CheckedDueFailure::from_store)?;
    let CheckedAttempt::Ready(previous) = observed else {
        return Ok(None);
    };
    if previous != rejected.attempt {
        return Ok(None);
    }
    record_checked_attempt(
        transaction,
        &subject,
        previous.as_ref(),
        rejected.class,
        command.now,
        command.ids.as_ref(),
    )
    .await
    .map_err(CheckedDueFailure::from_store)
}

#[cfg(test)]
#[path = "direct_mark_apply_test.rs"]
mod tests;
