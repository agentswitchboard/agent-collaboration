//! The Mail owner plans guarded edits to the typed bodies of queued notices.
use crate::stored_messages::decode_messages;
use central_storage::{GuardedStatement, SqlValue, Statement, StoreFailure};
use fleet_engine::{DueItemKey, DueKind, DueTableChange, plan_due_table_change};
use fleet_runtime_ports::{
    CentralStore,
    guarded_statements::{exact, text},
    stored_values::{format_utc_millis, string},
    workspace_decode_error::corrupt,
};
use workspace_model::{
    ActorIdentityId, ThreadId, Timestamp, WorkspaceId,
    central_mail::{MessageBody, MessageState},
};

fn notice_messages() -> Statement {
    Statement {
        sql: "SELECT message_id,sender,target_machine_id,target_session,message_kind,body,state,failure_reason,source_change_id,recipient_actor,source_workspace_id,source_thread_id,created_at,updated_at FROM mail_messages WHERE message_kind='notice' AND state='queued'",
        args: vec![],
    }
}

macro_rules! update_notice {
    ($emit:ident, $($argument:expr),+ $(,)?) => {
        $emit!("src/queries/update_cancelled_notice_items.sql", $($argument),+)
    };
}

/// The caller holds the Central turn through the drain's applying batch.
pub async fn plan_cancel_queued_notices_held<'event>(
    host: &impl CentralStore,
    workspace: &WorkspaceId,
    cancellations: impl IntoIterator<Item = (&'event ThreadId, Option<&'event ActorIdentityId>)> + Send,
    now: Timestamp,
) -> Result<Vec<GuardedStatement>, StoreFailure> {
    let rows = host.read(notice_messages()).await?;
    let mut notices = decode_messages(&rows)?
        .into_iter()
        .zip(&rows.rows)
        .map(|(notice, raw)| Ok((notice, string(&rows, raw, "body")?.to_owned())))
        .collect::<Result<Vec<_>, StoreFailure>>()?;
    let mut statements = Vec::new();

    for (thread, recipient) in cancellations {
        for (notice, previous_body) in &mut notices {
            if notice.state != MessageState::Queued {
                continue;
            }
            let source = notice
                .source
                .as_mut()
                .ok_or_else(|| corrupt("mail_messages.source_change_id"))?;
            if recipient.is_some_and(|recipient| recipient != &source.recipient_actor) {
                continue;
            }
            let MessageBody::Notice { items } = &mut notice.body else {
                return Err(corrupt("mail_messages.body"));
            };
            let item_count = items.len();
            items.retain(|item| {
                &item.workspace_id != workspace || item.thread_id.as_ref() != Some(thread)
            });
            if items.len() == item_count {
                continue;
            }

            let next_state = if let Some(first) = items.first() {
                source.change_id = first.change_id.clone();
                source.workspace_id = first.workspace_id.clone();
                source.thread_id = first.thread_id.clone();
                "queued"
            } else {
                notice.state = MessageState::Cancelled;
                "cancelled"
            };
            let body =
                serde_json::to_string(&notice.body).map_err(|_| corrupt("mail_messages.body"))?;
            let prior = std::mem::replace(previous_body, body.clone());
            macro_rules! guarded_update {
                ($file:literal, $body:expr, $state:expr, $change:expr, $workspace:expr, $thread:expr, $updated:expr, $key:expr, $prior:expr) => {
                    exact(
                        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", $file)),
                        vec![
                            text($body),
                            text($state),
                            text($change),
                            text($workspace),
                            $thread.map_or(SqlValue::Null, text),
                            text($updated),
                            text($key),
                            text($prior),
                        ],
                    )
                };
            }
            statements.push(update_notice!(
                guarded_update,
                body,
                next_state,
                &source.change_id,
                &source.workspace_id,
                source.thread_id.as_ref(),
                format_utc_millis(now),
                &notice.message_id,
                prior
            ));
            if notice.state == MessageState::Cancelled {
                statements.extend(plan_due_table_change(
                    &DueTableChange::RemoveItem(DueItemKey {
                        kind: DueKind::SystemMessageExpiry,
                        subject: central_engine_model::InstanceKey(notice.message_id.to_string()),
                    }),
                    now,
                )?);
            }
        }
    }
    Ok(statements)
}

#[cfg(test)]
#[path = "queued_notices_test.rs"]
mod tests;
