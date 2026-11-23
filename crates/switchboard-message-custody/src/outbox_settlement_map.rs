//! Translate a checked link reply into the owning store's settlement vocabulary.

use central_link_protocol::{ReplyPayload, RequestRefusal};
use switchboard_machine_core_store::{
    InvalidOutboxOutcome, OutboxAccepted, OutboxCentralRefusal, OutboxQueuedWrite, OutboxRefusal,
    OutboxScope, OutboxWriteOutcome,
};
use workspace_model::{
    ItemRef, RefusalReason, WorkspaceAnswer, WorkspaceReply, WorkspaceResult,
    central_mail::{CentralMailAnswer, CentralMailResult},
};

pub(crate) fn outcome_from_link_reply(
    write: &OutboxQueuedWrite,
    reply: ReplyPayload,
) -> Result<OutboxWriteOutcome, InvalidOutboxOutcome> {
    match reply {
        ReplyPayload::Workspace(WorkspaceReply { result, actors }) => match result {
            WorkspaceResult::Done(WorkspaceAnswer::Created {
                item: ItemRef::Post(post_id),
                position,
            }) => OutboxWriteOutcome::from_accepted(
                write,
                OutboxAccepted::ThreadPost {
                    post_id,
                    position,
                    actors,
                },
            ),
            WorkspaceResult::NotFound { workspace_id, item } => {
                OutboxWriteOutcome::from_workspace_not_found(
                    write,
                    &OutboxScope::Workspace(workspace_id),
                    item,
                )
            }
            WorkspaceResult::Refused(RefusalReason::NotFound { item }) => {
                OutboxWriteOutcome::from_workspace_not_found(write, &write.request_scope(), item)
            }
            WorkspaceResult::Refused(reason) => OutboxWriteOutcome::from_refusal(
                write,
                OutboxRefusal::Central(OutboxCentralRefusal::Workspace(reason)),
            ),
            WorkspaceResult::Done(_)
            | WorkspaceResult::Conflict
            | WorkspaceResult::OutcomeUnknown
            | WorkspaceResult::CentralUnavailable => Err(InvalidOutboxOutcome),
        },
        ReplyPayload::Mail(result) => match result {
            CentralMailResult::Done(CentralMailAnswer::Queued { message_id }) => {
                OutboxWriteOutcome::from_accepted(write, OutboxAccepted::SendMessage { message_id })
            }
            CentralMailResult::Refused(reason) => OutboxWriteOutcome::from_refusal(
                write,
                OutboxRefusal::Central(OutboxCentralRefusal::Mail(reason)),
            ),
            CentralMailResult::Done(_)
            | CentralMailResult::Conflict
            | CentralMailResult::OutcomeUnknown
            | CentralMailResult::CentralUnavailable => Err(InvalidOutboxOutcome),
        },
    }
}

pub(crate) fn central_status_reason(reason: RequestRefusal) -> OutboxRefusal {
    OutboxRefusal::Central(match reason {
        RequestRefusal::Workspace(reason) => OutboxCentralRefusal::Workspace(reason),
        RequestRefusal::Mail(reason) => OutboxCentralRefusal::Mail(reason),
    })
}

pub(crate) fn outcome_from_stored_refusal(
    write: &OutboxQueuedWrite,
    reason: OutboxRefusal,
) -> Result<OutboxWriteOutcome, InvalidOutboxOutcome> {
    match reason {
        OutboxRefusal::Central(OutboxCentralRefusal::Workspace(RefusalReason::NotFound {
            item,
        })) => OutboxWriteOutcome::from_workspace_not_found(write, &write.request_scope(), item),
        reason => OutboxWriteOutcome::from_refusal(write, reason),
    }
}

#[cfg(test)]
#[path = "outbox_settlement_map_test.rs"]
mod tests;
