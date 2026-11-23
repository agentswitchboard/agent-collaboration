//! Validated permanent outcomes for claimed writes.
use crate::{
    LocalOutboxRefusal, OutboxAccepted, OutboxQueuedWrite, OutboxRefusal, OutboxWriteTarget,
};
use central_link_protocol::{ReplyPayload, RequestRefusal};
use workspace_ids::RequestId;
use workspace_model::{
    ItemRef, RefusalReason, WorkspaceAnswer, WorkspaceResult,
    central_mail::{CentralMailAnswer, CentralMailRefusal, CentralMailResult},
};

/// A terminal result the outbox can persist after a claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxWriteOutcome {
    request_id: RequestId,
    target: OutboxWriteTarget,
    settlement: SettledWrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SettledWrite {
    Answered(OutboxAccepted),
    Refused(OutboxRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("outbox outcome is not a permanent result for the claimed operation")]
pub struct InvalidOutboxOutcome;

impl OutboxWriteOutcome {
    pub fn from_reply(
        write: &OutboxQueuedWrite,
        reply: ReplyPayload,
    ) -> Result<Self, InvalidOutboxOutcome> {
        let request_id = write.request_id().ok_or(InvalidOutboxOutcome)?.clone();
        let target = write.content().ok_or(InvalidOutboxOutcome)?.target;
        let refusal = match &reply {
            ReplyPayload::Workspace(reply) => match &reply.result {
                WorkspaceResult::Refused(reason) => Some(RequestRefusal::Workspace(reason.clone())),
                WorkspaceResult::NotFound { workspace_id, item } => {
                    if !matches!(&target, OutboxWriteTarget::Thread {
                        workspace_id: Some(claimed_workspace), ..
                    } if claimed_workspace == workspace_id)
                    {
                        return Err(InvalidOutboxOutcome);
                    }
                    Some(RequestRefusal::Workspace(RefusalReason::NotFound {
                        item: item.clone(),
                    }))
                }
                _ => None,
            },
            ReplyPayload::Mail(CentralMailResult::Refused(reason)) => {
                Some(RequestRefusal::Mail(reason.clone()))
            }
            ReplyPayload::Mail(_) => None,
        };
        let settlement = match refusal {
            Some(reason) => checked_refusal(&target, OutboxRefusal::Central(reason))?,
            None => {
                SettledWrite::Answered(accepted_reply(&target, reply).ok_or(InvalidOutboxOutcome)?)
            }
        };
        Ok(Self {
            request_id,
            target,
            settlement,
        })
    }

    pub fn from_refusal(
        write: &OutboxQueuedWrite,
        refusal: OutboxRefusal,
    ) -> Result<Self, InvalidOutboxOutcome> {
        let request_id = write.request_id().ok_or(InvalidOutboxOutcome)?.clone();
        let target = write.content().ok_or(InvalidOutboxOutcome)?.target;
        let settlement = checked_refusal(&target, refusal)?;
        Ok(Self {
            request_id,
            target,
            settlement,
        })
    }

    pub(crate) fn for_claim(
        &self,
        request_id: &RequestId,
        target: &OutboxWriteTarget,
    ) -> Result<&SettledWrite, InvalidOutboxOutcome> {
        // The target encodes the operation and the owning scope.
        (self.request_id == *request_id && self.target == *target)
            .then_some(&self.settlement)
            .ok_or(InvalidOutboxOutcome)
    }
}

fn checked_refusal(
    target: &OutboxWriteTarget,
    refusal: OutboxRefusal,
) -> Result<SettledWrite, InvalidOutboxOutcome> {
    if is_permanent_refusal(target, &refusal) {
        Ok(SettledWrite::Refused(refusal))
    } else {
        Err(InvalidOutboxOutcome)
    }
}

fn accepted_reply(target: &OutboxWriteTarget, reply: ReplyPayload) -> Option<OutboxAccepted> {
    let answer = match reply {
        ReplyPayload::Workspace(reply) => match reply.result {
            WorkspaceResult::Done(WorkspaceAnswer::Created {
                item: ItemRef::Post(post_id),
                position,
            }) => OutboxAccepted::ThreadPost {
                post_id,
                position,
                actors: reply.actors,
            },
            _ => return None,
        },
        ReplyPayload::Mail(CentralMailResult::Done(CentralMailAnswer::Queued { message_id })) => {
            OutboxAccepted::SendMessage { message_id }
        }
        ReplyPayload::Mail(_) => return None,
    };
    is_permanent_answer(target, &answer).then_some(answer)
}

pub(crate) fn is_permanent_answer(target: &OutboxWriteTarget, answer: &OutboxAccepted) -> bool {
    matches!(
        (target, answer),
        (
            OutboxWriteTarget::Thread { .. },
            OutboxAccepted::ThreadPost { .. }
        ) | (
            OutboxWriteTarget::Session { .. },
            OutboxAccepted::SendMessage { .. }
        )
    )
}

pub(crate) fn is_permanent_refusal(target: &OutboxWriteTarget, refusal: &OutboxRefusal) -> bool {
    match (target, refusal) {
        (_, OutboxRefusal::Local(LocalOutboxRefusal::RequestIdConflict)) => true,
        (
            OutboxWriteTarget::Thread { thread_id, .. },
            OutboxRefusal::Central(RequestRefusal::Workspace(reason)),
        ) => match reason {
            RefusalReason::RequestIdReused
            | RefusalReason::WorkspaceNotAllowed
            | RefusalReason::UnknownActor { .. }
            | RefusalReason::UnheldAddressee
            | RefusalReason::InvalidInput { .. } => true,
            RefusalReason::ThreadResolved { thread_id: target }
            | RefusalReason::NotMember { thread_id: target }
            | RefusalReason::NotParticipant { thread_id: target }
            | RefusalReason::NotFound {
                item: ItemRef::Thread(target),
            } => thread_id == target,
            _ => false,
        },
        (
            OutboxWriteTarget::Session { to },
            OutboxRefusal::Central(RequestRefusal::Mail(reason)),
        ) => match reason {
            CentralMailRefusal::UnknownSession { target } => to == target,
            CentralMailRefusal::TimeOutOfRange { field } => field == "anchorAt",
            CentralMailRefusal::Workspace(
                RefusalReason::RequestIdReused | RefusalReason::WorkspaceNotAllowed,
            ) => true,
            _ => false,
        },
        _ => false,
    }
}
