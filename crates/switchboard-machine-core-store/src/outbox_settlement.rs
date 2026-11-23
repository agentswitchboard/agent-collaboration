//! Validated permanent outcomes for claimed writes.
use crate::{LocalOutboxRefusal, OutboxQueuedWrite, OutboxRefusal, OutboxWriteTarget};
use central_link_protocol::{ReplyPayload, RequestRefusal};
use workspace_model::{
    ItemRef, RefusalReason, WorkspaceAnswer, WorkspaceResult,
    central_mail::{CentralMailAnswer, CentralMailRefusal, CentralMailResult},
};

/// A terminal result the outbox can persist after a claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxWriteOutcome(SettledWrite);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SettledWrite {
    Answered(ReplyPayload),
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
        let target = write.content().ok_or(InvalidOutboxOutcome)?.target;
        let refusal = match &reply {
            ReplyPayload::Workspace(reply) => match &reply.result {
                WorkspaceResult::Refused(reason) => Some(RequestRefusal::Workspace(reason.clone())),
                WorkspaceResult::NotFound { item, .. } => {
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
        match refusal {
            Some(reason) => Self::from_refusal(write, OutboxRefusal::Central(reason)),
            None if is_permanent_answer(&target, &reply) => Ok(Self(SettledWrite::Answered(reply))),
            None => Err(InvalidOutboxOutcome),
        }
    }

    pub fn from_refusal(
        write: &OutboxQueuedWrite,
        refusal: OutboxRefusal,
    ) -> Result<Self, InvalidOutboxOutcome> {
        let target = write.content().ok_or(InvalidOutboxOutcome)?.target;
        if !is_permanent_refusal(&target, &refusal) {
            return Err(InvalidOutboxOutcome);
        }
        Ok(Self(SettledWrite::Refused(refusal)))
    }

    pub(crate) fn for_target(
        &self,
        target: &OutboxWriteTarget,
    ) -> Result<&SettledWrite, InvalidOutboxOutcome> {
        let matches = match &self.0 {
            SettledWrite::Answered(reply) => is_permanent_answer(target, reply),
            SettledWrite::Refused(refusal) => is_permanent_refusal(target, refusal),
        };
        matches.then_some(&self.0).ok_or(InvalidOutboxOutcome)
    }
}

pub(crate) fn is_permanent_answer(target: &OutboxWriteTarget, reply: &ReplyPayload) -> bool {
    match (target, reply) {
        (OutboxWriteTarget::Thread { .. }, ReplyPayload::Workspace(reply)) => matches!(
            reply.result,
            WorkspaceResult::Done(WorkspaceAnswer::Created {
                item: ItemRef::Post(_),
                ..
            })
        ),
        (OutboxWriteTarget::Session { .. }, ReplyPayload::Mail(reply)) => {
            matches!(
                reply,
                CentralMailResult::Done(CentralMailAnswer::Queued { .. })
            )
        }
        _ => false,
    }
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
