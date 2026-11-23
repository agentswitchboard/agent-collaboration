//! Validated permanent outcomes for claimed writes.
use crate::{
    LocalOutboxRefusal, OutboxAccepted, OutboxCentralRefusal, OutboxQueuedWrite, OutboxRefusal,
    OutboxScope, OutboxWriteTarget,
};
use workspace_ids::RequestId;
use workspace_model::{ItemRef, RefusalReason, central_mail::CentralMailRefusal};

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
    pub fn from_accepted(
        write: &OutboxQueuedWrite,
        accepted: OutboxAccepted,
    ) -> Result<Self, InvalidOutboxOutcome> {
        Self::bind(write, |target| {
            is_permanent_answer(target, &accepted)
                .then_some(SettledWrite::Answered(accepted))
                .ok_or(InvalidOutboxOutcome)
        })
    }

    pub fn from_refusal(
        write: &OutboxQueuedWrite,
        refusal: OutboxRefusal,
    ) -> Result<Self, InvalidOutboxOutcome> {
        if matches!(
            refusal,
            OutboxRefusal::Central(OutboxCentralRefusal::Workspace(
                RefusalReason::NotFound { .. }
            ))
        ) {
            return Err(InvalidOutboxOutcome);
        }
        Self::bind(write, |target| checked_refusal(target, refusal))
    }

    /// Validates the reported scope before retaining the native NotFound reason.
    pub fn from_workspace_not_found(
        write: &OutboxQueuedWrite,
        scope: &OutboxScope,
        item: ItemRef,
    ) -> Result<Self, InvalidOutboxOutcome> {
        Self::bind(write, |target| {
            if target.request_scope() != *scope {
                return Err(InvalidOutboxOutcome);
            }
            checked_refusal(
                target,
                OutboxRefusal::Central(OutboxCentralRefusal::Workspace(RefusalReason::NotFound {
                    item,
                })),
            )
        })
    }

    fn bind(
        write: &OutboxQueuedWrite,
        settle: impl FnOnce(&OutboxWriteTarget) -> Result<SettledWrite, InvalidOutboxOutcome>,
    ) -> Result<Self, InvalidOutboxOutcome> {
        let request_id = write.request_id().ok_or(InvalidOutboxOutcome)?.clone();
        let target = write.content().ok_or(InvalidOutboxOutcome)?.target;
        let settlement = settle(&target)?;
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
            OutboxRefusal::Central(OutboxCentralRefusal::Workspace(reason)),
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
            OutboxRefusal::Central(OutboxCentralRefusal::Mail(reason)),
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
