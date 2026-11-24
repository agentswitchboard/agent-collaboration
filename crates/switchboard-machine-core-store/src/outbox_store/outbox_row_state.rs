//! Decoded row state and the payload each durable stage permits.
use crate::{
    OutboxAccepted, OutboxCancellation, OutboxQueuedWrite, OutboxRefusal, OutboxRequestStatus,
    OutboxScope, outbox_item_state::OutboxItemState, outbox_receipt_profile::AnsweredWrite,
    outbox_settlement::SettledWrite,
};
use state_machine_core::StateTag;
use std::sync::Arc;
use workspace_ids::RequestId;
use workspace_model::{Caller, Timestamp};

#[derive(Clone)]
pub(super) struct OutboxRowState {
    pub(super) request: RequestId,
    pub(super) caller: Caller,
    pub(super) sent_at: Timestamp,
    pub(super) flush_at_ms: i64,
    pub(super) deadline_ms: i64,
    pub(super) updated_at_ms: i64,
    pub(super) phase: OutboxPhase,
}

// Cloning a snapshot for the pure reducer shares the queued body. Claiming
// can then move the original body out once the temporary snapshot is dropped.
#[derive(Clone)]
pub(super) enum OutboxPhase {
    InOutbox(Arc<OutboxQueuedWrite>),
    SentAnswerLost(Arc<OutboxQueuedWrite>),
    Sent(Arc<OutboxQueuedWrite>),
    Resent(Arc<OutboxQueuedWrite>),
    Answered(AnsweredWrite, OutboxAccepted),
    Refused(AnsweredWrite, OutboxRefusal),
    NotSent(AnsweredWrite),
    Unconfirmed(AnsweredWrite, Option<SettledWrite>),
}

impl OutboxPhase {
    pub(super) const fn item_state(&self) -> OutboxItemState {
        match self {
            Self::InOutbox(_) => OutboxItemState::InOutbox,
            Self::SentAnswerLost(_) => OutboxItemState::SentAnswerLost,
            Self::Sent(_) => OutboxItemState::Sent,
            Self::Resent(_) => OutboxItemState::Resent,
            Self::Answered(..) => OutboxItemState::Answered,
            Self::Refused(..) => OutboxItemState::Refused,
            Self::NotSent(_) => OutboxItemState::NotSent,
            Self::Unconfirmed(..) => OutboxItemState::Unconfirmed,
        }
    }

    pub(super) fn profile(&self) -> Option<&AnsweredWrite> {
        match self {
            Self::Answered(profile, _)
            | Self::Refused(profile, _)
            | Self::NotSent(profile)
            | Self::Unconfirmed(profile, _) => Some(profile),
            Self::InOutbox(_) | Self::SentAnswerLost(_) | Self::Sent(_) | Self::Resent(_) => None,
        }
    }

    pub(super) fn into_queued(self) -> Option<OutboxQueuedWrite> {
        match self {
            Self::InOutbox(write)
            | Self::SentAnswerLost(write)
            | Self::Sent(write)
            | Self::Resent(write) => Some(Arc::unwrap_or_clone(write)),
            Self::Answered(..) | Self::Refused(..) | Self::NotSent(_) | Self::Unconfirmed(..) => {
                None
            }
        }
    }

    pub(super) fn request_scope(&self) -> OutboxScope {
        match self {
            Self::InOutbox(write)
            | Self::SentAnswerLost(write)
            | Self::Sent(write)
            | Self::Resent(write) => write.request_scope(),
            Self::Answered(profile, _)
            | Self::Refused(profile, _)
            | Self::NotSent(profile)
            | Self::Unconfirmed(profile, _) => profile.target.request_scope(),
        }
    }
}

impl OutboxRowState {
    pub(super) fn status(&self) -> OutboxRequestStatus {
        match &self.phase {
            OutboxPhase::InOutbox(_) | OutboxPhase::SentAnswerLost(_) => {
                OutboxRequestStatus::InOutbox {
                    flush_at_ms: self.flush_at_ms,
                }
            }
            OutboxPhase::Sent(_) | OutboxPhase::Resent(_) => OutboxRequestStatus::Sent,
            OutboxPhase::Answered(_, reply)
            | OutboxPhase::Unconfirmed(_, Some(SettledWrite::Answered(reply))) => {
                OutboxRequestStatus::Answered(reply.clone())
            }
            OutboxPhase::Refused(_, reason)
            | OutboxPhase::Unconfirmed(_, Some(SettledWrite::Refused(reason))) => {
                OutboxRequestStatus::Refused(reason.clone())
            }
            OutboxPhase::NotSent(_) => OutboxRequestStatus::Ended {
                reason: OutboxCancellation::NotSentWithin15Minutes,
            },
            OutboxPhase::Unconfirmed(_, None) => OutboxRequestStatus::Ended {
                reason: OutboxCancellation::NotConfirmedWithin15Minutes,
            },
        }
    }
}

impl StateTag for OutboxRowState {
    fn state_tag(&self) -> &'static str {
        self.phase.item_state().as_str()
    }

    fn is_terminal(&self) -> bool {
        ends_sending(self.phase.item_state())
    }
}

pub(super) const fn ends_sending(state: OutboxItemState) -> bool {
    match state {
        OutboxItemState::Answered
        | OutboxItemState::Refused
        | OutboxItemState::NotSent
        | OutboxItemState::Unconfirmed => true,
        OutboxItemState::InOutbox
        | OutboxItemState::SentAnswerLost
        | OutboxItemState::Sent
        | OutboxItemState::Resent => false,
    }
}
