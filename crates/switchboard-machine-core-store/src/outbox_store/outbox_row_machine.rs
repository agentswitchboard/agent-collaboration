//! The durable transitions of one outbox row.
#![deny(clippy::wildcard_enum_match_arm)]

use super::{
    NewOutboxItem, OutboxInsert, OutboxStoreError,
    outbox_row_state::{OutboxPhase, OutboxRowState, ends_sending},
};
use crate::{
    LocalOutboxRefusal, OutboxAccepted, OutboxCancellation, OutboxQueuedWrite, OutboxWriteOutcome,
    OutboxWriteTarget, StoredSenderOutcome, outbox_item_state::OutboxItemState,
    outbox_receipt_profile::AnsweredWrite, outbox_settlement::SettledWrite,
    sender_outcome::OutcomePrecedence,
};
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};
use std::sync::Arc;
use workspace_ids::RequestId;

#[derive(Clone)]
pub(super) enum OutboxRowEvent {
    Claim {
        now_ms: i64,
        eligible_through_ms: i64,
        required_deadline_after_ms: i64,
    },
    ReturnNotTransmitted {
        now_ms: i64,
    },
    ReturnUnanswered {
        now_ms: i64,
    },
    EndDue {
        now_ms: i64,
    },
    RecordOutcome {
        outcome: OutboxWriteOutcome,
        now_ms: i64,
    },
    RecordLateOutcome {
        outcome: OutboxWriteOutcome,
    },
    SenderDelivery {
        notice: StoredSenderOutcome,
        now_ms: i64,
    },
    Prune {
        cutoff_ms: i64,
    },
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum OutboxRowEffect {
    Stamp,
    Finish { reason: OutboxCancellation },
    StoreOutcome,
    StoreLateOutcome,
    StoreSenderOutcome,
    EnrichSenderProfile,
    SenderNoticeUnchanged { retained: bool },
    Delete,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum OutboxRowRefusal {
    NotDue,
    CorruptPayload,
    Digest,
    InvalidOutcome,
    LateOutcomeForNotSent,
    ContradictoryFinalOutcome,
    DeliveryOutcomeMismatch,
}

pub(super) struct OutboxRowMachine;

const CLAIM: &str = "claim";
const RETURN_NOT_TRANSMITTED: &str = "returnNotTransmitted";
const RETURN_UNANSWERED: &str = "returnUnanswered";
const END_DUE: &str = "endDue";
const RECORD_OUTCOME: &str = "recordOutcome";
const RECORD_LATE_OUTCOME: &str = "recordLateOutcome";
const SENDER_DELIVERY: &str = "senderDelivery";
const PRUNE: &str = "prune";

const TRANSITIONS: &[Transition] = {
    use OutboxItemState::{
        Answered, InOutbox, NotSent, Refused, Resent, Sent, SentAnswerLost, Unconfirmed,
    };
    &[
        edge(InOutbox, CLAIM, Sent),
        edge(SentAnswerLost, CLAIM, Resent),
        // A proven untouched first attempt can return to InOutbox at cutoff.
        edge(Sent, RETURN_NOT_TRANSMITTED, InOutbox),
        edge(Resent, RETURN_NOT_TRANSMITTED, SentAnswerLost),
        // Inconclusive live results return the claim. An inconclusive orphan
        // read leaves it alone, so the store receives no event.
        edge(Sent, RETURN_UNANSWERED, SentAnswerLost),
        edge(Resent, RETURN_UNANSWERED, SentAnswerLost),
        // At or past cutoff, unanswered claims whose frame may have reached Central end unconfirmed.
        edge(Sent, RETURN_UNANSWERED, Unconfirmed),
        edge(Resent, RETURN_UNANSWERED, Unconfirmed),
        edge(InOutbox, END_DUE, NotSent),
        edge(SentAnswerLost, END_DUE, Unconfirmed),
        edge(Sent, END_DUE, Unconfirmed),
        edge(Resent, END_DUE, Unconfirmed),
        edge(Sent, RECORD_OUTCOME, Answered),
        edge(Resent, RECORD_OUTCOME, Answered),
        edge(Sent, RECORD_OUTCOME, Refused),
        edge(Resent, RECORD_OUTCOME, Refused),
        edge(Unconfirmed, RECORD_LATE_OUTCOME, Unconfirmed),
        edge(Sent, SENDER_DELIVERY, Answered),
        edge(Resent, SENDER_DELIVERY, Answered),
        edge(SentAnswerLost, SENDER_DELIVERY, Answered),
        // If a notice proves acceptance first, the row is Answered and
        // retention starts at the notice. After a late acceptance it
        // remains Unconfirmed and retains the cutoff stamp.
        edge(Unconfirmed, SENDER_DELIVERY, Answered),
        edge(Unconfirmed, SENDER_DELIVERY, Unconfirmed),
        edge(Answered, SENDER_DELIVERY, Answered),
        // These effects delete the row. Its last state remains the snapshot
        // for the checker, while absence is a storage boundary.
        edge(Answered, PRUNE, Answered),
        edge(Refused, PRUNE, Refused),
        edge(NotSent, PRUNE, NotSent),
        edge(Unconfirmed, PRUNE, Unconfirmed),
    ]
};

const fn edge(from: OutboxItemState, event: &'static str, to: OutboxItemState) -> Transition {
    Transition {
        from: from.as_str(),
        event,
        to: to.as_str(),
        terminal: ends_sending(to),
    }
}

impl EventTag for OutboxRowEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Claim { .. } => CLAIM,
            Self::ReturnNotTransmitted { .. } => RETURN_NOT_TRANSMITTED,
            Self::ReturnUnanswered { .. } => RETURN_UNANSWERED,
            Self::EndDue { .. } => END_DUE,
            Self::RecordOutcome { .. } => RECORD_OUTCOME,
            Self::RecordLateOutcome { .. } => RECORD_LATE_OUTCOME,
            Self::SenderDelivery { .. } => SENDER_DELIVERY,
            Self::Prune { .. } => PRUNE,
        }
    }
}

impl RefusalTag for OutboxRowRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::NotDue => "notDue",
            Self::CorruptPayload => "corruptPayload",
            Self::Digest => "digest",
            Self::InvalidOutcome => "invalidOutcome",
            Self::LateOutcomeForNotSent => "lateOutcomeForNotSent",
            Self::ContradictoryFinalOutcome => "contradictoryFinalOutcome",
            Self::DeliveryOutcomeMismatch => "deliveryOutcomeMismatch",
        }
    }
}

impl OutboxRowMachine {
    pub(super) const INITIAL_STATE: OutboxItemState = OutboxItemState::InOutbox;

    pub(super) fn replay_insert(
        state: &OutboxRowState,
        item: &NewOutboxItem,
    ) -> Result<OutboxInsert, OutboxStoreError> {
        use OutboxPhase::{
            Answered, InOutbox, NotSent, Refused, Resent, Sent, SentAnswerLost, Unconfirmed,
        };
        let same_write = match &state.phase {
            InOutbox(write) | SentAnswerLost(write) | Sent(write) | Resent(write) => {
                **write == item.payload
            }
            Answered(profile, _)
            | Refused(profile, _)
            | NotSent(profile)
            | Unconfirmed(profile, _) => {
                item.payload
                    .content()
                    .is_some_and(|content| content.target == profile.target)
                    && item
                        .payload
                        .content_digest()
                        .map_err(|_| OutboxStoreError::Digest)?
                        == profile.content_digest
            }
        };
        Ok(if state.caller == item.caller && same_write {
            OutboxInsert::AlreadyStored(state.status())
        } else {
            OutboxInsert::Refused(LocalOutboxRefusal::RequestIdConflict)
        })
    }
}

impl StateMachine for OutboxRowMachine {
    type State = OutboxRowState;
    type Event = OutboxRowEvent;
    type Effect = OutboxRowEffect;
    type Error = OutboxRowRefusal;

    fn transition(state: &Self::State, event: Self::Event) -> StateMachineResult<Self> {
        use OutboxCancellation::{NotConfirmedWithin15Minutes, NotSentWithin15Minutes};
        use OutboxPhase::{
            Answered, InOutbox, NotSent, Refused, Resent, Sent, SentAnswerLost, Unconfirmed,
        };
        use OutboxRowEvent::{
            Claim, EndDue, Prune, RecordLateOutcome, RecordOutcome, ReturnNotTransmitted,
            ReturnUnanswered, SenderDelivery,
        };

        let not_allowed = TransitionError::NotAllowed {
            state: state.state_tag(),
            event: event.event_tag(),
        };
        let not_due = TransitionError::Domain(OutboxRowRefusal::NotDue);
        match (&state.phase, event) {
            (
                phase @ (InOutbox(write) | SentAnswerLost(write)),
                Claim {
                    now_ms,
                    eligible_through_ms,
                    required_deadline_after_ms,
                },
            ) => {
                if state.flush_at_ms > eligible_through_ms
                    || state.deadline_ms <= required_deadline_after_ms
                {
                    return Err(not_due);
                }
                let claimed = if matches!(phase, InOutbox(_)) {
                    Sent(Arc::clone(write))
                } else {
                    Resent(Arc::clone(write))
                };
                Ok(restamp(state, claimed, now_ms, OutboxRowEffect::Stamp))
            }
            (Sent(write), ReturnNotTransmitted { now_ms }) => Ok(restamp(
                state,
                InOutbox(Arc::clone(write)),
                now_ms,
                OutboxRowEffect::Stamp,
            )),
            (Resent(write), ReturnNotTransmitted { now_ms }) => Ok(restamp(
                state,
                SentAnswerLost(Arc::clone(write)),
                now_ms,
                OutboxRowEffect::Stamp,
            )),
            (Sent(write) | Resent(write), ReturnUnanswered { now_ms })
                if now_ms < state.deadline_ms =>
            {
                Ok(restamp(
                    state,
                    SentAnswerLost(Arc::clone(write)),
                    now_ms,
                    OutboxRowEffect::Stamp,
                ))
            }
            (Sent(write) | Resent(write), ReturnUnanswered { now_ms }) => {
                finish(state, write, now_ms, NotConfirmedWithin15Minutes)
            }
            (
                phase @ (InOutbox(write) | SentAnswerLost(write) | Sent(write) | Resent(write)),
                EndDue { now_ms },
            ) => {
                if now_ms < state.deadline_ms {
                    return Err(not_due);
                }
                let reason = if matches!(phase, InOutbox(_)) {
                    NotSentWithin15Minutes
                } else {
                    NotConfirmedWithin15Minutes
                };
                finish(state, write, now_ms, reason)
            }
            (Sent(write) | Resent(write), RecordOutcome { outcome, now_ms }) => {
                let target = write_target(write)?;
                let settlement = outcome
                    .for_claim(&state.request, &target)
                    .map_err(|_| TransitionError::Domain(OutboxRowRefusal::InvalidOutcome))?;
                let profile = body_free_profile(&state.request, write, target)?;
                let next = match settlement {
                    SettledWrite::Answered(reply) => Answered(profile, reply.clone()),
                    SettledWrite::Refused(reason) => Refused(profile, reason.clone()),
                };
                Ok(restamp(state, next, now_ms, OutboxRowEffect::StoreOutcome))
            }
            (Unconfirmed(profile, previous), RecordLateOutcome { outcome }) => {
                let settlement = outcome
                    .for_ended(&state.request, &profile.target, &profile.content_digest)
                    .map_err(|_| TransitionError::Domain(OutboxRowRefusal::InvalidOutcome))?;
                match previous {
                    Some(previous) if previous == settlement => Ok((state.clone(), Vec::new())),
                    Some(_) => Err(TransitionError::Domain(
                        OutboxRowRefusal::ContradictoryFinalOutcome,
                    )),
                    None => Ok(restamp(
                        state,
                        Unconfirmed(profile.clone(), Some(settlement.clone())),
                        state.updated_at_ms,
                        OutboxRowEffect::StoreLateOutcome,
                    )),
                }
            }
            (NotSent(_), RecordLateOutcome { .. }) => Err(TransitionError::Domain(
                OutboxRowRefusal::LateOutcomeForNotSent,
            )),
            (
                Sent(write) | Resent(write) | SentAnswerLost(write),
                SenderDelivery { notice, now_ms },
            ) => {
                let profile = body_free_profile(&state.request, write, write_target(write)?)?;
                apply_sender_notice(state, profile, None, notice, now_ms)
            }
            (Unconfirmed(profile, None), SenderDelivery { notice, now_ms }) => {
                apply_sender_notice(state, profile.clone(), None, notice, now_ms)
            }
            (
                Answered(profile, reply)
                | Unconfirmed(profile, Some(SettledWrite::Answered(reply))),
                SenderDelivery { notice, now_ms },
            ) => apply_sender_notice(state, profile.clone(), Some(reply), notice, now_ms),
            (
                InOutbox(_)
                | Refused(..)
                | NotSent(_)
                | Unconfirmed(_, Some(SettledWrite::Refused(_))),
                SenderDelivery { .. },
            ) => Err(TransitionError::Domain(
                OutboxRowRefusal::DeliveryOutcomeMismatch,
            )),
            (Answered(..) | Refused(..) | NotSent(_) | Unconfirmed(..), Prune { cutoff_ms }) => {
                if state.updated_at_ms > cutoff_ms {
                    return Err(not_due);
                }
                Ok((state.clone(), vec![OutboxRowEffect::Delete]))
            }
            (Sent(_) | Resent(_), Claim { .. })
            | (
                InOutbox(_) | SentAnswerLost(_),
                ReturnNotTransmitted { .. } | ReturnUnanswered { .. } | RecordOutcome { .. },
            )
            | (
                InOutbox(_) | SentAnswerLost(_) | Sent(_) | Resent(_) | Answered(..) | Refused(..),
                RecordLateOutcome { .. },
            )
            | (InOutbox(_) | SentAnswerLost(_) | Sent(_) | Resent(_), Prune { .. })
            | (
                Answered(..) | Refused(..) | NotSent(_) | Unconfirmed(..),
                Claim { .. }
                | ReturnNotTransmitted { .. }
                | ReturnUnanswered { .. }
                | EndDue { .. }
                | RecordOutcome { .. },
            ) => Err(not_allowed),
        }
    }

    fn transitions() -> &'static [Transition] {
        TRANSITIONS
    }
}

fn restamp(
    state: &OutboxRowState,
    phase: OutboxPhase,
    updated_at_ms: i64,
    effect: OutboxRowEffect,
) -> (OutboxRowState, Vec<OutboxRowEffect>) {
    let next = OutboxRowState {
        phase,
        updated_at_ms,
        ..state.clone()
    };
    (next, vec![effect])
}

fn finish(
    state: &OutboxRowState,
    write: &OutboxQueuedWrite,
    now_ms: i64,
    reason: OutboxCancellation,
) -> StateMachineResult<OutboxRowMachine> {
    let profile = body_free_profile(&state.request, write, write_target(write)?)?;
    let next = match reason {
        OutboxCancellation::NotSentWithin15Minutes => OutboxPhase::NotSent(profile),
        OutboxCancellation::NotConfirmedWithin15Minutes => OutboxPhase::Unconfirmed(profile, None),
    };
    Ok(restamp(
        state,
        next,
        now_ms,
        OutboxRowEffect::Finish { reason },
    ))
}

fn write_target(
    write: &OutboxQueuedWrite,
) -> Result<OutboxWriteTarget, TransitionError<OutboxRowRefusal>> {
    write
        .content()
        .map(|content| content.target)
        .ok_or(TransitionError::Domain(OutboxRowRefusal::CorruptPayload))
}

fn body_free_profile(
    request: &RequestId,
    write: &OutboxQueuedWrite,
    target: OutboxWriteTarget,
) -> Result<AnsweredWrite, TransitionError<OutboxRowRefusal>> {
    Ok(AnsweredWrite {
        request_id: request.clone(),
        target,
        content_digest: write
            .content_digest()
            .map_err(|_| TransitionError::Domain(OutboxRowRefusal::Digest))?,
        delivery_outcome: None,
    })
}

fn apply_sender_notice(
    state: &OutboxRowState,
    mut profile: AnsweredWrite,
    known_acceptance: Option<&OutboxAccepted>,
    notice: StoredSenderOutcome,
    now_ms: i64,
) -> StateMachineResult<OutboxRowMachine> {
    let mismatch = TransitionError::Domain(OutboxRowRefusal::DeliveryOutcomeMismatch);
    if notice.request_id != state.request {
        return Err(mismatch);
    }
    let acceptance = match known_acceptance {
        Some(reply @ OutboxAccepted::SendMessage { message_id })
            if *message_id == notice.message_id =>
        {
            reply.clone()
        }
        Some(_) => return Err(mismatch),
        None => OutboxAccepted::SendMessage {
            message_id: notice.message_id.clone(),
        },
    };
    if !matches!(profile.target, OutboxWriteTarget::Session { .. }) {
        return Err(mismatch);
    }
    match profile.delivery_outcome.as_ref() {
        Some(previous) if previous == &notice => {
            return Ok((
                state.clone(),
                vec![OutboxRowEffect::SenderNoticeUnchanged { retained: true }],
            ));
        }
        Some(previous) if previous.inbox_message_id == notice.inbox_message_id => {
            return Err(mismatch);
        }
        Some(previous) => match notice.precedence_over(previous) {
            OutcomePrecedence::Keep => {
                return Ok((
                    state.clone(),
                    vec![OutboxRowEffect::SenderNoticeUnchanged { retained: false }],
                ));
            }
            OutcomePrecedence::Replace => {}
            OutcomePrecedence::ContradictoryFinal => {
                return Err(TransitionError::Domain(
                    OutboxRowRefusal::ContradictoryFinalOutcome,
                ));
            }
        },
        None => {}
    }
    profile.delivery_outcome = Some(notice);
    if let OutboxPhase::Unconfirmed(_, Some(outcome)) = &state.phase {
        Ok(restamp(
            state,
            OutboxPhase::Unconfirmed(profile, Some(outcome.clone())),
            state.updated_at_ms,
            OutboxRowEffect::EnrichSenderProfile,
        ))
    } else {
        Ok(restamp(
            state,
            OutboxPhase::Answered(profile, acceptance),
            now_ms,
            OutboxRowEffect::StoreSenderOutcome,
        ))
    }
}

#[cfg(test)]
#[path = "outbox_row_machine_test.rs"]
mod tests;
