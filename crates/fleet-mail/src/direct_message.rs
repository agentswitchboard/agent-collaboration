//! Delivery evidence and deadline settlement for one Central direct message.
use super::message_effects::{NotUuidV7, queue_message};
use central_engine_model::{
    Context, EngineTime, EventTag, Machine, MachineKind, RefusalTag, StateTag, Transition,
    TransitionError,
};
use workspace_ids::{MachineId, MessageId, RequestId, SessionRef};
use workspace_model::{Timestamp, central_mail as mail};

pub struct DirectMessage;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DirectMessageSubject {
    pub(crate) id: MessageId,
    pub(crate) sender_session: Option<SessionRef>,
    pub(crate) sender_request_id: RequestId,
    pub(crate) recipient: MachineId,
    pub(crate) cutoff: Timestamp,
    pub(crate) mark: EngineTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FinalReceipt {
    Seen { completed_at: Timestamp },
    Refused { reason: mail::MessageRefusal },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectLifecycle {
    Queued,
    Delivered {
        arrived_at: Timestamp,
    },
    Final {
        receipt: FinalReceipt,
        arrived_at: Option<Timestamp>,
    },
}

impl DirectLifecycle {
    pub(crate) fn project_message_status(self, deadline_passed: bool) -> mail::MessageStatus {
        use mail::MessageStatus;
        match self {
            Self::Queued => MessageStatus::Sent { deadline_passed },
            Self::Delivered { arrived_at } => MessageStatus::Delivered {
                arrived_at,
                deadline_passed,
            },
            Self::Final { receipt, .. } => match receipt {
                FinalReceipt::Seen { completed_at } => MessageStatus::Seen { completed_at },
                FinalReceipt::Refused { reason } => MessageStatus::Refused { reason },
            },
        }
    }

    pub(crate) fn delivered_arrival(self) -> Option<Timestamp> {
        match self {
            Self::Queued => None,
            Self::Delivered { arrived_at } => Some(arrived_at),
            Self::Final { arrived_at, .. } => arrived_at,
        }
    }

    pub(crate) fn storage_tag(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivered { .. } => "delivered",
            Self::Final { receipt, .. } => match receipt {
                FinalReceipt::Seen { .. } => "seen",
                FinalReceipt::Refused { .. } => "refused",
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectReducerState {
    pub(crate) subject: DirectMessageSubject,
    pub(crate) lifecycle: DirectLifecycle,
    pub(crate) deadline_settled_at: Option<Timestamp>,
}

impl StateTag for DirectReducerState {
    fn state_tag(&self) -> &'static str {
        self.lifecycle.storage_tag()
    }

    fn is_terminal(&self) -> bool {
        matches!(self.lifecycle, DirectLifecycle::Final { .. })
    }
}

#[derive(Clone, Debug)]
pub enum DirectMessageEvent {
    Due { occurrence: EngineTime },
    Receipt(mail::ReceiptKind),
}

impl EventTag for DirectMessageEvent {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Due { .. } => "due",
            Self::Receipt(kind) => match kind {
                mail::ReceiptKind::Delivered { .. } => "delivered",
                mail::ReceiptKind::Seen { .. } => "seen",
                mail::ReceiptKind::Refused { .. } => "refused",
            },
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum DirectMessageEffect {
    TellSender(mail::MessageRecord),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DirectMessageRefusal {
    AlreadyRecorded,
    AlreadySettled,
    InvalidIdentifier,
}

impl RefusalTag for DirectMessageRefusal {
    fn refusal_tag(&self) -> &'static str {
        match self {
            Self::AlreadyRecorded => "already_recorded",
            Self::AlreadySettled => "already_settled",
            Self::InvalidIdentifier => "invalid_identifier",
        }
    }
}

impl From<NotUuidV7> for TransitionError<DirectMessageRefusal> {
    fn from(_: NotUuidV7) -> Self {
        Self::Domain(DirectMessageRefusal::InvalidIdentifier)
    }
}

fn tell_sender(
    subject: &DirectMessageSubject,
    outcome: mail::MessageOutcome,
    context: &mut Context<'_>,
) -> Result<Vec<DirectMessageEffect>, NotUuidV7> {
    let Some(session) = &subject.sender_session else {
        return Ok(vec![]);
    };
    let notice = queue_message(
        context,
        session.clone(),
        mail::MessageBody::DeliveryOutcome {
            message_id: subject.id.clone(),
            request_id: subject.sender_request_id.clone(),
            outcome,
        },
    )?;
    Ok(vec![DirectMessageEffect::TellSender(notice)])
}

type LifecycleChange = (DirectLifecycle, Option<mail::MessageOutcome>);

/// Covered receipts short-circuit the applying adapter so a replay writes nothing.
fn apply_receipt(
    prior: &DirectReducerState,
    receipt: mail::ReceiptKind,
) -> Result<LifecycleChange, DirectMessageRefusal> {
    use mail::{MessageOutcome, ReceiptKind};

    let final_receipt = match receipt {
        ReceiptKind::Delivered { arrived_at } => {
            return match prior.lifecycle {
                DirectLifecycle::Queued => Ok((DirectLifecycle::Delivered { arrived_at }, None)),
                DirectLifecycle::Delivered { .. } | DirectLifecycle::Final { .. } => {
                    Err(DirectMessageRefusal::AlreadyRecorded)
                }
            };
        }
        ReceiptKind::Seen { completed_at } => FinalReceipt::Seen { completed_at },
        ReceiptKind::Refused { reason } => FinalReceipt::Refused { reason },
    };

    match prior.lifecycle {
        DirectLifecycle::Queued | DirectLifecycle::Delivered { .. } => {}
        DirectLifecycle::Final { receipt, .. } => {
            return Err(if receipt == final_receipt {
                DirectMessageRefusal::AlreadyRecorded
            } else {
                DirectMessageRefusal::AlreadySettled
            });
        }
    }

    let outcome = match (final_receipt, prior.deadline_settled_at) {
        (FinalReceipt::Refused { reason }, None) => Some(MessageOutcome::Refused { reason }),
        (FinalReceipt::Seen { .. }, None | Some(_)) | (FinalReceipt::Refused { .. }, Some(_)) => {
            None
        }
    };
    Ok((
        DirectLifecycle::Final {
            receipt: final_receipt,
            arrived_at: prior.lifecycle.delivered_arrival(),
        },
        outcome,
    ))
}

const fn direct_transition_edge(
    from: &'static str,
    event: &'static str,
    to: &'static str,
    terminal: bool,
) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal,
    }
}

impl Machine for DirectMessage {
    const KIND: MachineKind = MachineKind("direct_message");
    type State = DirectReducerState;
    type Event = DirectMessageEvent;
    type Effect = DirectMessageEffect;
    type Error = DirectMessageRefusal;

    fn transition(
        state: &DirectReducerState,
        event: DirectMessageEvent,
        context: &mut Context<'_>,
    ) -> Result<(DirectReducerState, Vec<DirectMessageEffect>), TransitionError<DirectMessageRefusal>>
    {
        use mail::MessageOutcome;

        let (lifecycle, deadline_settled_at, outcome) = match event {
            DirectMessageEvent::Receipt(receipt) => {
                let (lifecycle, outcome) =
                    apply_receipt(state, receipt).map_err(TransitionError::Domain)?;
                (lifecycle, state.deadline_settled_at, outcome)
            }
            DirectMessageEvent::Due { occurrence } => {
                if Self::next_due(state) != Some(occurrence) || context.now < state.subject.cutoff {
                    return Err(TransitionError::Stale);
                }
                let outcome = state
                    .lifecycle
                    .delivered_arrival()
                    .map_or(MessageOutcome::DeliveryNotConfirmed, |arrived_at| {
                        MessageOutcome::DeliveredSeenNotConfirmed { arrived_at }
                    });
                (state.lifecycle, Some(context.now), Some(outcome))
            }
        };
        let effects = match outcome {
            Some(outcome) => tell_sender(&state.subject, outcome, context)?,
            None => vec![],
        };
        Ok((
            DirectReducerState {
                subject: state.subject.clone(),
                lifecycle,
                deadline_settled_at,
            },
            effects,
        ))
    }

    fn transitions() -> &'static [Transition] {
        const EDGES: &[Transition] = &[
            direct_transition_edge("queued", "delivered", "delivered", false),
            direct_transition_edge("queued", "seen", "seen", true),
            direct_transition_edge("queued", "refused", "refused", true),
            direct_transition_edge("queued", "due", "queued", false),
            direct_transition_edge("delivered", "seen", "seen", true),
            direct_transition_edge("delivered", "refused", "refused", true),
            direct_transition_edge("delivered", "due", "delivered", false),
        ];
        EDGES
    }

    fn next_due(state: &DirectReducerState) -> Option<EngineTime> {
        match (state.lifecycle, state.deadline_settled_at) {
            (DirectLifecycle::Queued | DirectLifecycle::Delivered { .. }, None) => {
                Some(state.subject.mark)
            }
            (DirectLifecycle::Queued | DirectLifecycle::Delivered { .. }, Some(_))
            | (DirectLifecycle::Final { .. }, None | Some(_)) => None,
        }
    }
}

#[cfg(test)]
#[path = "direct_message_test.rs"]
mod tests;
