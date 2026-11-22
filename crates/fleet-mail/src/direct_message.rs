//! The six-state Central lifecycle of one direct message.
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
pub(crate) struct SavedDelivery {
    pub(crate) arrived_at: Timestamp,
    pub(crate) waiting: Option<mail::WaitingReason>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FinalReceipt {
    Seen { completed_at: Timestamp },
    NotSeen { waiting: mail::WaitingReason },
    Refused { reason: mail::MessageRefusal },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectLifecycle {
    Queued,
    Delivered(SavedDelivery),
    PastTheMark(Option<SavedDelivery>),
    Final {
        receipt: FinalReceipt,
        saved: Option<SavedDelivery>,
    },
}

impl DirectLifecycle {
    pub(crate) fn project_message_status(
        self,
        cutoff: Timestamp,
        now: Timestamp,
    ) -> mail::MessageStatus {
        use mail::MessageStatus;
        let delivered = |observation: SavedDelivery, deadline_passed| MessageStatus::Delivered {
            arrived_at: observation.arrived_at,
            waiting: observation.waiting,
            deadline_passed,
        };
        match self {
            Self::Queued => MessageStatus::Sent {
                deadline_passed: now >= cutoff,
            },
            Self::Delivered(saved) => delivered(saved, now >= cutoff),
            Self::PastTheMark(Some(saved)) => delivered(saved, true),
            Self::PastTheMark(None) => MessageStatus::Sent {
                deadline_passed: true,
            },
            Self::Final { receipt, .. } => match receipt {
                FinalReceipt::Seen { completed_at } => MessageStatus::Seen { completed_at },
                FinalReceipt::NotSeen { waiting } => MessageStatus::NotSeen { waiting },
                FinalReceipt::Refused { reason } => MessageStatus::Refused { reason },
            },
        }
    }

    pub(crate) fn saved_delivery(self) -> Option<SavedDelivery> {
        match self {
            Self::Queued => None,
            Self::Delivered(saved) => Some(saved),
            Self::PastTheMark(saved) | Self::Final { saved, .. } => saved,
        }
    }

    pub(crate) fn storage_tag(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Delivered(_) => "delivered",
            Self::PastTheMark(_) => "pastTheMark",
            Self::Final { receipt, .. } => match receipt {
                FinalReceipt::Seen { .. } => "seen",
                FinalReceipt::NotSeen { .. } => "notSeen",
                FinalReceipt::Refused { .. } => "refused",
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectReducerState {
    pub(crate) subject: DirectMessageSubject,
    pub(crate) lifecycle: DirectLifecycle,
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
                mail::ReceiptKind::NotSeen { .. } => "notSeen",
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
    prior: DirectLifecycle,
    receipt: mail::ReceiptKind,
) -> Result<LifecycleChange, DirectMessageRefusal> {
    use mail::{MessageOutcome, ReceiptKind};

    let final_receipt = match receipt {
        ReceiptKind::Delivered {
            arrived_at,
            waiting,
        } => {
            let saved = SavedDelivery {
                arrived_at,
                waiting,
            };
            return match prior {
                DirectLifecycle::Queued => Ok((DirectLifecycle::Delivered(saved), None)),
                DirectLifecycle::PastTheMark(None) => {
                    Ok((DirectLifecycle::PastTheMark(Some(saved)), None))
                }
                DirectLifecycle::Delivered(_)
                | DirectLifecycle::PastTheMark(Some(_))
                | DirectLifecycle::Final { .. } => Err(DirectMessageRefusal::AlreadyRecorded),
            };
        }
        ReceiptKind::Seen { completed_at } => FinalReceipt::Seen { completed_at },
        ReceiptKind::NotSeen { waiting } => FinalReceipt::NotSeen { waiting },
        ReceiptKind::Refused { reason } => FinalReceipt::Refused { reason },
    };

    let outcome = match prior {
        DirectLifecycle::Queued | DirectLifecycle::Delivered(_) => match final_receipt {
            FinalReceipt::Seen { .. } => None,
            FinalReceipt::NotSeen { waiting } => Some(MessageOutcome::NotSeen { waiting }),
            FinalReceipt::Refused { reason } => Some(MessageOutcome::Refused { reason }),
        },
        DirectLifecycle::PastTheMark(_) => None,
        DirectLifecycle::Final { receipt, .. } => {
            return Err(if receipt == final_receipt {
                DirectMessageRefusal::AlreadyRecorded
            } else {
                DirectMessageRefusal::AlreadySettled
            });
        }
    };
    Ok((
        DirectLifecycle::Final {
            receipt: final_receipt,
            saved: prior.saved_delivery(),
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

        let (lifecycle, outcome) = match event {
            DirectMessageEvent::Receipt(receipt) => {
                apply_receipt(state.lifecycle, receipt).map_err(TransitionError::Domain)?
            }
            DirectMessageEvent::Due { occurrence } => {
                if Self::next_due(state) != Some(occurrence) {
                    return Err(TransitionError::Stale);
                }
                let saved = state.lifecycle.saved_delivery();
                let outcome = saved.map_or(MessageOutcome::DeliveryNotConfirmed, |delivery| {
                    MessageOutcome::DeliveredSeenNotConfirmed {
                        arrived_at: delivery.arrived_at,
                        waiting: delivery.waiting,
                    }
                });
                (DirectLifecycle::PastTheMark(saved), Some(outcome))
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
            },
            effects,
        ))
    }

    fn transitions() -> &'static [Transition] {
        const EDGES: &[Transition] = &[
            direct_transition_edge("queued", "delivered", "delivered", false),
            direct_transition_edge("queued", "seen", "seen", true),
            direct_transition_edge("queued", "notSeen", "notSeen", true),
            direct_transition_edge("queued", "refused", "refused", true),
            direct_transition_edge("queued", "due", "pastTheMark", false),
            direct_transition_edge("delivered", "seen", "seen", true),
            direct_transition_edge("delivered", "notSeen", "notSeen", true),
            direct_transition_edge("delivered", "refused", "refused", true),
            direct_transition_edge("delivered", "due", "pastTheMark", false),
            direct_transition_edge("pastTheMark", "delivered", "pastTheMark", false),
            direct_transition_edge("pastTheMark", "seen", "seen", true),
            direct_transition_edge("pastTheMark", "notSeen", "notSeen", true),
            direct_transition_edge("pastTheMark", "refused", "refused", true),
        ];
        EDGES
    }

    fn next_due(state: &DirectReducerState) -> Option<EngineTime> {
        match state.lifecycle {
            DirectLifecycle::Queued | DirectLifecycle::Delivered(_) => Some(state.subject.mark),
            DirectLifecycle::PastTheMark(_) | DirectLifecycle::Final { .. } => None,
        }
    }
}

#[cfg(test)]
#[path = "direct_message_test.rs"]
mod tests;
