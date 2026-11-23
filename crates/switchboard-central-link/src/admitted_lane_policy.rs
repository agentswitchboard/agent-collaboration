//! Admitted command custody and correlation on one socket.

use std::sync::Arc;

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, CorrelatedRequestReply, LinkLaneDisposition,
    LinkLaneReply, WriteBatchDisposition, WriteBatchMemberDisposition,
};
use central_link_protocol::{
    CorrelationId, LinkRequest, LinkRequestBatch, ReceiptResult, ReplyPayload, RequestPayload,
    RequestStatusAnswer,
};
use router_clock::ClockInstant;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LaneState {
    WaitingForAdmission,
    AdmittedIdle(AdmittedConnection),
    AdmittedBusy {
        admitted: AdmittedConnection,
        pending: ExpectedReply,
        custody: WriteCustody,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum WriteCustody {
    Queued,
    Writing,
    AwaitingReply { deadline: ClockInstant },
}

impl LaneState {
    pub(super) fn admitted(&self) -> Option<&AdmittedConnection> {
        match self {
            Self::WaitingForAdmission => None,
            Self::AdmittedIdle(admitted) | Self::AdmittedBusy { admitted, .. } => Some(admitted),
        }
    }

    pub(super) fn can_offer(&self) -> bool {
        matches!(self, Self::AdmittedIdle(_))
    }

    pub(super) fn reply_deadline(&self) -> Option<ClockInstant> {
        match self {
            Self::AdmittedBusy {
                custody: WriteCustody::AwaitingReply { deadline },
                ..
            } => Some(*deadline),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum ExpectedReply {
    WriteBatch {
        members: Vec<RequestMember>,
    },
    ImmediateRequest(RequestMember),
    RequestStatus(CorrelationId),
    ReceiptResults {
        correlation_id: CorrelationId,
        length: usize,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RequestMember {
    correlation_id: CorrelationId,
    family: RequestFamily,
    result: Option<Arc<ReplyPayload>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestFamily {
    Workspace,
    Mail,
}

impl RequestMember {
    fn from_request(request: &LinkRequest) -> Self {
        Self {
            correlation_id: request.correlation_id().clone(),
            family: match request.payload() {
                RequestPayload::Workspace(_) => RequestFamily::Workspace,
                RequestPayload::Mail(_) => RequestFamily::Mail,
            },
            result: None,
        }
    }

    fn accepts(&self, correlation_id: &CorrelationId, result: &ReplyPayload) -> bool {
        self.result.is_none()
            && &self.correlation_id == correlation_id
            && matches!(
                (self.family, result),
                (RequestFamily::Workspace, ReplyPayload::Workspace(_))
                    | (RequestFamily::Mail, ReplyPayload::Mail(_))
            )
    }

    fn disposition(&self) -> WriteBatchMemberDisposition {
        match &self.result {
            Some(result) => WriteBatchMemberDisposition::Answered {
                reply: CorrelatedRequestReply {
                    correlation_id: self.correlation_id.clone(),
                    result: Box::new(result.as_ref().clone()),
                },
            },
            None => WriteBatchMemberDisposition::AnswerLost {
                correlation_id: self.correlation_id.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum SocketReply {
    Request {
        correlation_id: CorrelationId,
        result: Box<ReplyPayload>,
    },
    RequestStatus {
        correlation_id: CorrelationId,
        result: Box<RequestStatusAnswer>,
    },
    ReceiptResults {
        correlation_id: CorrelationId,
        results: Vec<ReceiptResult>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LaneResult {
    Batch(WriteBatchDisposition),
    Single(LinkLaneDisposition),
}

impl ExpectedReply {
    pub(super) fn batch(requests: &LinkRequestBatch) -> Self {
        Self::WriteBatch {
            members: requests
                .requests()
                .iter()
                .map(RequestMember::from_request)
                .collect(),
        }
    }

    pub(super) fn immediate(request: &LinkRequest) -> Self {
        Self::ImmediateRequest(RequestMember::from_request(request))
    }

    pub(super) fn correlations(&self) -> Vec<CorrelationId> {
        match self {
            Self::WriteBatch { members } => members
                .iter()
                .map(|member| member.correlation_id.clone())
                .collect(),
            Self::ImmediateRequest(member) => vec![member.correlation_id.clone()],
            Self::RequestStatus(correlation_id) | Self::ReceiptResults { correlation_id, .. } => {
                vec![correlation_id.clone()]
            }
        }
    }

    fn batch_result(members: &[RequestMember]) -> LaneResult {
        LaneResult::Batch(WriteBatchDisposition::MemberResults {
            members: members.iter().map(RequestMember::disposition).collect(),
        })
    }

    fn unanswered(&self, custody: &WriteCustody) -> LaneResult {
        match (self, custody) {
            (Self::WriteBatch { .. }, WriteCustody::Queued) => {
                LaneResult::Batch(WriteBatchDisposition::NotReached)
            }
            (
                Self::WriteBatch { members },
                WriteCustody::Writing | WriteCustody::AwaitingReply { .. },
            ) => Self::batch_result(members),
            (_, WriteCustody::Queued) => LaneResult::Single(LinkLaneDisposition::NotReached),
            (_, WriteCustody::Writing | WriteCustody::AwaitingReply { .. }) => {
                LaneResult::Single(LinkLaneDisposition::AnswerLost)
            }
        }
    }

    fn observe_reply(
        &mut self,
        reply: SocketReply,
    ) -> Result<Option<LaneResult>, TransitionError<LaneRefusal>> {
        let answered = match (self, reply) {
            (
                Self::WriteBatch { members },
                SocketReply::Request {
                    correlation_id,
                    result,
                },
            ) => {
                let Some(member) = members
                    .iter_mut()
                    .find(|member| member.accepts(&correlation_id, &result))
                else {
                    return Err(TransitionError::Stale);
                };
                member.result = Some(Arc::from(result));
                return Ok(members
                    .iter()
                    .all(|member| member.result.is_some())
                    .then(|| Self::batch_result(members)));
            }
            (
                Self::ImmediateRequest(member),
                SocketReply::Request {
                    correlation_id,
                    result,
                },
            ) if member.accepts(&correlation_id, &result) => LinkLaneReply::ImmediateRequest {
                reply: CorrelatedRequestReply {
                    correlation_id,
                    result,
                },
            },
            (
                Self::RequestStatus(expected),
                SocketReply::RequestStatus {
                    correlation_id,
                    result,
                },
            ) if *expected == correlation_id => LinkLaneReply::RequestStatus {
                correlation_id,
                result,
            },
            (
                Self::ReceiptResults {
                    correlation_id: expected,
                    length,
                },
                SocketReply::ReceiptResults {
                    correlation_id,
                    results,
                },
            ) if *expected == correlation_id && results.len() == *length => {
                LinkLaneReply::ReceiptResults {
                    correlation_id,
                    results,
                }
            }
            _ => return Err(TransitionError::Stale),
        };
        Ok(Some(LaneResult::Single(LinkLaneDisposition::Answered {
            reply: answered,
        })))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LaneInput {
    Admit(AdmittedConnection),
    Offer {
        connection: ConnectionAttempt,
        expected: ExpectedReply,
    },
    WriteStarted,
    WriteFlushed {
        deadline: ClockInstant,
    },
    Reply {
        connection: ConnectionAttempt,
        reply: SocketReply,
        now: ClockInstant,
    },
    Elapsed {
        now: ClockInstant,
    },
    Close {
        connection: ConnectionAttempt,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LaneEffect {
    Settle(LaneResult),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum LaneRefusal {
    WrongPhase,
}

impl RefusalTag for LaneRefusal {
    fn refusal_tag(&self) -> &'static str {
        "wrong_phase"
    }
}

impl StateTag for LaneState {
    fn state_tag(&self) -> &'static str {
        match self {
            Self::WaitingForAdmission => "waiting_for_admission",
            Self::AdmittedIdle(_) => "admitted_idle",
            Self::AdmittedBusy {
                custody: WriteCustody::Queued,
                ..
            } => "queued",
            Self::AdmittedBusy {
                custody: WriteCustody::Writing,
                ..
            } => "writing",
            Self::AdmittedBusy {
                custody: WriteCustody::AwaitingReply { .. },
                ..
            } => "awaiting_reply",
        }
    }

    fn is_terminal(&self) -> bool {
        false
    }
}

impl EventTag for LaneInput {
    fn event_tag(&self) -> &'static str {
        match self {
            Self::Admit(_) => "admit",
            Self::Offer { .. } => "offer",
            Self::WriteStarted => "write_started",
            Self::WriteFlushed { .. } => "write_flushed",
            Self::Reply { .. } => "reply",
            Self::Elapsed { .. } => "elapsed",
            Self::Close { .. } => "close",
        }
    }
}

pub(super) struct AdmittedLanePolicy;

impl StateMachine for AdmittedLanePolicy {
    type State = LaneState;
    type Event = LaneInput;
    type Effect = LaneEffect;
    type Error = LaneRefusal;

    fn transition(state: &LaneState, event: LaneInput) -> StateMachineResult<Self> {
        let mut effects = Vec::new();
        let mut next = state.clone();
        match event {
            LaneInput::Admit(admitted) if matches!(state, LaneState::WaitingForAdmission) => {
                next = LaneState::AdmittedIdle(admitted);
            }
            LaneInput::Offer {
                connection,
                expected,
            } => {
                let LaneState::AdmittedIdle(admitted) = state else {
                    return Err(TransitionError::Domain(LaneRefusal::WrongPhase));
                };
                if admitted.connection != connection {
                    return Err(TransitionError::Stale);
                }
                next = LaneState::AdmittedBusy {
                    admitted: admitted.clone(),
                    pending: expected,
                    custody: WriteCustody::Queued,
                };
            }
            LaneInput::WriteStarted => {
                let LaneState::AdmittedBusy {
                    custody: custody @ WriteCustody::Queued,
                    ..
                } = &mut next
                else {
                    return Err(TransitionError::Domain(LaneRefusal::WrongPhase));
                };
                *custody = WriteCustody::Writing;
            }
            LaneInput::WriteFlushed { deadline } => {
                let LaneState::AdmittedBusy {
                    custody: custody @ WriteCustody::Writing,
                    ..
                } = &mut next
                else {
                    return Err(TransitionError::Domain(LaneRefusal::WrongPhase));
                };
                *custody = WriteCustody::AwaitingReply { deadline };
            }
            LaneInput::Reply {
                connection,
                reply,
                now,
            } => {
                let LaneState::AdmittedBusy {
                    admitted,
                    pending,
                    custody: WriteCustody::AwaitingReply { deadline },
                } = &mut next
                else {
                    return Err(TransitionError::Stale);
                };
                if admitted.connection != connection || now >= *deadline {
                    return Err(TransitionError::Stale);
                }
                if let Some(result) = pending.observe_reply(reply)? {
                    effects.push(LaneEffect::Settle(result));
                    next = LaneState::AdmittedIdle(admitted.clone());
                }
            }
            LaneInput::Elapsed { now } => {
                let LaneState::AdmittedBusy {
                    admitted,
                    pending,
                    custody: custody @ WriteCustody::AwaitingReply { deadline },
                } = state
                else {
                    return Err(TransitionError::Stale);
                };
                if now < *deadline {
                    return Err(TransitionError::Stale);
                }
                effects.push(LaneEffect::Settle(pending.unanswered(custody)));
                next = LaneState::AdmittedIdle(admitted.clone());
            }
            LaneInput::Close { connection } => {
                if !state
                    .admitted()
                    .is_some_and(|admitted| admitted.connection == connection)
                {
                    return Err(TransitionError::Stale);
                }
                if let LaneState::AdmittedBusy {
                    pending, custody, ..
                } = state
                {
                    effects.push(LaneEffect::Settle(pending.unanswered(custody)));
                }
                next = LaneState::WaitingForAdmission;
            }
            LaneInput::Admit(_) => return Err(TransitionError::Domain(LaneRefusal::WrongPhase)),
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        const STEPS: &[Transition] = &[
            transition("waiting_for_admission", "admit", "admitted_idle"),
            transition("admitted_idle", "offer", "queued"),
            transition("queued", "write_started", "writing"),
            transition("writing", "write_flushed", "awaiting_reply"),
            transition("awaiting_reply", "reply", "awaiting_reply"),
            transition("awaiting_reply", "reply", "admitted_idle"),
            transition("awaiting_reply", "elapsed", "admitted_idle"),
            transition("admitted_idle", "close", "waiting_for_admission"),
            transition("queued", "close", "waiting_for_admission"),
            transition("writing", "close", "waiting_for_admission"),
            transition("awaiting_reply", "close", "waiting_for_admission"),
        ];
        STEPS
    }
}

const fn transition(from: &'static str, event: &'static str, to: &'static str) -> Transition {
    Transition {
        from,
        event,
        to,
        terminal: false,
    }
}

#[cfg(test)]
#[path = "admitted_lane_policy_test.rs"]
mod tests;
