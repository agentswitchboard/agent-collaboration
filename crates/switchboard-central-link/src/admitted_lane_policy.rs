//! Command custody and correlation on one admitted socket.

use std::sync::Arc;

use central_link_client::{
    AdmittedConnection, CheckedBatchResults, ConnectionAttempt, CorrelatedReceiptResults,
    CorrelatedRequestReply, CorrelatedStatusReply, UntouchedCommand, WriteBatchMemberDisposition,
};
use central_link_protocol::{
    CorrelationId, LinkRequest, LinkRequestBatch, ReceiptResult, ReplyPayload, RequestStatusAnswer,
};
use router_clock::ClockInstant;
use state_machine_core::{
    EventTag, RefusalTag, StateMachine, StateMachineResult, StateTag, Transition, TransitionError,
};

use crate::reply_matcher::ReplyExpectation;

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
        requests: Arc<LinkRequestBatch>,
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
    expectation: ReplyExpectation,
    result: Option<Arc<ReplyPayload>>,
}

impl RequestMember {
    fn from_request(request: &LinkRequest) -> Self {
        Self {
            correlation_id: request.correlation_id().clone(),
            expectation: ReplyExpectation::from_request(request.payload()),
            result: None,
        }
    }

    fn accepts(&self, correlation_id: &CorrelationId, result: &ReplyPayload) -> bool {
        self.result.is_none()
            && &self.correlation_id == correlation_id
            && self.expectation.accepts(result)
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
    Untouched(UntouchedCommand),
    Batch(CheckedBatchResults),
    SingleAnswered(LaneAnswer),
    SingleLost,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum LaneAnswer {
    ImmediateRequest(CorrelatedRequestReply),
    RequestStatus(CorrelatedStatusReply),
    ReceiptResults(CorrelatedReceiptResults),
}

impl ExpectedReply {
    pub(super) fn batch(requests: &LinkRequestBatch) -> Self {
        Self::WriteBatch {
            requests: Arc::new(requests.clone()),
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
            Self::WriteBatch { members, .. } => members
                .iter()
                .map(|member| member.correlation_id.clone())
                .collect(),
            Self::ImmediateRequest(member) => vec![member.correlation_id.clone()],
            Self::RequestStatus(correlation_id) | Self::ReceiptResults { correlation_id, .. } => {
                vec![correlation_id.clone()]
            }
        }
    }

    fn batch_result(requests: &LinkRequestBatch, members: &[RequestMember]) -> LaneResult {
        match CheckedBatchResults::new(requests, members.iter().map(RequestMember::disposition)) {
            Ok(results) => LaneResult::Batch(results),
            Err(_) => LaneResult::SingleLost,
        }
    }

    fn unanswered(&self, custody: &WriteCustody) -> LaneResult {
        match (self, custody) {
            (_, WriteCustody::Queued) => {
                LaneResult::Untouched(UntouchedCommand::ConnectionUnavailable)
            }
            (
                Self::WriteBatch { requests, members },
                WriteCustody::Writing | WriteCustody::AwaitingReply { .. },
            ) => Self::batch_result(requests, members),
            (_, WriteCustody::Writing | WriteCustody::AwaitingReply { .. }) => {
                LaneResult::SingleLost
            }
        }
    }

    fn observe_reply(
        &mut self,
        reply: SocketReply,
    ) -> Result<Option<LaneResult>, TransitionError<LaneRefusal>> {
        let answer = match (self, reply) {
            (
                Self::WriteBatch { requests, members },
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
                    .then(|| Self::batch_result(requests, members)));
            }
            (
                Self::ImmediateRequest(member),
                SocketReply::Request {
                    correlation_id,
                    result,
                },
            ) if member.accepts(&correlation_id, &result) => {
                LaneAnswer::ImmediateRequest(CorrelatedRequestReply {
                    correlation_id,
                    result,
                })
            }
            (
                Self::RequestStatus(expected),
                SocketReply::RequestStatus {
                    correlation_id,
                    result,
                },
            ) if *expected == correlation_id => LaneAnswer::RequestStatus(CorrelatedStatusReply {
                correlation_id,
                result,
            }),
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
                LaneAnswer::ReceiptResults(CorrelatedReceiptResults {
                    correlation_id,
                    results,
                })
            }
            _ => return Err(TransitionError::Stale),
        };
        Ok(Some(LaneResult::SingleAnswered(answer)))
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
    WriteRejected {
        cause: UntouchedCommand,
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
            Self::WriteRejected { .. } => "write_rejected",
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
                    return Err(wrong_phase());
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
                    return Err(wrong_phase());
                };
                *custody = WriteCustody::Writing;
            }
            LaneInput::WriteFlushed { deadline } => {
                let LaneState::AdmittedBusy {
                    custody: custody @ WriteCustody::Writing,
                    ..
                } = &mut next
                else {
                    return Err(wrong_phase());
                };
                *custody = WriteCustody::AwaitingReply { deadline };
            }
            LaneInput::WriteRejected { cause } => {
                let LaneState::AdmittedBusy {
                    admitted,
                    custody: WriteCustody::Queued,
                    ..
                } = state
                else {
                    return Err(wrong_phase());
                };
                effects.push(LaneEffect::Settle(LaneResult::Untouched(cause)));
                next = LaneState::AdmittedIdle(admitted.clone());
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
            LaneInput::Admit(_) => return Err(wrong_phase()),
        }
        Ok((next, effects))
    }

    fn transitions() -> &'static [Transition] {
        const STEPS: &[Transition] = &[
            transition("waiting_for_admission", "admit", "admitted_idle"),
            transition("admitted_idle", "offer", "queued"),
            transition("queued", "write_started", "writing"),
            transition("writing", "write_flushed", "awaiting_reply"),
            transition("queued", "write_rejected", "admitted_idle"),
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

const fn wrong_phase() -> TransitionError<LaneRefusal> {
    TransitionError::Domain(LaneRefusal::WrongPhase)
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
