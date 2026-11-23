//! Connect the admitted-lane policy to one socket owner's channels.

use std::{collections::BTreeSet, time::Duration};

use central_link_client::{
    AdmittedConnection, ClientEvent, ConnectionAttempt, LinkLaneCommand, LinkLaneDisposition,
    LinkLaneEvent, WriteBatchDisposition,
};
use central_link_protocol::{CorrelationId, DataFrame, LinkFrame};
use router_clock::ClockInstant;
use state_machine_core::StateMachine;
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    admitted_lane_policy::{
        AdmittedLanePolicy, ExpectedReply, LaneEffect, LaneInput, LaneResult, LaneState,
        SocketReply,
    },
    link_lane::{LaneRequest, LinkLaneOwner},
};

const ANSWER_LIMIT: Duration = Duration::from_secs(30);
const CONNECTION_CORRELATION_LIMIT: usize = 4096;

pub(super) struct AdmittedSocketLane {
    state: LaneState,
    admitted: watch::Sender<Option<AdmittedConnection>>,
    events: mpsc::Sender<LinkLaneEvent>,
    operation: Option<PendingOperation>,
    used_correlations: BTreeSet<CorrelationId>,
}

struct PendingOperation {
    correlations: Vec<CorrelationId>,
    responder: OperationResponder,
}

enum OperationResponder {
    Batch(oneshot::Sender<WriteBatchDisposition>),
    Single(oneshot::Sender<LinkLaneDisposition>),
}

impl OperationResponder {
    fn not_reached(self) {
        match self {
            Self::Batch(sender) => {
                let _ = sender.send(WriteBatchDisposition::NotReached);
            }
            Self::Single(sender) => {
                let _ = sender.send(LinkLaneDisposition::NotReached);
            }
        }
    }

    fn settle(self, result: LaneResult) {
        match (self, result) {
            (Self::Batch(sender), LaneResult::Batch(result)) => {
                let _ = sender.send(result);
            }
            (Self::Single(sender), LaneResult::Single(result)) => {
                let _ = sender.send(result);
            }
            (Self::Batch(_), LaneResult::Single(_)) | (Self::Single(_), LaneResult::Batch(_)) => {}
        }
    }
}

pub(super) enum FrameOffer {
    Queued(LinkFrame),
    Declined,
    Reconnect,
}

impl AdmittedSocketLane {
    pub(super) fn new(owner: &LinkLaneOwner) -> Self {
        Self {
            state: LaneState::WaitingForAdmission,
            admitted: owner.admitted.clone(),
            events: owner.events.clone(),
            operation: None,
            used_correlations: BTreeSet::new(),
        }
    }

    fn apply(&mut self, input: LaneInput) -> bool {
        let Ok((state, effects)) = AdmittedLanePolicy::transition(&self.state, input) else {
            return false;
        };
        self.state = state;
        for LaneEffect::Settle(result) in effects {
            if let Some(operation) = self.operation.take() {
                operation.responder.settle(result);
            }
        }
        true
    }

    pub(super) fn admit(&mut self, admitted: AdmittedConnection) -> bool {
        // Set aside one channel slot for withdrawal before publishing admission.
        if self.events.capacity() < 2
            || self.events.is_closed()
            || !self.apply(LaneInput::Admit(admitted.clone()))
        {
            return false;
        }
        self.admitted.send_replace(Some(admitted.clone()));
        self.events
            .try_send(LinkLaneEvent::ConnectionAdmitted(admitted))
            .is_ok()
    }

    pub(super) fn deliver(&self, event: ClientEvent) -> bool {
        let (
            Some(admitted),
            ClientEvent::Deliver {
                message,
                sender,
                time_left,
            },
        ) = (self.state.admitted(), event)
        else {
            return false;
        };
        self.events.capacity() > 1
            && self
                .events
                .try_send(LinkLaneEvent::Deliver {
                    connection: admitted.connection.clone(),
                    message,
                    sender,
                    time_left,
                })
                .is_ok()
    }

    pub(super) fn can_offer(&self) -> bool {
        self.state.can_offer()
    }

    pub(super) fn is_admitted(&self) -> bool {
        self.state.admitted().is_some()
    }

    pub(super) fn reply_deadline(&self) -> Option<ClockInstant> {
        self.state.reply_deadline()
    }

    pub(super) fn expire(&mut self, now: ClockInstant) {
        if self
            .reply_deadline()
            .is_some_and(|deadline| now >= deadline)
        {
            self.apply(LaneInput::Elapsed { now });
        }
    }

    pub(super) fn reply(
        &mut self,
        connection: &ConnectionAttempt,
        reply: SocketReply,
        now: ClockInstant,
    ) {
        self.apply(LaneInput::Reply {
            connection: connection.clone(),
            reply,
            now,
        });
    }

    pub(super) fn offer(&mut self, request: LaneRequest) -> FrameOffer {
        let (connection, frame, expected, responder) = prepare_frame(request);
        if !self.state.can_offer()
            || !self
                .state
                .admitted()
                .is_some_and(|admitted| admitted.connection == connection)
        {
            responder.not_reached();
            return FrameOffer::Declined;
        }
        let correlations = expected.correlations();
        if self.used_correlations.len() + correlations.len() > CONNECTION_CORRELATION_LIMIT {
            responder.not_reached();
            // A fresh connection bounds the ledger without making old IDs reusable on this socket.
            return FrameOffer::Reconnect;
        }
        if correlations
            .iter()
            .any(|id| self.used_correlations.contains(id))
            || !self.apply(LaneInput::Offer {
                connection,
                expected,
            })
        {
            responder.not_reached();
            return FrameOffer::Declined;
        }
        self.operation = Some(PendingOperation {
            correlations,
            responder,
        });
        FrameOffer::Queued(LinkFrame::Data(frame))
    }

    pub(super) fn start_write(&mut self) -> bool {
        if !self.apply(LaneInput::WriteStarted) {
            return false;
        }
        if let Some(operation) = self.operation.as_mut() {
            self.used_correlations
                .extend(operation.correlations.drain(..));
        }
        true
    }

    pub(super) fn finish_write(&mut self, now: ClockInstant) {
        self.apply(LaneInput::WriteFlushed {
            deadline: now.after(ANSWER_LIMIT),
        });
    }
}

impl Drop for AdmittedSocketLane {
    fn drop(&mut self) {
        if let Some(admitted) = self.state.admitted() {
            let connection = admitted.connection.clone();
            self.admitted.send_replace(None);
            self.apply(LaneInput::Close {
                connection: connection.clone(),
            });
            let _ = self
                .events
                .try_send(LinkLaneEvent::ConnectionNotAdmitted { connection });
        }
    }
}

fn prepare_frame(
    request: LaneRequest,
) -> (
    ConnectionAttempt,
    DataFrame,
    ExpectedReply,
    OperationResponder,
) {
    match request {
        LaneRequest::WriteBatch(command, settled) => {
            let expected = ExpectedReply::batch(&command.requests);
            (
                command.connection,
                DataFrame::Batch {
                    requests: command.requests,
                },
                expected,
                OperationResponder::Batch(settled),
            )
        }
        LaneRequest::SingleCommand(command, settled) => {
            let (connection, frame, expected) = match command {
                LinkLaneCommand::SubmitReceipts {
                    connection,
                    correlation_id,
                    receipts,
                } => {
                    let expected = ExpectedReply::ReceiptResults {
                        correlation_id: correlation_id.clone(),
                        length: receipts.receipts().len(),
                    };
                    (
                        connection,
                        DataFrame::Receipts {
                            correlation_id,
                            receipts,
                        },
                        expected,
                    )
                }
                LinkLaneCommand::RequestStatus {
                    connection,
                    correlation_id,
                    caller,
                    query,
                } => {
                    let expected = ExpectedReply::RequestStatus(correlation_id.clone());
                    (
                        connection,
                        DataFrame::RequestStatus {
                            correlation_id,
                            caller,
                            query,
                        },
                        expected,
                    )
                }
                LinkLaneCommand::ImmediateRequest {
                    connection,
                    request,
                } => {
                    let expected = ExpectedReply::immediate(&request);
                    (connection, DataFrame::Request { request }, expected)
                }
            };
            (
                connection,
                frame,
                expected,
                OperationResponder::Single(settled),
            )
        }
    }
}
