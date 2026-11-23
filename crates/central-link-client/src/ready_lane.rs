//! Commands and observations shared by the socket owner and its ready-link users.

use std::{future::Future, pin::Pin};

use central_link_protocol::{
    CorrelationId, DeliveryMessage, LinkRequest, LinkRequestBatch, ReceiptBatch, ReceiptResult,
    ReplyPayload, RequestStatusAnswer, RequestStatusQuery, TimeLeft,
};
use router_clock::ClockInstant;
use tokio::sync::watch;
use workspace_model::{Caller, ResolvedSender, Timestamp};

use crate::ConnectionAttempt;

/// The socket owner pairs Central's Welcome time with its injected elapsed clock at receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WelcomeClockAnchor {
    pub central_time: Timestamp,
    pub observed_at: ClockInstant,
}

/// The socket owner publishes this only after registration and admission on this connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadyConnection {
    pub connection: ConnectionAttempt,
    pub clock_anchor: WelcomeClockAnchor,
}

/// A link user prepares one frame for the connection it observed as ready.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneCommand {
    WriteBatch {
        connection: ConnectionAttempt,
        requests: LinkRequestBatch,
    },
    SubmitReceipts {
        connection: ConnectionAttempt,
        correlation_id: CorrelationId,
        receipts: ReceiptBatch,
    },
    RequestStatus {
        connection: ConnectionAttempt,
        correlation_id: CorrelationId,
        caller: Caller,
        query: RequestStatusQuery,
    },
    ImmediateRequest {
        connection: ConnectionAttempt,
        request: Box<LinkRequest>,
    },
}

/// The socket owner pairs a checked Central reply with its originating request correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedRequestReply {
    pub correlation_id: CorrelationId,
    pub result: Box<ReplyPayload>,
}

/// The socket owner returns these replies after checking connection, correlation and result shape.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneReply {
    WriteBatch {
        replies: Vec<CorrelatedRequestReply>,
    },
    ReceiptResults {
        correlation_id: CorrelationId,
        results: Vec<ReceiptResult>,
    },
    RequestStatus {
        correlation_id: CorrelationId,
        result: Box<RequestStatusAnswer>,
    },
    ImmediateRequest {
        reply: CorrelatedRequestReply,
    },
}

/// The socket owner's knowledge of a handed-over frame when it settles the command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneDisposition {
    /// The writer proves that this frame never started on its named connection.
    NotReached,
    /// Central returned the complete checked answer on the named connection.
    Answered { reply: LinkLaneReply },
    /// The write started or may have started, and the socket supplied no valid complete answer.
    AnswerLost,
}

/// The socket owner stamps each delivery and each readiness change with its connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneEvent {
    ConnectionReady(ReadyConnection),
    ConnectionNotReady {
        connection: ConnectionAttempt,
    },
    Deliver {
        connection: ConnectionAttempt,
        message: Box<DeliveryMessage>,
        sender: ResolvedSender,
        time_left: Option<TimeLeft>,
    },
}

/// The socket owner supplies readiness, a command lane and an event stream to its link users.
pub trait ReadyLinkLane: Send + Sync {
    /// The current ready connection. The watch contains `None` while unavailable.
    fn watch_readiness(&self) -> watch::Receiver<Option<ReadyConnection>>;

    /// Hand over one frame for its named connection and await the writer's final disposition.
    fn submit_command(
        &self,
        command: LinkLaneCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition> + Send + '_>>;

    /// Read the next link event, or `None` when the driver permanently closes the event stream.
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>>;
}
