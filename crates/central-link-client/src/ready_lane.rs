//! Commands and observations shared by the socket owner and its ready-link users.

use std::{future::Future, pin::Pin};

use central_link_protocol::{
    CorrelationId, DeliveryMessage, LinkRequest, LinkRequestBatch, ReceiptBatch, ReceiptResult,
    ReplyPayload, RequestStatusAnswer, RequestStatusQuery, TimeLeft,
};
use router_clock::ClockInstant;
use tokio::sync::watch;
use workspace_ids::RouterRunId;
use workspace_model::{Caller, ResolvedSender, Timestamp};

use crate::ConnectionAttempt;

/// The socket owner pairs Central's Welcome time with its injected elapsed clock at receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WelcomeClockAnchor {
    pub central_time: Timestamp,
    pub observed_at: ClockInstant,
}

/// The socket owner publishes this after Central welcomes and admits this connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedConnection {
    pub connection: ConnectionAttempt,
    pub clock_anchor: WelcomeClockAnchor,
}

/// The run gate publishes this after Central registers the run on the admitted connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredConnection {
    pub connection: ConnectionAttempt,
    pub clock_anchor: WelcomeClockAnchor,
    pub router_run: RouterRunId,
}

/// A link user prepares a batch of writes for one connection, retaining each supplied request ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteBatchCommand {
    pub connection: ConnectionAttempt,
    pub requests: LinkRequestBatch,
}

/// A link user prepares one single-answer frame for the connection it observed as admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneCommand {
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

/// The socket owner reports what it learned about an individual write on the batch's connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteBatchMemberDisposition {
    Answered { reply: CorrelatedRequestReply },
    AnswerLost { correlation_id: CorrelationId },
}

/// The socket owner settles the batch frame and preserves each checked member reply across loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteBatchDisposition {
    /// The writer proves that no part of the batch frame started on the named connection.
    NotReached,
    /// The write started or may have started. There is one result per request, in batch order.
    MemberResults {
        members: Box<[WriteBatchMemberDisposition]>,
    },
}

/// The socket owner checks the connection, correlation and shape of each single-frame reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneReply {
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

/// The socket owner's knowledge of a single-answer frame when it settles the command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneDisposition {
    /// The writer proves that this frame never started on its named connection.
    NotReached,
    /// Central returned the complete checked answer on the named connection.
    Answered { reply: LinkLaneReply },
    /// The write started or may have started, and the socket supplied no valid complete answer.
    AnswerLost,
}

/// The socket owner stamps message deliveries and admission changes with their connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneEvent {
    ConnectionAdmitted(AdmittedConnection),
    ConnectionNotAdmitted {
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
    /// The currently admitted connection for message operations.
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>>;

    /// The current connection for operations requiring a registered Router run.
    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>>;

    /// Hand over a batch for its named connection and await its writer and per-member facts.
    /// An unknown, repeated or foreign correlation settles no member.
    fn submit_write_batch(
        &self,
        command: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>>;

    /// Hand over a single-answer frame for its named connection and await its disposition.
    fn submit_command(
        &self,
        command: LinkLaneCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition> + Send + '_>>;

    /// Read the next link event, or `None` when the driver permanently closes the event stream.
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>>;
}
