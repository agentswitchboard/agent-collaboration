//! Commands and observations shared by the socket owner and its ready-link users.

use std::{future::Future, io, pin::Pin, time::Duration};

use central_link_protocol::{
    CentralCall, CentralCallReply, CorrelationId, DataFrame, DeliveryMessage, LinkFrame,
    LinkRequest, LinkRequestBatch, ReceiptBatch, ReceiptResult, ReplyPayload, RequestStatusAnswer,
    RequestStatusQuery, TimeLeft, TypedCallResult,
    approval_rpc::{ApprovalServiceTarget, RegisterRunReply},
};
use router_clock::ClockInstant;
use tokio::sync::watch;
use workspace_ids::{RequestId, RouterRunId};
use workspace_model::{Caller, ResolvedSender, Timestamp, central_mail::MessageBody};

use crate::{ConnectionAttempt, ReplyExpectation};

/// The usable UTF-8 payload budget for a client frame after its longest masked header.
pub const LINK_LANE_FRAME_BYTES: usize = central_link_protocol::LINK_MAX_EGRESS_TEXT_BYTES;

const FRAME_WRITE_LIMIT: Duration = Duration::from_secs(5);
const FRAME_ANSWER_LIMIT: Duration = Duration::from_secs(30);

/// The maximum elapsed time from a command frame's first accepted transport byte
/// to its lane disposition, including the remaining flush and wait for an answer.
pub const LINK_LANE_SETTLEMENT_LIMIT: Duration =
    FRAME_WRITE_LIMIT.saturating_add(FRAME_ANSWER_LIMIT);

/// Calculate the socket writer's elapsed deadline for a frame it is about to write.
pub fn link_frame_write_deadline(write_began_at: ClockInstant) -> ClockInstant {
    write_began_at.after(FRAME_WRITE_LIMIT)
}

/// Calculate the command owner's answer deadline after a completed transport flush.
pub fn link_frame_answer_deadline(
    write_began_at: ClockInstant,
    flushed_at: ClockInstant,
) -> ClockInstant {
    flushed_at
        .after(FRAME_ANSWER_LIMIT)
        .min(write_began_at.after(LINK_LANE_SETTLEMENT_LIMIT))
}

/// The write-batch envelope cost in addition to encoded members and their commas.
pub const WRITE_BATCH_ENVELOPE_BYTES: usize = b"{\"kind\":\"batch\",\"requests\":[]}".len();

/// A failure the link client reports when measuring an outgoing request with its JSON serializer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LaneFrameSizeError {
    #[error("the link request could not be measured as JSON")]
    EncodeFailed,
}

/// Count one request as encoded inside the batch array, without allocating its wire text.
pub fn encoded_request_size(request: &LinkRequest) -> Result<usize, LaneFrameSizeError> {
    struct ByteCount(usize);
    impl io::Write for ByteCount {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("encoded link size overflow"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut count = ByteCount(0);
    serde_json::to_writer(&mut count, request).map_err(|_| LaneFrameSizeError::EncodeFailed)?;
    Ok(count.0)
}

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

/// The run gate derives this from admission and Central's matching registration result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredConnection {
    admitted: AdmittedConnection,
    router_run: RouterRunId,
}

impl RegisteredConnection {
    /// The run gate publishes the view only after its pending registration settles successfully.
    pub fn from_registration(evidence: RunRegistrationEvidence) -> Self {
        Self {
            admitted: evidence.admitted,
            router_run: evidence.router_run,
        }
    }

    pub const fn admitted(&self) -> &AdmittedConnection {
        &self.admitted
    }

    pub const fn router_run(&self) -> &RouterRunId {
        &self.router_run
    }
}

/// The registration owner records the exact RegisterRun frame written on one admitted connection.
pub struct PendingRunRegistration {
    request: Option<SentRegistration>,
}

struct SentRegistration {
    admitted: AdmittedConnection,
    correlation_id: CorrelationId,
    target: ApprovalServiceTarget,
    router_run: RouterRunId,
}

/// The pending registration owner mints this only on its matching successful Central reply.
pub struct RunRegistrationEvidence {
    admitted: AdmittedConnection,
    router_run: RouterRunId,
}

impl PendingRunRegistration {
    pub fn from_sent_frame(admitted: AdmittedConnection, frame: &LinkFrame) -> Option<Self> {
        let LinkFrame::Data(DataFrame::CallCentral {
            correlation_id,
            request,
        }) = frame
        else {
            return None;
        };
        let CentralCall::RegisterRun(registration) = request.as_ref() else {
            return None;
        };
        Some(Self {
            request: Some(SentRegistration {
                admitted,
                correlation_id: correlation_id.clone(),
                target: registration.target.clone(),
                router_run: registration.body.router_run.clone(),
            }),
        })
    }

    /// Settle a reply observed by the current socket owner. Foreign observations leave it pending.
    pub fn observe_reply(
        &mut self,
        attempt: &ConnectionAttempt,
        frame: &LinkFrame,
    ) -> Option<RunRegistrationEvidence> {
        let pending = self.request.as_ref()?;
        let LinkFrame::Data(DataFrame::CentralCallReply {
            correlation_id,
            result,
        }) = frame
        else {
            return None;
        };
        if &pending.admitted.connection != attempt || &pending.correlation_id != correlation_id {
            return None;
        }
        let registered = match result.as_ref() {
            TypedCallResult::Answered {
                reply: CentralCallReply::RegisterRun(reply),
            } if reply.target == pending.target => {
                matches!(reply.body, RegisterRunReply::Registered {})
            }
            TypedCallResult::Refused { .. } => false,
            TypedCallResult::Answered { .. } => return None,
        };
        let settled = self.request.take()?;
        registered.then_some(RunRegistrationEvidence {
            admitted: settled.admitted,
            router_run: settled.router_run,
        })
    }
}

/// The outbox owner prepares writes for one connection with the strict upper elapsed send fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteBatchCommand {
    pub connection: ConnectionAttempt,
    pub send_before: ClockInstant,
    pub requests: LinkRequestBatch,
}

/// The receipt owner prepares one frame for a named connection and supplies its send fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmitReceiptsCommand {
    pub connection: ConnectionAttempt,
    pub send_before: ClockInstant,
    pub correlation_id: CorrelationId,
    pub receipts: ReceiptBatch,
}

/// The request owner prepares one status query for a named connection and supplies its send fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestStatusCommand {
    pub connection: ConnectionAttempt,
    pub send_before: ClockInstant,
    pub correlation_id: CorrelationId,
    pub caller: Caller,
    pub query: RequestStatusQuery,
}

/// The request owner prepares an immediate operation for a named connection and supplies its send fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImmediateRequestCommand {
    pub connection: ConnectionAttempt,
    pub send_before: ClockInstant,
    pub request: Box<LinkRequest>,
}

/// The socket owner pairs a checked Central reply with its originating request correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedRequestReply {
    pub correlation_id: CorrelationId,
    pub result: Box<ReplyPayload>,
}

/// The socket owner pairs checked positional receipt results with the frame correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedReceiptResults {
    pub correlation_id: CorrelationId,
    pub results: Vec<ReceiptResult>,
}

/// The socket owner pairs a checked status answer with its query correlation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedStatusReply {
    pub correlation_id: CorrelationId,
    pub result: Box<RequestStatusAnswer>,
}

/// The socket owner reports what it learned about an individual write on the batch's connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteBatchMemberDisposition {
    Answered {
        reply: CorrelatedRequestReply,
        /// The socket owner samples the injected elapsed clock when it accepts this answer after
        /// checking the connection, correlation and operation, independently of caller polling.
        accepted_at: ClockInstant,
    },
    AnswerLost {
        correlation_id: CorrelationId,
    },
}

impl WriteBatchMemberDisposition {
    pub const fn correlation_id(&self) -> &CorrelationId {
        match self {
            Self::Answered { reply, .. } => &reply.correlation_id,
            Self::AnswerLost { correlation_id } => correlation_id,
        }
    }
}

/// The socket owner associates a checked disposition with its original durable write identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedBatchMember {
    pub request_id: RequestId,
    pub disposition: WriteBatchMemberDisposition,
}

/// A constructor failure the link client reports for an invalid batch settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BatchResultsError {
    #[error("the result count differs from the write batch")]
    WrongCount,
    #[error("the result correlation differs from the write batch member at {index}")]
    WrongMember { index: usize },
    #[error("the write batch member at {index} lacks a durable request ID")]
    MissingWriteId { index: usize },
    #[error("the reply is incompatible with the write batch member at {index}")]
    IncompatibleReply { index: usize },
}

/// The socket owner builds this checked result against the original, ordered write requests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckedBatchResults(Box<[CheckedBatchMember]>);

impl CheckedBatchResults {
    pub fn new(
        requests: &LinkRequestBatch,
        dispositions: impl IntoIterator<Item = WriteBatchMemberDisposition>,
    ) -> Result<Self, BatchResultsError> {
        let mut dispositions = dispositions.into_iter();
        let mut members = Vec::with_capacity(requests.requests().len());
        for (index, request) in requests.requests().iter().enumerate() {
            let disposition = dispositions.next().ok_or(BatchResultsError::WrongCount)?;
            if disposition.correlation_id() != request.correlation_id() {
                return Err(BatchResultsError::WrongMember { index });
            }
            if let WriteBatchMemberDisposition::Answered { reply, .. } = &disposition
                && !ReplyExpectation::from_request(request.payload()).accepts(&reply.result)
            {
                return Err(BatchResultsError::IncompatibleReply { index });
            }
            let request_id = request
                .payload()
                .write_request_id()
                .ok_or(BatchResultsError::MissingWriteId { index })?
                .clone();
            members.push(CheckedBatchMember {
                request_id,
                disposition,
            });
        }
        if dispositions.next().is_some() {
            return Err(BatchResultsError::WrongCount);
        }
        Ok(Self(members.into_boxed_slice()))
    }

    pub fn members(&self) -> &[CheckedBatchMember] {
        &self.0
    }

    pub fn into_members(self) -> Box<[CheckedBatchMember]> {
        self.0
    }
}

/// The handoff or writer owner states why none of the named frame reached the socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UntouchedCommand {
    ConnectionUnavailable,
    WriterBusy,
    SendWindowElapsed,
    FrameTooLarge,
    InvalidFrame,
    SinkRejected,
}

/// The socket owner settles the frame and preserves checked member replies across loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteBatchDisposition {
    NotReached { cause: UntouchedCommand },
    MemberResults(CheckedBatchResults),
}

/// The socket owner settles one single-answer frame using its operation's reply type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneDisposition<Reply> {
    NotReached {
        cause: UntouchedCommand,
    },
    Answered {
        reply: Reply,
        /// The socket owner samples the injected elapsed clock when it accepts this answer after
        /// checking the connection, correlation and operation, independently of caller polling.
        accepted_at: ClockInstant,
    },
    AnswerLost,
}

/// A failure the link client reports for an invalid delivery and time-left combination.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InvalidDeliveryOffer {
    #[error("the message kind and delivery time-left do not match")]
    WrongKindOrTime,
}

/// The socket owner validates the message kind and its required time-left together.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatedDeliveryOffer {
    message: Box<DeliveryMessage>,
    time_left: Option<TimeLeft>,
}

impl ValidatedDeliveryOffer {
    pub fn new(
        message: Box<DeliveryMessage>,
        time_left: Option<TimeLeft>,
    ) -> Result<Self, InvalidDeliveryOffer> {
        match (message.as_ref(), time_left) {
            (DeliveryMessage::Direct(_), Some(_)) => {}
            (DeliveryMessage::System(record), None)
                if matches!(
                    record.body,
                    MessageBody::Notice { .. } | MessageBody::DeliveryOutcome { .. }
                ) => {}
            _ => return Err(InvalidDeliveryOffer::WrongKindOrTime),
        }
        Ok(Self { message, time_left })
    }

    pub fn message(&self) -> &DeliveryMessage {
        &self.message
    }

    pub const fn time_left(&self) -> Option<TimeLeft> {
        self.time_left
    }

    pub fn into_parts(self) -> (Box<DeliveryMessage>, Option<TimeLeft>) {
        (self.message, self.time_left)
    }
}

/// The socket owner stamps message deliveries and admission changes with their connection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkLaneEvent {
    ConnectionAdmitted(AdmittedConnection),
    ConnectionNotAdmitted {
        connection: ConnectionAttempt,
    },
    ConnectionFaulted {
        connection: ConnectionAttempt,
        cause: LinkLocalFault,
    },
    Deliver {
        connection: ConnectionAttempt,
        /// The socket owner's injected UTC time when it accepted the validated offer.
        arrived_at: Timestamp,
        offer: ValidatedDeliveryOffer,
        sender: ResolvedSender,
    },
}

/// The socket owner publishes a local fault when it withdraws an admitted connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LinkLocalFault {
    DeliveryConsumerStalled,
}

/// The socket owner supplies readiness, typed commands and events to link users.
///
/// Once the socket owner accepts a command, it owns settlement. Dropping the waiting future
/// withdraws the caller's interest in the result and leaves the accepted command in driver custody.
/// The event consumer saves each offer durably to its local store and continues taking offers
/// independently of lane commands and replies. Dispatch and receipts run outside that intake.
pub trait ReadyLinkLane: Send + Sync {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>>;

    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>>;

    fn submit_write_batch(
        &self,
        command: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>>;

    /// Receipt commands settle with receipt results.
    ///
    /// ```compile_fail,E0308
    /// use central_link_client::{CorrelatedStatusReply, LinkLaneDisposition, ReadyLinkLane, SubmitReceiptsCommand};
    /// async fn wrong_family(lane: &dyn ReadyLinkLane, command: SubmitReceiptsCommand) {
    ///     let _: LinkLaneDisposition<CorrelatedStatusReply> = lane.submit_receipts(command).await;
    /// }
    /// ```
    fn submit_receipts(
        &self,
        command: SubmitReceiptsCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedReceiptResults>> + Send + '_>>;

    fn request_status(
        &self,
        command: RequestStatusCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedStatusReply>> + Send + '_>>;

    fn submit_immediate_request(
        &self,
        command: ImmediateRequestCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedRequestReply>> + Send + '_>>;

    /// Read the next event, or `None` when the driver permanently closes the stream.
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>>;
}

#[cfg(test)]
#[path = "ready_lane_test.rs"]
mod tests;
