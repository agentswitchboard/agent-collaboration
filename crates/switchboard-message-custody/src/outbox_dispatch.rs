//! Prepares and settles the one outbox batch custody owns at a time.

use central_link_client::{
    ConnectionAttempt, LINK_LANE_FRAME_BYTES, LaneFrameSizeError, UntouchedCommand,
    WRITE_BATCH_ENVELOPE_BYTES, WelcomeClockAnchor, WriteBatchCommand, WriteBatchDisposition,
    WriteBatchMemberDisposition, encoded_request_size,
};
use central_link_protocol::{
    CorrelationId, FreshCorrelationId, LinkRequestBatch, ProtocolError, ReplyPayload,
};
use runtime_primitives::SyncTime;
use switchboard_machine_core_store::{EndedWrite, OutboxStoreError, StoredOutboxWrite};
use workspace_ids::RequestId;
use workspace_model::Caller;

use crate::{
    custody_operator::CustodyOperatorEvent,
    flush_clock::send_fence_for_mark,
    outbox_settlement_map::outcome_from_link_reply,
    outbox_wire::request_for_write,
    outbox_work::{FlushSelection, OutboxWork, OutboxWorkError},
};

/// A checked answer custody persisted for one member of its own flush.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FlushedWriteAnswer {
    pub request: RequestId,
    pub caller: Caller,
    pub reply: ReplyPayload,
}

/// A problem custody observed while settling a batch or returning a claim.
#[derive(Debug, thiserror::Error)]
pub enum BatchDispatchError {
    #[error("the outbox batch could not be prepared")]
    Work(#[from] OutboxWorkError),
    #[error("the outbox batch failed wire validation")]
    Wire(#[from] ProtocolError),
    #[error("the outbox batch could not be measured")]
    WireSize(#[from] LaneFrameSizeError),
    #[error("a stored write does not fit alone in a link frame")]
    OversizeClaim,
    #[error("the lane returned results outside the current batch's order or identity")]
    ResultsMismatch,
    #[error("the lane's reply does not settle request {0}")]
    UnsettledReply(RequestId),
    #[error("the store did not update request {request}")]
    StoreMember {
        request: RequestId,
        #[source]
        error: OutboxStoreError,
    },
}

/// One batch's results, kept by custody for the outcome sink.
#[derive(Debug, Default)]
pub struct FlushReport {
    pub answered: Vec<FlushedWriteAnswer>,
    pub ended: Vec<EndedWrite>,
    pub issues: Vec<BatchDispatchError>,
}

impl FlushReport {
    pub(crate) fn append(&mut self, other: Self) {
        self.answered.extend(other.answered);
        self.ended.extend(other.ended);
        self.issues.extend(other.issues);
    }
}

/// The command and claim token prepared by the one batch dispatcher.
#[derive(Clone, Debug)]
pub struct PreparedWriteBatch {
    command: WriteBatchCommand,
    token: CorrelationId,
}

impl PreparedWriteBatch {
    #[must_use]
    pub fn command(&self) -> WriteBatchCommand {
        self.command.clone()
    }
}

/// Work selected at a minute mark, before the caller submits its frame.
#[derive(Debug)]
pub struct PreparedFlush {
    pub batch: Option<PreparedWriteBatch>,
    pub report: FlushReport,
}

struct ClaimedWriteBatch {
    ticket: PreparedWriteBatch,
    writes: Vec<StoredOutboxWrite>,
}

/// One custody task creates a dispatcher that serializes claims and settles only its current claim.
#[derive(Default)]
pub struct OutboxDispatcher {
    in_flight: Option<ClaimedWriteBatch>,
}

impl OutboxDispatcher {
    /// Select an ordered claim for the caller's armed mark, then recheck the window before sending.
    pub async fn prepare_at_mark(
        &mut self,
        work: &mut OutboxWork,
        connection: ConnectionAttempt,
        anchor: WelcomeClockAnchor,
        mark: SyncTime,
    ) -> Result<PreparedFlush, BatchDispatchError> {
        let mut report = FlushReport::default();
        if self.in_flight.is_some() {
            return Ok(PreparedFlush {
                batch: None,
                report,
            });
        }
        report.ended.extend(work.recover_orphaned_claims().await?);
        let FlushSelection::Claimed(claimed) = work.claim_at_mark(anchor, mark).await? else {
            return Ok(PreparedFlush {
                batch: None,
                report,
            });
        };
        report.ended.extend(claimed.ended);
        if claimed.writes.is_empty() {
            return Ok(PreparedFlush {
                batch: None,
                report,
            });
        }

        let now_ms = work.local_now_ms();
        let batch = match pack_wire_batch(&claimed.writes, now_ms) {
            Ok(requests) => {
                let count = requests.requests().len();
                let mut selected = Vec::with_capacity(count);
                for (index, write) in claimed.writes.into_iter().enumerate() {
                    if index < count {
                        selected.push(write);
                    } else {
                        return_not_reached(work, write.request, &mut report).await;
                    }
                }
                let send_before = selected
                    .iter()
                    .map(|write| work.write_send_fence(write.sent_at))
                    .fold(send_fence_for_mark(anchor, mark), std::cmp::min);
                if !work.mark_is_open(anchor, mark) || !work.before_elapsed_fence(send_before) {
                    for write in selected {
                        return_not_reached(work, write.request, &mut report).await;
                    }
                    collect_due_writes(work, &mut report).await;
                    return Ok(PreparedFlush {
                        batch: None,
                        report,
                    });
                }
                let ticket = PreparedWriteBatch {
                    command: WriteBatchCommand {
                        connection,
                        send_before,
                        requests,
                    },
                    token: fresh_correlation(now_ms),
                };
                self.in_flight = Some(ClaimedWriteBatch {
                    ticket: ticket.clone(),
                    writes: selected,
                });
                Some(ticket)
            }
            Err(error) => {
                report.issues.push(error);
                for write in claimed.writes {
                    return_not_reached(work, write.request, &mut report).await;
                }
                collect_due_writes(work, &mut report).await;
                None
            }
        };
        Ok(PreparedFlush { batch, report })
    }

    /// Keep checked replies even if other members lost their answers on this batch's connection.
    pub async fn settle_batch(
        &mut self,
        work: &mut OutboxWork,
        submitted: PreparedWriteBatch,
        disposition: WriteBatchDisposition,
    ) -> Option<FlushReport> {
        let active = self.in_flight.as_ref()?;
        if active.ticket.token != submitted.token || active.ticket.command != submitted.command {
            return None;
        }
        let active = self.in_flight.take()?;
        let mut report = FlushReport::default();
        match disposition {
            WriteBatchDisposition::NotReached { cause } => {
                match cause {
                    UntouchedCommand::FrameTooLarge
                    | UntouchedCommand::InvalidFrame
                    | UntouchedCommand::SinkRejected => {
                        work.report_operator_event(CustodyOperatorEvent::UntouchedBatchFault(
                            cause,
                        ));
                    }
                    UntouchedCommand::ConnectionUnavailable
                    | UntouchedCommand::WriterBusy
                    | UntouchedCommand::SendWindowElapsed => {}
                }
                for write in active.writes {
                    return_not_reached(work, write.request, &mut report).await;
                }
                collect_due_writes(work, &mut report).await;
            }
            WriteBatchDisposition::MemberResults(results) => {
                let requests = submitted.command.requests.requests();
                let members = results.members();
                if requests.len() != members.len()
                    || !requests
                        .iter()
                        .zip(members.iter())
                        .all(|(request, member)| {
                            request.correlation_id() == member.disposition.correlation_id()
                                && request.payload().write_request_id() == Some(&member.request_id)
                        })
                {
                    report.issues.push(BatchDispatchError::ResultsMismatch);
                    for write in active.writes {
                        return_answer_lost(work, write.request, &mut report).await;
                    }
                    return Some(report);
                }
                for (write, member) in active.writes.into_iter().zip(results.into_members()) {
                    match member.disposition {
                        WriteBatchMemberDisposition::Answered { reply } => {
                            settle_answer(work, write, *reply.result, &mut report).await;
                        }
                        WriteBatchMemberDisposition::AnswerLost { .. } => {
                            return_answer_lost(work, write.request, &mut report).await;
                        }
                    }
                }
            }
        }
        Some(report)
    }
}

fn pack_wire_batch(
    writes: &[StoredOutboxWrite],
    now_ms: i64,
) -> Result<LinkRequestBatch, BatchDispatchError> {
    let mut bytes = WRITE_BATCH_ENVELOPE_BYTES;
    let mut requests = Vec::new();
    for write in writes {
        let wire = request_for_write(&write.caller, &write.payload, write.sent_at, now_ms)?;
        let next = bytes
            .saturating_add(usize::from(!requests.is_empty()))
            .saturating_add(encoded_request_size(&wire)?);
        if next > LINK_LANE_FRAME_BYTES {
            if requests.is_empty() {
                return Err(BatchDispatchError::OversizeClaim);
            }
            break;
        }
        bytes = next;
        requests.push(wire);
    }
    Ok(LinkRequestBatch::new(requests)?)
}

fn fresh_correlation(now_ms: i64) -> CorrelationId {
    FreshCorrelationId::mint_at(now_ms).wire_id().clone()
}

async fn settle_answer(
    work: &mut OutboxWork,
    write: StoredOutboxWrite,
    reply: ReplyPayload,
    report: &mut FlushReport,
) {
    let outcome = match outcome_from_link_reply(&write.payload, reply.clone()) {
        Ok(outcome) => outcome,
        Err(_) => {
            report
                .issues
                .push(BatchDispatchError::UnsettledReply(write.request.clone()));
            return_answer_lost(work, write.request, report).await;
            return;
        }
    };
    let now_ms = work.local_now_ms();
    match work
        .outbox_storage()
        .record_outcome(&write.request, &outcome, now_ms)
        .await
    {
        Ok(()) => report.answered.push(FlushedWriteAnswer {
            request: write.request,
            caller: write.caller,
            reply,
        }),
        Err(error) => report.issues.push(BatchDispatchError::StoreMember {
            request: write.request,
            error,
        }),
    }
}

async fn return_not_reached(work: &mut OutboxWork, request: RequestId, report: &mut FlushReport) {
    let now_ms = work.local_now_ms();
    if let Err(error) = work
        .outbox_storage()
        .return_not_transmitted(&request, now_ms)
        .await
    {
        report
            .issues
            .push(BatchDispatchError::StoreMember { request, error });
    }
}

async fn return_answer_lost(work: &mut OutboxWork, request: RequestId, report: &mut FlushReport) {
    match work.return_unanswered(std::slice::from_ref(&request)).await {
        Ok(ended) => report.ended.extend(ended),
        Err(error) => report
            .issues
            .push(BatchDispatchError::StoreMember { request, error }),
    }
}

async fn collect_due_writes(work: &mut OutboxWork, report: &mut FlushReport) {
    match work.end_due_writes().await {
        Ok(ended) => report.ended.extend(ended),
        Err(error) => report.issues.push(BatchDispatchError::Work(error)),
    }
}
