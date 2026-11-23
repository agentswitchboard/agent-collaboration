//! Prepares and settles the one outbox batch custody owns at a time.

use central_link_client::{
    ConnectionAttempt, WelcomeClockAnchor, WriteBatchCommand, WriteBatchDisposition,
    WriteBatchMemberDisposition,
};
use central_link_protocol::{
    CorrelationId, FreshCorrelationId, LinkRequest, LinkRequestBatch, ProtocolError, ReplyPayload,
    RequestPayload,
};
use runtime_primitives::SyncTime;
use switchboard_machine_core_store::{
    EndedWrite, OutboxQueuedWrite, OutboxStoreError, OutboxWriteOutcome, StoredOutboxWrite,
};
use workspace_ids::RequestId;
use workspace_model::{
    Caller,
    central_mail::{CentralMailOperation, CentralMailRequest},
};

use crate::outbox_work::{FlushSelection, OutboxWork, OutboxWorkError};

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
        let requests = build_wire_batch(&claimed.writes, now_ms);
        let batch = match requests {
            Ok(requests) if work.mark_is_open(anchor, mark) => {
                let ticket = PreparedWriteBatch {
                    command: WriteBatchCommand {
                        connection,
                        requests,
                    },
                    token: fresh_correlation(now_ms),
                };
                self.in_flight = Some(ClaimedWriteBatch {
                    ticket: ticket.clone(),
                    writes: claimed.writes,
                });
                Some(ticket)
            }
            checked => {
                if let Err(error) = checked {
                    report.issues.push(BatchDispatchError::Wire(error));
                }
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
            WriteBatchDisposition::NotReached => {
                for write in active.writes {
                    return_not_reached(work, write.request, &mut report).await;
                }
                collect_due_writes(work, &mut report).await;
            }
            WriteBatchDisposition::MemberResults { members } => {
                let requests = submitted.command.requests.requests();
                if requests.len() != members.len()
                    || !requests
                        .iter()
                        .zip(members.iter())
                        .all(|(request, member)| {
                            request.correlation_id() == member_correlation(member)
                        })
                {
                    report.issues.push(BatchDispatchError::ResultsMismatch);
                    for write in active.writes {
                        return_answer_lost(work, write.request, &mut report).await;
                    }
                    return Some(report);
                }
                for (write, member) in active.writes.into_iter().zip(members) {
                    match member {
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

fn build_wire_batch(
    writes: &[StoredOutboxWrite],
    now_ms: i64,
) -> Result<LinkRequestBatch, ProtocolError> {
    let requests = writes
        .iter()
        .map(|write| {
            let payload = match &write.payload {
                OutboxQueuedWrite::Workspace(request) => RequestPayload::Workspace(request.clone()),
                OutboxQueuedWrite::SendMessage {
                    request_id,
                    message,
                } => RequestPayload::Mail(CentralMailRequest {
                    request_id: request_id.clone(),
                    operation: CentralMailOperation::SendMessage {
                        to: message.to.clone(),
                        body: message.body.clone(),
                        delivery: message.delivery,
                        reply_to: message.reply_to.clone(),
                        sent_at: write.sent_at,
                    },
                }),
            };
            LinkRequest::new(fresh_correlation(now_ms), write.caller.clone(), payload)
        })
        .collect::<Result<Vec<_>, _>>()?;
    LinkRequestBatch::new(requests)
}

fn fresh_correlation(now_ms: i64) -> CorrelationId {
    FreshCorrelationId::mint_at(now_ms).wire_id().clone()
}

fn member_correlation(member: &WriteBatchMemberDisposition) -> &CorrelationId {
    match member {
        WriteBatchMemberDisposition::Answered { reply } => &reply.correlation_id,
        WriteBatchMemberDisposition::AnswerLost { correlation_id } => correlation_id,
    }
}

async fn settle_answer(
    work: &mut OutboxWork,
    write: StoredOutboxWrite,
    reply: ReplyPayload,
    report: &mut FlushReport,
) {
    let outcome = match OutboxWriteOutcome::from_reply(&write.payload, reply.clone()) {
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
