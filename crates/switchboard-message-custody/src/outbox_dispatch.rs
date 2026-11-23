//! Prepares and settles the one outbox batch custody owns at a time.

use central_link_client::{
    ConnectionAttempt, LINK_LANE_FRAME_BYTES, UntouchedCommand, WRITE_BATCH_ENVELOPE_BYTES,
    WelcomeClockAnchor, WriteBatchCommand, WriteBatchDisposition, WriteBatchMemberDisposition,
    encoded_request_size,
};
use central_link_protocol::{CorrelationId, FreshCorrelationId, LinkRequestBatch, ReplyPayload};
use runtime_primitives::SyncTime;
use std::collections::BTreeSet;
use switchboard_machine_core_store::{StoredOutboxWrite, UnansweredOutboxClaim};
use workspace_ids::RequestId;

use crate::{
    custody_operator::CustodyOperatorEvent,
    flush_clock::send_fence_for_mark,
    local_settlement::{LocalSettlements, MemberReturn},
    outbox_wire::request_for_write,
    outbox_work::{FlushSelection, OutboxWork},
};

pub use crate::flush_report::{
    AppliedSenderNotice, BatchDispatchError, FlushReport, FlushedWriteAnswer,
};

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
    settling: LocalSettlements,
}

impl OutboxDispatcher {
    pub(crate) fn protected_request_ids(&self) -> BTreeSet<RequestId> {
        self.in_flight
            .iter()
            .flat_map(|batch| batch.writes.iter().map(|write| &write.request))
            .chain(self.settling.protected_ids())
            .cloned()
            .collect()
    }

    pub(crate) fn owes_local_settlement(&self) -> bool {
        self.settling.protected_ids().next().is_some()
    }

    pub(crate) async fn retry_local_settlements(&mut self, work: &mut OutboxWork) -> FlushReport {
        let mut report = FlushReport::default();
        self.settling.retry(work, &mut report).await;
        collect_due_writes(work, &mut report).await;
        report
    }

    pub(crate) async fn settle_orphan_answer(
        &mut self,
        work: &mut OutboxWork,
        claim: UnansweredOutboxClaim,
        reply: ReplyPayload,
        report: &mut FlushReport,
    ) {
        self.settling
            .answer(
                work,
                claim.key.request_id,
                claim.caller,
                claim.payload,
                reply,
                report,
            )
            .await;
    }

    pub(crate) async fn return_recovered_claim(
        &mut self,
        work: &mut OutboxWork,
        claim: UnansweredOutboxClaim,
        departure: MemberReturn,
        report: &mut FlushReport,
    ) {
        self.settling
            .return_recovered_claim(work, claim, departure, report)
            .await;
    }

    /// Select an ordered claim for the caller's armed mark, then recheck the window before sending.
    pub async fn prepare_at_mark(
        &mut self,
        work: &mut OutboxWork,
        connection: ConnectionAttempt,
        anchor: WelcomeClockAnchor,
        mark: SyncTime,
    ) -> Result<PreparedFlush, BatchDispatchError> {
        let mut report = FlushReport::default();
        match self
            .prepare_due_through(work, connection, anchor, mark, mark, &mut report)
            .await
        {
            Ok(batch) => Ok(PreparedFlush { batch, report }),
            Err(error) if report.is_empty() => Err(error),
            Err(error) => Err(BatchDispatchError::Interrupted {
                completed: Box::new(report),
                error: Box::new(error),
            }),
        }
    }

    pub(crate) async fn prepare_due_through(
        &mut self,
        work: &mut OutboxWork,
        connection: ConnectionAttempt,
        anchor: WelcomeClockAnchor,
        mark: SyncTime,
        eligible_through: SyncTime,
        report: &mut FlushReport,
    ) -> Result<Option<PreparedWriteBatch>, BatchDispatchError> {
        if self.in_flight.is_some() {
            return Ok(None);
        }
        self.settling.retry(work, report).await;
        let FlushSelection::Claimed(claimed) = work
            .claim_due_through(anchor, mark, eligible_through)
            .await?
        else {
            return Ok(None);
        };
        report.ended.extend(claimed.ended);
        if claimed.writes.is_empty() {
            return Ok(None);
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
                        self.settling
                            .return_member(work, write, MemberReturn::NotTransmitted, report)
                            .await;
                    }
                }
                let send_before = selected
                    .iter()
                    .map(|write| work.write_send_fence(write.sent_at))
                    .fold(send_fence_for_mark(anchor, mark), std::cmp::min);
                if !work.mark_is_open(anchor, mark) || !work.before_elapsed_fence(send_before) {
                    for write in selected {
                        self.settling
                            .return_member(work, write, MemberReturn::NotTransmitted, report)
                            .await;
                    }
                    collect_due_writes(work, report).await;
                    return Ok(None);
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
                    self.settling
                        .return_member(work, write, MemberReturn::NotTransmitted, report)
                        .await;
                }
                collect_due_writes(work, report).await;
                None
            }
        };
        Ok(batch)
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
                    self.settling
                        .return_member(work, write, MemberReturn::NotTransmitted, &mut report)
                        .await;
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
                        self.settling
                            .return_member(
                                work,
                                write,
                                MemberReturn::MayHaveTransmitted,
                                &mut report,
                            )
                            .await;
                    }
                    return Some(report);
                }
                for (write, member) in active.writes.into_iter().zip(results.into_members()) {
                    match member.disposition {
                        WriteBatchMemberDisposition::Answered { reply, .. } => {
                            self.settling
                                .answer(
                                    work,
                                    write.request,
                                    write.caller,
                                    write.payload,
                                    *reply.result,
                                    &mut report,
                                )
                                .await;
                        }
                        WriteBatchMemberDisposition::AnswerLost { .. } => {
                            self.settling
                                .return_member(
                                    work,
                                    write,
                                    MemberReturn::MayHaveTransmitted,
                                    &mut report,
                                )
                                .await;
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

async fn collect_due_writes(work: &mut OutboxWork, report: &mut FlushReport) {
    match work.end_due_writes().await {
        Ok(ended) => report.ended.extend(ended),
        Err(error) => report.issues.push(BatchDispatchError::Work(error)),
    }
}
