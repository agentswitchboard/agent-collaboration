//! Prepares and settles the one outbox batch custody owns at a time.

use central_link_client::{
    ConnectionAttempt, LINK_LANE_FRAME_BYTES, UntouchedCommand, WRITE_BATCH_ENVELOPE_BYTES,
    WelcomeClockAnchor, WriteBatchCommand, WriteBatchDisposition, WriteBatchMemberDisposition,
    encoded_request_size,
};
use central_link_protocol::{CorrelationId, FreshCorrelationId, LinkRequestBatch, ReplyPayload};
use router_clock::ClockInstant;
use runtime_primitives::SyncTime;
use std::collections::BTreeSet;
use switchboard_machine_core_store::{StoredOutboxWrite, UnansweredOutboxClaim};
use workspace_ids::RequestId;

use crate::{
    custody_operator::CustodyOperatorEvent,
    flush_clock::send_fence_for_mark,
    local_settlement::{AnswerAtCutoff, LocalSettlements, MemberReturn},
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

    pub(crate) fn connection(&self) -> &ConnectionAttempt {
        &self.command.connection
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
    ended_at_cutoff: BTreeSet<RequestId>,
}

struct FrameWindow {
    anchor: WelcomeClockAnchor,
    mark: SyncTime,
    eligible_through: SyncTime,
    closes_at: ClockInstant,
}

/// One custody task creates a dispatcher that serializes claims and settles only its current claim.
#[derive(Default)]
pub struct OutboxDispatcher {
    in_flight: Option<ClaimedWriteBatch>,
    settling: LocalSettlements,
    attempted_mark: Option<SyncTime>,
    attempted_requests: BTreeSet<RequestId>,
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

    pub(crate) fn active_deadline(&self, work: &OutboxWork) -> Option<ClockInstant> {
        let batch = self.in_flight.as_ref()?;
        batch.writes.iter()
            .filter(|write| !batch.ended_at_cutoff.contains(&write.request))
            .map(|write| work.write_deadline(write.sent_at))
            .min()
    }

    pub(crate) async fn end_active_due(
        &mut self,
        work: &mut OutboxWork,
        report: &mut FlushReport,
    ) -> Result<(), OutboxWorkError> {
        let Some(batch) = self.in_flight.as_mut() else { return Ok(()); };
        let due: BTreeSet<_> = batch.writes.iter().filter(|write|
            !batch.ended_at_cutoff.contains(&write.request)
                && !work.before_elapsed_fence(work.write_deadline(write.sent_at))
        ).map(|write| write.request.clone()).collect();
        if due.is_empty() {
            return Ok(());
        }
        let ended = work.end_selected_claims(&due).await?;
        batch.ended_at_cutoff.extend(ended.iter().map(|write| write.request_id.clone()));
        report.ended.extend(ended);
        for write in &batch.writes {
            if due.contains(&write.request) && !batch.ended_at_cutoff.contains(&write.request) {
                if let Some(ending) = LocalSettlements::recover_cutoff(work, write).await? {
                    report.ended.push(ending);
                }
                batch.ended_at_cutoff.insert(write.request.clone());
            }
        }
        Ok(())
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
        cutoff: AnswerAtCutoff,
        report: &mut FlushReport,
    ) {
        self.settling
            .answer(work, claim.into(), reply, cutoff, report)
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
        if self.attempted_mark != Some(mark) {
            self.attempted_mark = Some(mark);
            self.attempted_requests.clear();
        }
        // Retrying a returned member in the same mark would starve later writes.
        let mut excluded = self.protected_request_ids();
        excluded.extend(self.attempted_requests.iter().cloned());
        let window = FrameWindow { anchor, mark, eligible_through, closes_at: send_fence_for_mark(anchor, mark) };
        loop {
            match self.claim_frame(work, &connection, &window, &excluded, report).await? {
                ClaimFrame::Prepared(ticket) => return Ok(Some(ticket)),
                ClaimFrame::Finished => return Ok(None),
                ClaimFrame::ExpiredPrefix => excluded.extend(self.settling.protected_ids().cloned()),
            }
        }
    }

    async fn claim_frame(
        &mut self,
        work: &mut OutboxWork,
        connection: &ConnectionAttempt,
        window: &FrameWindow,
        excluded: &BTreeSet<RequestId>,
        report: &mut FlushReport,
    ) -> Result<ClaimFrame, BatchDispatchError> {
        // A return can be committed while its read-back is still failing.
        // Protect its request before selection or expiry can touch that row.
        let FlushSelection::Claimed(claimed) = work
            .claim_due_through(window.anchor, window.mark, window.eligible_through, excluded)
            .await?
        else {
            return Ok(ClaimFrame::Finished);
        };
        report.ended.extend(claimed.ended);
        let Some(first) = claimed.writes.first() else {
            return Ok(ClaimFrame::Finished);
        };

        let now_ms = work.local_now_ms();
        let send_before = window.closes_at.min(work.write_deadline(first.sent_at));
        let eligible_prefix = claimed.writes.partition_point(|write|
            window.closes_at.min(work.write_deadline(write.sent_at)) == send_before
        );
        let batch = match pack_wire_batch(&claimed.writes[..eligible_prefix], now_ms) {
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
                if !work.before_elapsed_fence(send_before) {
                    for write in selected {
                        self.settling
                            .return_member(work, write, MemberReturn::NotTransmitted, report)
                            .await;
                    }
                    collect_due_writes(work, report).await;
                    return Ok(if work.before_elapsed_fence(window.closes_at) {
                        ClaimFrame::ExpiredPrefix
                    } else {
                        ClaimFrame::Finished
                    });
                }
                let ticket = PreparedWriteBatch {
                    command: WriteBatchCommand {
                        connection: connection.clone(),
                        send_before,
                        requests,
                    },
                    token: fresh_correlation(now_ms),
                };
                self.attempted_requests.extend(selected.iter().map(|write| write.request.clone()));
                self.in_flight = Some(ClaimedWriteBatch {
                    ticket: ticket.clone(),
                    writes: selected,
                    ended_at_cutoff: BTreeSet::new(),
                });
                ClaimFrame::Prepared(ticket)
            }
            Err(error) => {
                report.issues.push(error);
                for write in claimed.writes {
                    self.settling
                        .return_member(work, write, MemberReturn::NotTransmitted, report)
                        .await;
                }
                collect_due_writes(work, report).await;
                ClaimFrame::Finished
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
                    if active.ended_at_cutoff.contains(&write.request) {
                        continue;
                    }
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
                        if active.ended_at_cutoff.contains(&write.request) {
                            continue;
                        }
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
                        WriteBatchMemberDisposition::Answered { reply, accepted_at } => {
                            let cutoff = if active.ended_at_cutoff.contains(&write.request) {
                                AnswerAtCutoff::StatusOnly
                            } else {
                                AnswerAtCutoff::new(accepted_at, work.write_deadline(write.sent_at))
                            };
                            self.settling
                                .answer(work, write.into(), *reply.result, cutoff, &mut report)
                                .await;
                        }
                        WriteBatchMemberDisposition::AnswerLost { .. } => {
                            if active.ended_at_cutoff.contains(&write.request) {
                                continue;
                            }
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

enum ClaimFrame {
    Prepared(PreparedWriteBatch),
    ExpiredPrefix,
    Finished,
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
