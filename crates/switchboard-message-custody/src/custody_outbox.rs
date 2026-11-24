//! The outbox driver selects, submits and settles frames, and ends overdue writes.

use central_link_client::{
    AdmittedConnection, CorrelatedStatusReply, LinkLaneDisposition, UntouchedCommand,
    WelcomeClockAnchor, WriteBatchDisposition,
};
use router_clock::ClockInstant;
use runtime_primitives::SyncTime;
use std::{collections::BTreeSet, time::Duration};
use workspace_ids::RequestId;

use crate::{
    custody_submission::{PreparedSubmission, Submission, submit_to_outbox},
    flush_clock::{shared_time_at, wake_for_mark},
    flush_marks::mark_in_window,
};
use switchboard_machine_core_store::OutboxQueuedWrite;
use workspace_model::Caller;

#[path = "outbox_driver/batch.rs"]
mod batch;
#[path = "outbox_driver/claim_recovery.rs"]
mod claim_recovery;
#[path = "outbox_driver/recovery.rs"]
mod recovery;
#[path = "outbox_driver/report.rs"]
mod report;
#[path = "outbox_driver/schedule.rs"]
mod schedule;
#[path = "outbox_driver/settlement.rs"]
mod settlement;
#[path = "outbox_driver/work.rs"]
mod work;

pub use batch::{OutboxDispatcher, PreparedFlush, PreparedWriteBatch};
pub(crate) use recovery::OrphanProbe;
pub use report::{AppliedSenderNotice, BatchDispatchError, FlushReport, FlushedWriteAnswer};
pub use schedule::{FlushEffect, FlushEvent, FlushTransitionError, OutboxFlushState, transition};
pub use work::{FlushSelection, OutboxWork, OutboxWorkError};

use batch::FrameWindow;
use claim_recovery::{ClaimRecovery, ClaimRecoveryEvent};
use recovery::{OrphanTurn, prepare_orphan_turn, settle_orphan_probe};
use schedule::advance_schedule;
use work::{RebasedMarks, WaitingCutoffSchedule};

use super::{CustodyLink, CustodyLinkError};

const WRITE_TURN_RETRY: Duration = Duration::from_millis(10);

pub(super) struct OutboxDriver {
    pub(super) work: OutboxWork,
    dispatcher: OutboxDispatcher,
    waiting_cutoffs: WaitingCutoffSchedule,
    schedule: OutboxFlushState,
    write_retry_at: Option<ClockInstant>,
    claim_recovery: ClaimRecovery,
    last_clock_anchor: Option<WelcomeClockAnchor>,
    rebased_marks: Option<RebasedMarks>,
    drain_connection: Option<central_link_client::ConnectionAttempt>,
    startup_orphan_claims: Option<BTreeSet<RequestId>>,
}

impl OutboxDriver {
    pub(super) fn new(work: OutboxWork) -> Self {
        Self {
            work,
            dispatcher: OutboxDispatcher::default(),
            waiting_cutoffs: WaitingCutoffSchedule::default(),
            schedule: OutboxFlushState::default(),
            write_retry_at: None,
            claim_recovery: ClaimRecovery::Clear,
            last_clock_anchor: None,
            rebased_marks: None,
            drain_connection: None,
            startup_orphan_claims: None,
        }
    }
}

fn finish_flush(
    report: FlushReport,
    result: Result<(), CustodyLinkError>,
) -> Result<FlushReport, CustodyLinkError> {
    match result {
        Ok(()) => Ok(report),
        Err(error) if report.is_empty() => Err(error),
        Err(error) => Err(CustodyLinkError::InterruptedFlush {
            completed: Box::new(report),
            error: Box::new(error),
        }),
    }
}

impl CustodyLink {
    pub(super) async fn queue_outbox_write(
        &mut self,
        admitted: Option<AdmittedConnection>,
        caller: Caller,
        request: RequestId,
        payload: OutboxQueuedWrite,
    ) -> PreparedSubmission {
        let observed_at = self.clock.monotonic_now();
        let now = match admitted {
            Some(ready) => self.observe_shared_clock(ready.clock_anchor, observed_at),
            None => self.prior_shared_time(observed_at),
        };
        let mark = self.outbox_driver.schedule.receipt_mark(now);
        let now_ms = self.outbox_driver.work.local_now_ms();
        let (submitted, new_request) = submit_to_outbox(
            self.outbox_driver.work.outbox_storage(),
            now_ms,
            caller,
            request,
            payload,
            mark,
        )
        .await
        .into_reply();
        if let (Some(rebased), Some(request)) = (&mut self.outbox_driver.rebased_marks, new_request)
        {
            rebased.remember_admission(request, mark);
        }
        if let Submission::InOutbox(receipt) = &submitted {
            // Stored always accepts an event; the next wake also reads the durable row.
            let _ = self.apply_flush_event(FlushEvent::Stored {
                flush_at: receipt.flush_at,
            });
        }
        PreparedSubmission::Complete(submitted)
    }

    fn apply_flush_event(
        &mut self,
        event: FlushEvent,
    ) -> Result<Option<SyncTime>, FlushTransitionError> {
        let effects = advance_schedule(&mut self.outbox_driver.schedule, event)?;
        Ok(effects.into_iter().find_map(|effect| match effect {
            FlushEffect::SendBatch { mark } => Some(mark),
            FlushEffect::WakeAt { .. } => None,
        }))
    }
    /// A wait for the shared minute selected by the scheduler. `None` means the lane is unavailable or idle.
    pub(crate) async fn refresh_flush_wake(
        &mut self,
    ) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok(None);
        };
        self.fence_mark_drain(&admitted)?;
        let observed_at = self.clock.monotonic_now();
        let now = self.observe_shared_clock(admitted.clock_anchor, observed_at);
        if self.outbox_driver.schedule.draining_mark() != mark_in_window(now)
            || self.outbox_driver.schedule.draining_mark().is_none()
        {
            self.outbox_driver
                .claim_recovery
                .observe(ClaimRecoveryEvent::DrainReset);
        }
        if matches!(
            self.outbox_driver.claim_recovery,
            ClaimRecovery::ParkedForScan | ClaimRecovery::ParkedForReturn
        ) {
            return Ok(None);
        }
        if let Some(retry) = self
            .outbox_driver
            .write_retry_at
            .filter(|retry| observed_at < *retry)
        {
            return Ok(Some(retry));
        }
        self.outbox_driver.write_retry_at = None;
        let earliest_due = self.read_due_for_mark(now).await?;
        self.apply_flush_event(FlushEvent::LinkConnected { now, earliest_due })?;
        Ok(self.outbox_driver.schedule.armed_mark().map(|mark| {
            wake_for_mark(admitted.clock_anchor, mark, self.jitter.jitter_millis(mark))
        }))
    }

    pub(crate) fn outbox_retry_wake(&self) -> Option<ClockInstant> {
        self.outbox_driver.write_retry_at
    }

    fn defer_write_turn(&mut self) {
        self.outbox_driver.write_retry_at =
            Some(self.clock.monotonic_now().after(WRITE_TURN_RETRY));
    }

    pub(crate) async fn project_write_cutoff_wake(
        &mut self,
    ) -> (Option<ClockInstant>, Option<OutboxWorkError>) {
        let fault = self
            .outbox_driver
            .waiting_cutoffs
            .refresh(&mut self.outbox_driver.work)
            .await;
        let wake = self
            .outbox_driver
            .waiting_cutoffs
            .next_wake()
            .into_iter()
            .chain(
                self.outbox_driver
                    .dispatcher
                    .active_deadline(&self.outbox_driver.work),
            )
            .min();
        (wake, fault)
    }

    pub(crate) fn retire_receipt_protections(&mut self, report: &FlushReport) {
        if let Some(rebased) = &mut self.outbox_driver.rebased_marks {
            for request in report
                .answered
                .iter()
                .map(|answer| &answer.request)
                .chain(report.ended.iter().map(|ending| &ending.request_id))
                .chain(
                    report
                        .sender_notices
                        .iter()
                        .map(|applied| &applied.notice.request_id),
                )
            {
                rebased.forget_completed(request);
            }
        }
    }

    pub(crate) async fn prepare_recovery_turn(
        &mut self,
        can_probe: bool,
        deferred: &BTreeSet<RequestId>,
    ) -> Result<OrphanTurn, OutboxWorkError> {
        let ready = can_probe.then(|| self.admitted_connection()).flatten();
        let turn = prepare_orphan_turn(
            &mut self.outbox_driver.work,
            &mut self.outbox_driver.dispatcher,
            &mut self.outbox_driver.startup_orphan_claims,
            ready,
            self.clock.monotonic_now(),
            deferred,
        )
        .await?;
        self.outbox_driver
            .claim_recovery
            .observe(ClaimRecoveryEvent::ScanFinished {
                owes_local_return: self.outbox_driver.dispatcher.owes_local_settlement(),
            });
        Ok(turn)
    }

    pub(crate) fn has_startup_claim_inventory(&self) -> bool {
        self.outbox_driver.startup_orphan_claims.is_some()
    }

    pub(crate) async fn settle_recovery_probe(
        &mut self,
        probe: OrphanProbe,
        result: LinkLaneDisposition<CorrelatedStatusReply>,
        cutoff_reported: bool,
    ) -> FlushReport {
        settle_orphan_probe(
            &mut self.outbox_driver.work,
            &mut self.outbox_driver.dispatcher,
            probe,
            result,
            cutoff_reported,
        )
        .await
    }

    pub(crate) fn owes_local_settlement(&self) -> bool {
        self.outbox_driver.dispatcher.owes_local_settlement()
    }

    pub(crate) async fn retry_local_settlement(&mut self) -> FlushReport {
        let report = self
            .outbox_driver
            .dispatcher
            .retry_local_settlements(&mut self.outbox_driver.work)
            .await;
        self.outbox_driver
            .claim_recovery
            .observe(ClaimRecoveryEvent::LocalRetryFinished {
                owes_local_return: self.outbox_driver.dispatcher.owes_local_settlement(),
            });
        report
    }

    /// Run the local deadline wake and return the exact typed endings for the caller to report.
    pub(crate) async fn end_due_writes(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let mut report = FlushReport::default();
        let result = self.end_due_writes_into(&mut report).await;
        finish_flush(report, result)
    }

    async fn end_due_writes_into(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<(), CustodyLinkError> {
        let active = self
            .outbox_driver
            .dispatcher
            .end_active_due(&mut self.outbox_driver.work, report)
            .await;
        let waiting = self
            .outbox_driver
            .waiting_cutoffs
            .end_due(&mut self.outbox_driver.work)
            .await;
        match (active, waiting) {
            (active, Ok(ended)) => {
                report.ended.extend(ended);
                active.map_err(Into::into)
            }
            (Ok(()), Err(error)) => Err(error.into()),
            (Err(active), Err(waiting)) => {
                report.issues.push(BatchDispatchError::Work(waiting));
                Err(active.into())
            }
        }
    }

    pub(crate) async fn prepare_write_turn(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<Option<PreparedWriteBatch>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok(None);
        };
        self.fence_mark_drain(&admitted)?;
        if self
            .outbox_driver
            .write_retry_at
            .is_some_and(|retry| self.clock.monotonic_now() < retry)
        {
            return Ok(None);
        }
        self.outbox_driver.write_retry_at = None;
        let now = self.observe_shared_clock(admitted.clock_anchor, self.clock.monotonic_now());
        let earliest_due = self.read_due_for_mark(now).await?;
        self.apply_flush_event(FlushEvent::LinkConnected { now, earliest_due })?;
        let Some(mark) = self.apply_flush_event(FlushEvent::Tick { now, earliest_due })? else {
            return Ok(None);
        };
        self.outbox_driver.drain_connection = Some(admitted.connection.clone());

        let (eligible_through, deferred_requests) =
            self.outbox_driver.rebased_marks.as_ref().map_or_else(
                || (mark, BTreeSet::new()),
                |rebased| {
                    (
                        rebased.due_through.max(mark),
                        rebased.defer_fresh_receipts(mark),
                    )
                },
            );
        let window = FrameWindow::new(
            admitted.clock_anchor,
            mark,
            eligible_through,
            deferred_requests,
        );
        let prepared = self
            .outbox_driver
            .dispatcher
            .prepare_due_through(
                &mut self.outbox_driver.work,
                admitted.connection.clone(),
                window,
                report,
            )
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.outbox_driver
                    .claim_recovery
                    .observe(ClaimRecoveryEvent::ClaimFailed);
                self.defer_write_turn();
                self.apply_flush_event(FlushEvent::BatchEnded {
                    earliest_due,
                    continue_mark: true,
                })?;
                return Err(error.into());
            }
        };
        let Some(batch) = prepared else {
            self.outbox_driver
                .claim_recovery
                .observe(ClaimRecoveryEvent::ClaimEmpty);
            self.release_flush_slot(
                report,
                self.outbox_driver.claim_recovery != ClaimRecovery::Clear,
            )
            .await?;
            return Ok(None);
        };
        let cause = if self.admitted_connection().as_ref() != Some(&admitted) {
            Some(UntouchedCommand::ConnectionUnavailable)
        } else if !self
            .outbox_driver
            .work
            .before_elapsed_fence(batch.command().send_before)
        {
            Some(UntouchedCommand::SendWindowElapsed)
        } else {
            None
        };
        if let Some(cause) = cause {
            if let Some(settled) = self
                .outbox_driver
                .dispatcher
                .settle_batch(
                    &mut self.outbox_driver.work,
                    batch,
                    WriteBatchDisposition::NotReached { cause },
                )
                .await
            {
                report.append(settled);
                self.release_flush_slot(report, cause == UntouchedCommand::SendWindowElapsed)
                    .await?;
            }
            return Ok(None);
        }
        Ok(Some(batch))
    }

    pub(crate) async fn settle_write_turn(
        &mut self,
        batch: PreparedWriteBatch,
        disposition: WriteBatchDisposition,
        report: &mut FlushReport,
    ) -> Result<(), CustodyLinkError> {
        let connection = batch.connection().clone();
        let untouched = matches!(&disposition, WriteBatchDisposition::NotReached { .. });
        let Some(settled) = self
            .outbox_driver
            .dispatcher
            .settle_batch(&mut self.outbox_driver.work, batch, disposition)
            .await
        else {
            return Ok(());
        };
        report.append(settled);
        let continue_mark = self
            .admitted_connection()
            .is_some_and(|current| current.connection == connection);
        if continue_mark && untouched {
            self.defer_write_turn();
        }
        self.release_flush_slot(report, continue_mark).await?;
        Ok(())
    }

    fn fence_mark_drain(
        &mut self,
        admitted: &AdmittedConnection,
    ) -> Result<(), FlushTransitionError> {
        if self
            .outbox_driver
            .drain_connection
            .as_ref()
            .is_some_and(|connection| connection != &admitted.connection)
        {
            self.apply_flush_event(FlushEvent::DrainFenced)?;
            self.outbox_driver.drain_connection = None;
            self.outbox_driver.write_retry_at = None;
            self.outbox_driver
                .claim_recovery
                .observe(ClaimRecoveryEvent::DrainReset);
        }
        Ok(())
    }

    pub(super) fn observe_unavailable(&mut self) -> Result<(), FlushTransitionError> {
        if !self.outbox_driver.schedule.batch_in_flight() {
            self.apply_flush_event(FlushEvent::LinkLost)?;
        }
        self.outbox_driver.write_retry_at = None;
        self.outbox_driver
            .claim_recovery
            .observe(ClaimRecoveryEvent::DrainReset);
        Ok(())
    }

    pub(super) fn prior_shared_time(&self, observed_at: ClockInstant) -> SyncTime {
        self.outbox_driver.last_clock_anchor.map_or_else(
            || SyncTime::from_unix_millis(self.outbox_driver.work.local_time_at(observed_at)),
            |anchor| shared_time_at(anchor, observed_at),
        )
    }

    /// When Welcome moves backwards, old receipt marks through the previous
    /// grid's first eligible mark are due on the next unused open window of
    /// the new grid. Keep their original durable receipts and deadlines.
    /// Already-used windows remain consumed for this worker, even if used on
    /// an earlier connection. If none can be used in time, normal expiry applies.
    pub(super) fn observe_shared_clock(
        &mut self,
        anchor: WelcomeClockAnchor,
        observed_at: ClockInstant,
    ) -> SyncTime {
        let actual = shared_time_at(anchor, observed_at);
        if self.outbox_driver.last_clock_anchor != Some(anchor) {
            let previous = self.prior_shared_time(observed_at);
            if actual < previous {
                let old_eligible = self.outbox_driver.schedule.receipt_mark(previous);
                let rebased = self
                    .outbox_driver
                    .rebased_marks
                    .get_or_insert_with(|| RebasedMarks::new(old_eligible));
                rebased.due_through = rebased.due_through.max(old_eligible);
            }
            self.outbox_driver.last_clock_anchor = Some(anchor);
        }
        actual
    }

    async fn earliest_eligible_mark(
        &mut self,
        now: SyncTime,
    ) -> Result<Option<SyncTime>, OutboxWorkError> {
        let due = self.outbox_driver.work.earliest_waiting_mark().await?;
        Ok(due.map(|mark| {
            if self
                .outbox_driver
                .rebased_marks
                .as_ref()
                .is_some_and(|old| mark <= old.due_through)
            {
                let eligible = self
                    .outbox_driver
                    .schedule
                    .draining_mark()
                    .filter(|draining| mark_in_window(now) == Some(*draining))
                    .unwrap_or_else(|| self.outbox_driver.schedule.receipt_mark(now));
                return mark.min(eligible);
            }
            mark
        }))
    }

    async fn read_due_for_mark(
        &mut self,
        now: SyncTime,
    ) -> Result<Option<SyncTime>, OutboxWorkError> {
        let due = self.earliest_eligible_mark(now).await;
        if due.is_err() {
            self.defer_write_turn();
        }
        due
    }

    async fn release_flush_slot(
        &mut self,
        report: &mut FlushReport,
        continue_mark: bool,
    ) -> Result<(), FlushTransitionError> {
        let earliest_due = match self.outbox_driver.work.earliest_waiting_mark().await {
            Ok(due) => due,
            Err(error) => {
                report.issues.push(BatchDispatchError::Work(error));
                self.defer_write_turn();
                self.outbox_driver.schedule.draining_mark()
            }
        };
        self.apply_flush_event(FlushEvent::BatchEnded {
            earliest_due,
            continue_mark: continue_mark
                && (earliest_due.is_some()
                    || self.outbox_driver.claim_recovery != ClaimRecovery::Clear),
        })?;
        if let Err(error) = self.outbox_driver.work.prune_ended_writes().await {
            report.issues.push(BatchDispatchError::Work(error));
        }
        Ok(())
    }
}
