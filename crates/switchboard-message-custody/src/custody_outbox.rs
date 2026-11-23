//! Custody selects, submits and settles outbox frames and independently ends overdue writes.

use central_link_client::{
    AdmittedConnection, CorrelatedStatusReply, LinkLaneDisposition, UntouchedCommand,
    WelcomeClockAnchor, WriteBatchDisposition,
};
use router_clock::ClockInstant;
use runtime_primitives::SyncTime;
use std::{collections::BTreeSet, time::Duration};
use workspace_ids::RequestId;

use crate::{
    flush_clock::{shared_time_at, wake_for_mark},
    flush_marks::mark_in_window,
    orphan_recovery::{OrphanProbe, OrphanTurn, prepare_orphan_turn, settle_orphan_probe},
    outbox_dispatch::{BatchDispatchError, FlushReport, FrameWindow, PreparedWriteBatch},
    outbox_flush::{FlushEvent, FlushTransitionError, RebasedMarks},
    outbox_work::OutboxWorkError,
};

use super::{CustodyLink, CustodyLinkError, finish_flush};

const WRITE_TURN_RETRY: Duration = Duration::from_millis(10);

impl CustodyLink {
    /// A wait for the shared minute selected by the scheduler. `None` means the lane is unavailable or idle.
    pub async fn refresh_flush_wake(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok(None);
        };
        self.fence_mark_drain(&admitted);
        let observed_at = self.clock.monotonic_now();
        if let Some(retry) = self.write_retry_at.filter(|retry| observed_at < *retry) {
            return Ok(Some(retry));
        }
        self.write_retry_at = None;
        let now = self.observe_shared_clock(admitted.clock_anchor, observed_at);
        let earliest_due = self.read_due_for_mark(now).await?;
        self.apply_flush_event(FlushEvent::LinkConnected { now, earliest_due })?;
        Ok(self.schedule.armed_mark().map(|mark| {
            wake_for_mark(admitted.clock_anchor, mark, self.jitter.jitter_millis(mark))
        }))
    }

    pub(crate) fn outbox_retry_wake(&self) -> Option<ClockInstant> {
        self.write_retry_at
    }

    fn defer_write_turn(&mut self) {
        self.write_retry_at = Some(self.clock.monotonic_now().after(WRITE_TURN_RETRY));
    }

    /// The independent deadline for waiting writes and the members still held by the driver.
    pub async fn next_write_deadline(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let (wake, fault) = self.project_write_cutoff_wake().await;
        fault.map_or(Ok(wake), |error| Err(error.into()))
    }

    pub(crate) async fn project_write_cutoff_wake(
        &mut self,
    ) -> (Option<ClockInstant>, Option<OutboxWorkError>) {
        let fault = self.waiting_cutoffs.refresh(&mut self.outbox).await;
        let wake = self
            .waiting_cutoffs
            .next_wake()
            .into_iter()
            .chain(self.dispatcher.active_deadline(&self.outbox))
            .min();
        (wake, fault)
    }

    /// Wait on the injected clock for one scheduled flush, if there is work on an admitted link.
    pub async fn flush_next(&mut self) -> Result<Option<FlushReport>, CustodyLinkError> {
        let Some(wake) = self.refresh_flush_wake().await? else {
            return Ok(None);
        };
        self.clock.sleep_until(wake).await;
        Ok(Some(self.flush_writes().await?))
    }

    /// Drive an orphan recovery pass without the command worker, using the same scoped status path.
    pub async fn recover_outbox(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let mut report = FlushReport::default();
        let result = self.recover_orphans_into(&mut report).await;
        finish_flush(report, result)
    }

    async fn recover_orphans_into(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<(), CustodyLinkError> {
        let mut checked = BTreeSet::new();
        loop {
            let turn = self.prepare_recovery_turn(true, &checked).await?;
            report.append(turn.report);
            let Some(probe) = turn.probe else {
                return Ok(());
            };
            checked.insert(probe.claim.key.request_id.clone());
            let result = self.lane.request_status(probe.command.clone()).await;
            report.append(self.settle_recovery_probe(probe, result, false).await);
        }
    }

    pub(crate) async fn prepare_recovery_turn(
        &mut self,
        can_probe: bool,
        deferred: &BTreeSet<RequestId>,
    ) -> Result<OrphanTurn, OutboxWorkError> {
        let ready = can_probe.then(|| self.admitted_connection()).flatten();
        prepare_orphan_turn(
            &mut self.outbox,
            &mut self.dispatcher,
            &mut self.startup_orphan_claims,
            ready,
            self.clock.monotonic_now(),
            deferred,
        )
        .await
    }

    pub(crate) fn has_startup_claim_inventory(&self) -> bool {
        self.startup_orphan_claims.is_some()
    }

    pub(crate) async fn settle_recovery_probe(
        &mut self,
        probe: OrphanProbe,
        result: LinkLaneDisposition<CorrelatedStatusReply>,
        cutoff_reported: bool,
    ) -> FlushReport {
        settle_orphan_probe(
            &mut self.outbox,
            &mut self.dispatcher,
            probe,
            result,
            cutoff_reported,
        )
        .await
    }

    pub(crate) fn owes_local_settlement(&self) -> bool {
        self.dispatcher.owes_local_settlement()
    }

    pub(crate) async fn retry_local_settlement(&mut self) -> FlushReport {
        self.dispatcher
            .retry_local_settlements(&mut self.outbox)
            .await
    }

    /// Run the local deadline wake and return the exact typed endings for the caller to report.
    pub async fn end_due_writes(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let mut report = FlushReport::default();
        let result = self.end_due_writes_into(&mut report).await;
        finish_flush(report, result)
    }

    async fn end_due_writes_into(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<(), CustodyLinkError> {
        let active = self
            .dispatcher
            .end_active_due(&mut self.outbox, report)
            .await;
        let waiting = self.waiting_cutoffs.end_due(&mut self.outbox).await;
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

    /// Drain the current minute while it remains open, settling one batch at a time.
    pub async fn flush_writes(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let mut report = FlushReport::default();
        let result = self.flush_writes_into(&mut report).await;
        finish_flush(report, result)
    }

    async fn flush_writes_into(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<(), CustodyLinkError> {
        if self.owes_local_settlement() {
            report.append(self.retry_local_settlement().await);
        }
        self.recover_orphans_into(report).await?;
        let lane = self.lane.clone();
        let clock = self.clock.clone();
        while let Some(batch) = self.prepare_write_turn(report).await? {
            let current_mark = self.schedule.draining_mark();
            let mut answering = lane.submit_write_batch(batch.command());
            let mut cutoff_result = Ok(());
            let disposition = loop {
                let (deadline, fault) = self.project_write_cutoff_wake().await;
                if let Some(error) = fault {
                    report.issues.push(BatchDispatchError::Work(error));
                }
                tokio::select! {
                    biased;
                    outcome = &mut answering => break outcome,
                    () = async {
                        if let Some(deadline) = deadline {
                            clock.sleep_until(deadline).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    } => {
                        let ending = self.end_due_writes_into(report).await;
                        cutoff_result = cutoff_result.and(ending);
                    }
                }
            };
            self.settle_write_turn(batch, disposition, report).await?;
            cutoff_result?;
            if !self.admitted_connection().is_some_and(|admitted| {
                current_mark == self.schedule.draining_mark()
                    && current_mark
                        == mark_in_window(shared_time_at(
                            admitted.clock_anchor,
                            clock.monotonic_now(),
                        ))
            }) {
                break;
            }
        }
        Ok(())
    }

    pub(crate) async fn prepare_write_turn(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<Option<PreparedWriteBatch>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok(None);
        };
        self.fence_mark_drain(&admitted);
        if self
            .write_retry_at
            .is_some_and(|retry| self.clock.monotonic_now() < retry)
        {
            return Ok(None);
        }
        self.write_retry_at = None;
        let now = self.observe_shared_clock(admitted.clock_anchor, self.clock.monotonic_now());
        let earliest_due = self.read_due_for_mark(now).await?;
        self.apply_flush_event(FlushEvent::LinkConnected { now, earliest_due })?;
        let Some(mark) = self.apply_flush_event(FlushEvent::Tick { now, earliest_due })? else {
            return Ok(None);
        };
        self.drain_connection = Some(admitted.connection.clone());

        let (eligible_through, deferred_requests) = self.rebased_marks.as_mut().map_or_else(
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
            .dispatcher
            .prepare_due_through(
                &mut self.outbox,
                admitted.connection.clone(),
                window,
                report,
            )
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.defer_write_turn();
                self.apply_flush_event(FlushEvent::BatchEnded {
                    earliest_due,
                    continue_mark: true,
                })?;
                return Err(error.into());
            }
        };
        let Some(batch) = prepared else {
            self.release_flush_slot(report, false).await?;
            return Ok(None);
        };
        let cause = if self.admitted_connection().as_ref() != Some(&admitted) {
            Some(UntouchedCommand::ConnectionUnavailable)
        } else if !self
            .outbox
            .before_elapsed_fence(batch.command().send_before)
        {
            Some(UntouchedCommand::SendWindowElapsed)
        } else {
            None
        };
        if let Some(cause) = cause {
            if let Some(settled) = self
                .dispatcher
                .return_unsubmitted(&mut self.outbox, batch, cause)
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
            .dispatcher
            .settle_batch(&mut self.outbox, batch, disposition)
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

    fn fence_mark_drain(&mut self, admitted: &AdmittedConnection) {
        if self
            .drain_connection
            .as_ref()
            .is_some_and(|connection| connection != &admitted.connection)
        {
            self.schedule.stop_draining_mark();
            self.drain_connection = None;
            self.write_retry_at = None;
        }
    }

    pub(super) fn observe_unavailable(&mut self) -> Result<(), FlushTransitionError> {
        if !self.schedule.batch_in_flight() {
            self.apply_flush_event(FlushEvent::LinkLost)?;
        }
        self.write_retry_at = None;
        Ok(())
    }

    pub(super) fn prior_shared_time(&self, observed_at: ClockInstant) -> SyncTime {
        self.last_clock_anchor.map_or_else(
            || SyncTime::from_unix_millis(self.outbox.local_time_at(observed_at)),
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
        if self.last_clock_anchor != Some(anchor) {
            let previous = self.prior_shared_time(observed_at);
            if actual < previous {
                let old_eligible = self.schedule.receipt_mark(previous);
                let frontier = self
                    .rebased_marks
                    .as_ref()
                    .map_or(old_eligible, |older| older.due_through.max(old_eligible));
                self.rebased_marks = Some(RebasedMarks::new(frontier));
            }
            self.last_clock_anchor = Some(anchor);
        }
        actual
    }

    async fn earliest_eligible_mark(
        &mut self,
        now: SyncTime,
    ) -> Result<Option<SyncTime>, OutboxWorkError> {
        let due = self.outbox.earliest_waiting_mark().await?;
        Ok(due.map(|mark| {
            if self
                .rebased_marks
                .as_ref()
                .is_some_and(|old| mark <= old.due_through)
            {
                let eligible = self
                    .schedule
                    .draining_mark()
                    .filter(|draining| mark_in_window(now) == Some(*draining))
                    .unwrap_or_else(|| self.schedule.receipt_mark(now));
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
        let earliest_due = match self.outbox.earliest_waiting_mark().await {
            Ok(due) => due,
            Err(error) => {
                report.issues.push(BatchDispatchError::Work(error));
                self.defer_write_turn();
                self.schedule.draining_mark()
            }
        };
        self.apply_flush_event(FlushEvent::BatchEnded {
            earliest_due,
            continue_mark: continue_mark && earliest_due.is_some(),
        })?;
        if let Err(error) = self.outbox.prune_ended_writes().await {
            report.issues.push(BatchDispatchError::Work(error));
        }
        Ok(())
    }
}
