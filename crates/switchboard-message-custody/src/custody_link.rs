//! Custody's user of the admitted message lane. The driver owns the socket.

use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedStatusReply, ImmediateRequestCommand,
    LinkLaneDisposition, ReadyLinkLane, RequestStatusCommand, UntouchedCommand, WelcomeClockAnchor,
    WriteBatchDisposition,
};
use central_link_protocol::{FreshCorrelationId, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    OutboxStoreError, OutgoingWrite, PendingWrite, StoredSenderOutcome,
};
use workspace_ids::{MessageId, RequestId, SessionRef};
use workspace_model::{
    Caller,
    central_mail::{MessageRecord, MessageStatus},
};

use crate::{
    custody_operator::CustodyOperatorEvent,
    custody_status::{
        CustodyRequestStatus, MessageStatusError, PreparedRequestStatus, local_status,
        message_query, message_status_reply, remote_status, retained_delivery_notice, status_key,
    },
    custody_submission::{
        PreparedSubmission, Submission, SubmissionPayload, SubmissionRoute, SubmitRefusal,
        choose_submission_route, prepare_immediate_request, settle_immediate_request,
        submit_to_outbox,
    },
    flush_clock::{FlushJitter, shared_time_at, wake_for_mark},
    flush_marks::mark_in_window,
    offer_taker::{SharedRecipientInbox, TakenEvent, admitted_for, take_event},
    orphan_recovery::{OrphanProbe, OrphanTurn, prepare_orphan_turn, settle_orphan_probe},
    outbox_dispatch::{
        AppliedSenderNotice, BatchDispatchError, FlushReport, OutboxDispatcher, PreparedWriteBatch,
    },
    outbox_flush::{
        FlushEffect, FlushEvent, FlushTransitionError, OutboxFlushState, advance_schedule,
    },
    outbox_work::{OutboxWork, OutboxWorkError},
    receipt_delivery::{ReceiptDeliveryError, ReceiptFrame, ReceiptFrameOutcome},
    recipient_inbox::{InboxCustodyError, RecipientInbox},
};

const IMMEDIATE_COMMAND_WINDOW: Duration = Duration::from_secs(30);

/// A failure produced while custody uses a ready link or its durable stores.
#[derive(Debug, thiserror::Error)]
pub enum CustodyLinkError {
    #[error("the custody task closed before giving an answer")]
    TaskClosed,
    #[error("the outbox is unavailable")]
    Outbox(#[from] OutboxWorkError),
    #[error("the minute scheduler rejected a transition")]
    Schedule(#[from] FlushTransitionError),
    #[error("the outbox batch could not be dispatched")]
    Batch(#[from] BatchDispatchError),
    #[error("the delivery could not be kept")]
    Inbox(#[from] InboxCustodyError),
    #[error("the inbox receipts could not be sent or confirmed")]
    Receipt(#[from] ReceiptDeliveryError),
    #[error("a flush failed after completing local work")]
    InterruptedFlush {
        completed: Box<FlushReport>,
        #[source]
        error: Box<Self>,
    },
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

/// Custody projects durable notices and keeps their inbox payloads until the sink accepts the reports.
#[derive(Default)]
pub(crate) struct SenderNoticeProjection {
    pub(crate) report: FlushReport,
    pub(crate) completable: Vec<MessageId>,
    pub(crate) errors: Vec<CustodyLinkError>,
}

pub use crate::offer_taker::CustodyLinkObservation;

/// Custody owns the stores, one minute scheduler and one consumer of the driver's admitted message lane.
pub struct CustodyLink {
    pub(crate) lane: Arc<dyn ReadyLinkLane>,
    pub(crate) clock: Arc<dyn RuntimeClock>,
    jitter: Arc<dyn FlushJitter>,
    outbox: OutboxWork,
    pub(crate) inbox: SharedRecipientInbox,
    dispatcher: OutboxDispatcher,
    schedule: OutboxFlushState,
    last_clock_anchor: Option<WelcomeClockAnchor>,
    rebased_due_through: Option<SyncTime>,
    drain_connection: Option<central_link_client::ConnectionAttempt>,
    startup_orphan_claims: Option<BTreeSet<RequestId>>,
}

impl CustodyLink {
    #[must_use]
    pub fn new(
        lane: Arc<dyn ReadyLinkLane>,
        clock: Arc<dyn RuntimeClock>,
        jitter: Arc<dyn FlushJitter>,
        outbox: OutboxWork,
        inbox: RecipientInbox,
    ) -> Self {
        Self {
            lane,
            clock,
            jitter,
            outbox,
            inbox: Arc::new(tokio::sync::Mutex::new(inbox)),
            dispatcher: OutboxDispatcher::default(),
            schedule: OutboxFlushState::default(),
            last_clock_anchor: None,
            rebased_due_through: None,
            drain_connection: None,
            startup_orphan_claims: None,
        }
    }

    fn admitted_connection(&self) -> Option<AdmittedConnection> {
        admitted_for(self.lane.as_ref())
    }

    /// Submit through the frozen seam. Only a message or thread post enters the outbox.
    pub async fn submit(&mut self, caller: Caller, payload: SubmissionPayload) -> Submission {
        match self.prepare_submission(caller, payload).await {
            PreparedSubmission::Complete(submission) => submission,
            PreparedSubmission::Immediate(command) => {
                let request = command.request.clone();
                let disposition = self.lane.submit_immediate_request(command).await;
                settle_immediate_request(&request, disposition)
            }
        }
    }

    pub(crate) async fn prepare_submission(
        &mut self,
        caller: Caller,
        payload: SubmissionPayload,
    ) -> PreparedSubmission {
        let route = match choose_submission_route(payload) {
            Ok(route) => route,
            Err(refusal) => return PreparedSubmission::Complete(Submission::Refused(refusal)),
        };
        let admitted = self.admitted_connection();
        match route {
            SubmissionRoute::Outbox { request, payload } => {
                let observed_at = self.clock.monotonic_now();
                let now = match admitted {
                    Some(ready) => self.observe_shared_clock(ready.clock_anchor, observed_at),
                    None => self.prior_shared_time(observed_at),
                };
                let mark = self.schedule.receipt_mark(now);
                let submitted =
                    submit_to_outbox(&mut self.outbox, caller, request, payload, mark).await;
                if let Submission::InOutbox(receipt) = &submitted {
                    // Stored always accepts an event; the next wake also reads the durable row.
                    let _ = self.apply_flush_event(FlushEvent::Stored {
                        flush_at: receipt.flush_at,
                    });
                }
                PreparedSubmission::Complete(submitted)
            }
            SubmissionRoute::Immediate(payload) => {
                let Some(admitted) = admitted else {
                    return PreparedSubmission::Complete(Submission::Refused(
                        SubmitRefusal::CentralUnavailable,
                    ));
                };
                let request =
                    match prepare_immediate_request(caller, payload, self.outbox.local_now_ms()) {
                        Ok(request) => request,
                        Err(refusal) => {
                            return PreparedSubmission::Complete(Submission::Refused(refusal));
                        }
                    };
                PreparedSubmission::Immediate(ImmediateRequestCommand {
                    connection: admitted.connection,
                    send_before: self.clock.monotonic_now().after(IMMEDIATE_COMMAND_WINDOW),
                    request: Box::new(request),
                })
            }
        }
    }

    /// Read the caller's original request result, consulting central only on a local miss.
    pub async fn request_status(
        &mut self,
        caller: Caller,
        query: RequestStatusQuery,
    ) -> CustodyRequestStatus {
        match self.prepare_request_status(caller, query).await {
            PreparedRequestStatus::Complete(status) => status,
            PreparedRequestStatus::Remote(command) => {
                let correlation = command.correlation_id.clone();
                remote_status(&correlation, self.lane.request_status(command).await)
            }
        }
    }

    pub(crate) async fn prepare_request_status(
        &mut self,
        caller: Caller,
        query: RequestStatusQuery,
    ) -> PreparedRequestStatus {
        let stored = self
            .outbox
            .outbox_storage()
            .scoped_request_status(&caller, &status_key(&query))
            .await;
        match stored {
            Ok(status) => {
                if let Some(answer) = local_status(status) {
                    return PreparedRequestStatus::Complete(answer);
                }
            }
            Err(error) => {
                self.outbox.report_point_read_error(&error);
                return PreparedRequestStatus::Complete(CustodyRequestStatus::StatusUnavailable);
            }
        }
        let Some(admitted) = self.admitted_connection() else {
            return PreparedRequestStatus::Complete(CustodyRequestStatus::StatusUnavailable);
        };
        let correlation_id = FreshCorrelationId::mint_at(self.outbox.local_now_ms())
            .wire_id()
            .clone();
        PreparedRequestStatus::Remote(RequestStatusCommand {
            connection: admitted.connection,
            send_before: self.clock.monotonic_now().after(IMMEDIATE_COMMAND_WINDOW),
            correlation_id,
            caller,
            query,
        })
    }

    /// Ask central for one sender-scoped message status without placing a write in the outbox.
    pub async fn message_status(
        &mut self,
        caller: Caller,
        message_id: MessageId,
    ) -> Result<Option<MessageStatus>, MessageStatusError> {
        let submission = self.submit(caller, message_query(message_id)?).await;
        message_status_reply(submission)
    }

    /// Read the sender's local overlay for their unsent and possibly sent writes.
    pub async fn pending_writes(
        &mut self,
        caller: &Caller,
    ) -> Result<Vec<PendingWrite>, CustodyLinkError> {
        let result = self.outbox.outbox_storage().pending_for(caller).await;
        self.report_outbox_read(result)
    }

    /// Read the sender's local history, scoped to the caller.
    pub async fn outgoing_writes(
        &mut self,
        caller: &Caller,
        limit: u16,
    ) -> Result<Vec<OutgoingWrite>, CustodyLinkError> {
        let result = self
            .outbox
            .outbox_storage()
            .outgoing_for(caller, limit)
            .await;
        self.report_outbox_read(result)
    }

    /// Read the retained central delivery notice for the caller's own original request.
    pub async fn sender_outcome(
        &mut self,
        caller: &Caller,
        request: &RequestId,
    ) -> Result<Option<StoredSenderOutcome>, CustodyLinkError> {
        let result = self
            .outbox
            .outbox_storage()
            .sender_outcome(caller, request)
            .await;
        self.report_outbox_read(result)
    }

    fn report_outbox_read<Value>(
        &mut self,
        result: Result<Value, OutboxStoreError>,
    ) -> Result<Value, CustodyLinkError> {
        result.map_err(|error| {
            self.outbox.report_point_read_error(&error);
            OutboxWorkError::Store(error).into()
        })
    }

    pub(crate) fn take_outbox_operator_events(&mut self) -> Vec<CustodyOperatorEvent> {
        self.outbox.take_operator_events()
    }

    /// Drive one delivery using the same save-only path as the offer task.
    pub async fn observe_next_event(&mut self) -> Result<CustodyLinkObservation, CustodyLinkError> {
        let event = self.lane.next_event().await;
        let saved = take_event(&self.inbox, event).await?;
        let sender_notice_saved = saved.sender_notice_saved;
        let observation = self.observe_taken_event(saved).await?;
        if sender_notice_saved
            && let Some(error) = self
                .project_sender_notices()
                .await
                .errors
                .into_iter()
                .next()
        {
            return Err(error);
        }
        Ok(observation)
    }

    pub(crate) async fn observe_taken_event(
        &mut self,
        saved: TakenEvent,
    ) -> Result<CustodyLinkObservation, CustodyLinkError> {
        match &saved.observation {
            CustodyLinkObservation::AdmissionChanged => {
                self.refresh_flush_wake().await?;
            }
            CustodyLinkObservation::DriverClosed => {
                self.observe_unavailable()?;
            }
            CustodyLinkObservation::ConnectionFaulted { connection, cause } => {
                self.outbox
                    .report_operator_event(CustodyOperatorEvent::LinkFaulted {
                        connection: connection.clone(),
                        cause: *cause,
                    });
                if self
                    .admitted_connection()
                    .is_none_or(|ready| ready.connection == *connection)
                {
                    self.observe_unavailable()?;
                }
            }
            CustodyLinkObservation::Delivery { .. }
            | CustodyLinkObservation::LocalRetryCompleted => {}
        }
        Ok(saved.observation)
    }

    pub(crate) async fn project_sender_notices(&mut self) -> SenderNoticeProjection {
        let mut projected = SenderNoticeProjection::default();
        let retained = self.inbox.lock().await.pending_sender_notices().await;
        let records = match retained {
            Ok(records) => records,
            Err(error) => {
                projected.errors.push(error.into());
                return projected;
            }
        };
        for record in records {
            let Some((caller, notice)) = retained_delivery_notice(&record) else {
                continue;
            };
            let applied = match self.apply_sender_notice(&caller, &notice).await {
                Ok(applied) => applied,
                Err(
                    error @ (OutboxStoreError::DeliveryOutcomeMismatch { .. }
                    | OutboxStoreError::ContradictoryFinalOutcome { .. }),
                ) => {
                    projected
                        .report
                        .issues
                        .push(BatchDispatchError::StoreMember {
                            request: notice.request_id.clone(),
                            error,
                        });
                    false
                }
                Err(error) => {
                    projected.errors.push(OutboxWorkError::Store(error).into());
                    continue;
                }
            };
            projected.completable.push(record.message_id);
            if applied {
                projected
                    .report
                    .sender_notices
                    .push(AppliedSenderNotice { caller, notice });
            }
        }
        projected
    }

    pub(crate) async fn complete_sender_notices(
        &mut self,
        ids: Vec<MessageId>,
    ) -> Vec<CustodyLinkError> {
        let mut inbox = self.inbox.lock().await;
        let mut errors = Vec::new();
        for id in ids {
            if let Err(error) = inbox.finish_sender_notice(&id).await {
                errors.push(error.into());
            }
        }
        errors
    }

    async fn apply_sender_notice(
        &mut self,
        caller: &Caller,
        notice: &StoredSenderOutcome,
    ) -> Result<bool, OutboxStoreError> {
        let now_ms = self.outbox.local_now_ms();
        let store = self.outbox.outbox_storage();
        match store.record_sender_delivery(caller, notice, now_ms).await {
            Ok(applied) => Ok(applied),
            Err(error) => match store.sender_outcome(caller, &notice.request_id).await {
                Ok(Some(stored)) if &stored == notice => Ok(true),
                _ => Err(error),
            },
        }
    }

    /// A wait for the shared minute selected by the scheduler. `None` means the lane is unavailable or idle.
    pub async fn refresh_flush_wake(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok(None);
        };
        self.fence_mark_drain(&admitted);
        let observed_at = self.clock.monotonic_now();
        let now = self.observe_shared_clock(admitted.clock_anchor, observed_at);
        let earliest_due = self.earliest_eligible_mark(now).await?;
        self.apply_flush_event(FlushEvent::LinkConnected { now, earliest_due })?;
        Ok(self.schedule.armed_mark().map(|mark| {
            wake_for_mark(admitted.clock_anchor, mark, self.jitter.jitter_millis(mark))
        }))
    }

    /// The independent deadline for waiting writes and the members still held by the driver.
    pub async fn next_write_deadline(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let waiting = self.outbox.earliest_deadline().await?;
        Ok(waiting
            .into_iter()
            .chain(self.active_write_deadline())
            .min())
    }

    pub(crate) fn active_write_deadline(&self) -> Option<ClockInstant> {
        self.dispatcher.active_deadline(&self.outbox)
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
        self.dispatcher
            .end_active_due(&mut self.outbox, report)
            .await?;
        report.ended.extend(self.outbox.end_due_writes().await?);
        Ok(())
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
            let mut retry_cutoff_at = None;
            let mut cutoff_result = Ok(());
            let disposition = loop {
                let deadline = retry_cutoff_at.or_else(|| self.active_write_deadline());
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
                        cutoff_result = self.end_due_writes_into(report).await;
                        retry_cutoff_at = cutoff_result.is_err().then(|| self.local_retry_mark());
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
        let now = self.observe_shared_clock(admitted.clock_anchor, self.clock.monotonic_now());
        let earliest_due = self.earliest_eligible_mark(now).await?;
        self.apply_flush_event(FlushEvent::LinkConnected { now, earliest_due })?;
        let Some(mark) = self.apply_flush_event(FlushEvent::Tick { now, earliest_due })? else {
            return Ok(None);
        };
        self.drain_connection = Some(admitted.connection.clone());

        let eligible_through = self.rebased_due_through.map_or(mark, |old| old.max(mark));
        let prepared = self
            .dispatcher
            .prepare_due_through(
                &mut self.outbox,
                admitted.connection.clone(),
                admitted.clock_anchor,
                mark,
                eligible_through,
                report,
            )
            .await;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.apply_flush_event(FlushEvent::BatchEnded {
                    earliest_due,
                    continue_mark: false,
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
        }
    }

    fn observe_unavailable(&mut self) -> Result<(), FlushTransitionError> {
        if !self.schedule.batch_in_flight() {
            self.apply_flush_event(FlushEvent::LinkLost)?;
        }
        Ok(())
    }

    fn prior_shared_time(&self, observed_at: ClockInstant) -> SyncTime {
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
    fn observe_shared_clock(
        &mut self,
        anchor: WelcomeClockAnchor,
        observed_at: ClockInstant,
    ) -> SyncTime {
        let actual = shared_time_at(anchor, observed_at);
        if self.last_clock_anchor != Some(anchor) {
            let previous = self.prior_shared_time(observed_at);
            if actual < previous {
                let old_eligible = self.schedule.receipt_mark(previous);
                self.rebased_due_through = Some(
                    self.rebased_due_through
                        .map_or(old_eligible, |older| older.max(old_eligible)),
                );
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
            if self.rebased_due_through.is_some_and(|old| mark <= old) {
                return mark.min(self.schedule.receipt_mark(now));
            }
            mark
        }))
    }

    async fn release_flush_slot(
        &mut self,
        report: &mut FlushReport,
        continue_mark: bool,
    ) -> Result<(), FlushTransitionError> {
        let earliest_due = self.outbox.earliest_waiting_mark().await;
        self.apply_flush_event(FlushEvent::BatchEnded {
            earliest_due: earliest_due.as_ref().ok().copied().flatten(),
            continue_mark: continue_mark && earliest_due.as_ref().is_ok_and(Option::is_some),
        })?;
        if let Err(error) = earliest_due {
            report.issues.push(BatchDispatchError::Work(error));
        }
        if let Err(error) = self.outbox.prune_ended_writes().await {
            report.issues.push(BatchDispatchError::Work(error));
        }
        Ok(())
    }

    /// Submit a frame of durable inbox receipts for the current connection.
    pub async fn flush_receipts(
        &mut self,
    ) -> Result<Option<ReceiptFrameOutcome>, CustodyLinkError> {
        let Some(frame) = self.prepare_receipt_turn().await? else {
            return Ok(None);
        };
        let send_before = self.clock.monotonic_now().after(IMMEDIATE_COMMAND_WINDOW);
        let disposition = self.lane.submit_receipts(frame.command(send_before)).await;
        Ok(Some(self.settle_receipt_turn(frame, disposition).await?))
    }

    pub(crate) async fn prepare_receipt_turn(
        &mut self,
    ) -> Result<Option<ReceiptFrame>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            return Ok(None);
        };
        Ok(self
            .inbox
            .lock()
            .await
            .prepare_receipts(&admitted.connection)
            .await?)
    }

    pub(crate) async fn settle_receipt_turn(
        &mut self,
        frame: ReceiptFrame,
        disposition: LinkLaneDisposition<CorrelatedReceiptResults>,
    ) -> Result<ReceiptFrameOutcome, CustodyLinkError> {
        let current = self.admitted_connection();
        Ok(self
            .inbox
            .lock()
            .await
            .settle_receipts(
                frame,
                current.as_ref().map(|admitted| &admitted.connection),
                disposition,
            )
            .await?)
    }

    pub(crate) async fn next_inbox_retention(
        &mut self,
    ) -> Result<Option<ClockInstant>, InboxCustodyError> {
        let deadline = self.inbox.lock().await.next_retention_deadline().await?;
        let now_ms = self.clock.now().unix_millis();
        let observed_at = self.clock.monotonic_now();
        Ok(deadline.map(|deadline| {
            let delay = deadline.saturating_sub(now_ms);
            observed_at.after(Duration::from_millis(u64::try_from(delay).unwrap_or(0)))
        }))
    }

    pub(crate) async fn prune_inbox(&mut self) -> Result<(), InboxCustodyError> {
        self.inbox.lock().await.prune_completed().await
    }

    pub(crate) fn next_receipt_minute(
        &self,
        last_mark: Option<&(central_link_client::ConnectionAttempt, SyncTime)>,
    ) -> Option<(
        (central_link_client::ConnectionAttempt, SyncTime),
        ClockInstant,
    )> {
        let now = self.clock.monotonic_now();
        let admitted = self.admitted_connection()?;
        // A receipt may be retried on a replacement connection. Its minute
        // belongs to that connection's Welcome, independent of local retries.
        let previous = last_mark
            .filter(|(connection, _)| connection == &admitted.connection)
            .map(|(_, mark)| *mark);
        let mark = crate::flush_marks::earliest_open_mark(
            shared_time_at(admitted.clock_anchor, now),
            previous,
        );
        let wake = wake_for_mark(admitted.clock_anchor, mark, self.jitter.jitter_millis(mark));
        Some(((admitted.connection, mark), wake))
    }

    pub(crate) async fn next_intake_retry(&mut self) -> Option<ClockInstant> {
        self.inbox
            .lock()
            .await
            .needs_local_retry()
            .then(|| self.local_retry_mark())
    }

    pub(crate) fn local_retry_mark(&self) -> ClockInstant {
        let now = self.outbox.local_now_ms();
        let next = crate::flush_marks::next_mark_after(SyncTime::from_unix_millis(now));
        let millis = next.unix_millis().saturating_sub(now);
        self.clock
            .monotonic_now()
            .after(Duration::from_millis(u64::try_from(millis).unwrap_or(0)))
    }

    /// Retry offers deferred by local lookup or storage at the next minute wake.
    pub async fn retry_inbox(&mut self) -> Vec<InboxCustodyError> {
        let mut failures = Vec::new();
        let offered = self.inbox.lock().await.take_uncommitted();
        for offer in offered.into_values() {
            if let Err(error) = RecipientInbox::keep_shared_offer(&self.inbox, offer).await {
                failures.push(error);
            }
        }
        failures
    }

    /// Read typed corruption events on the operator path, independent of session queries.
    pub async fn take_operator_events(&mut self) -> Vec<CustodyOperatorEvent> {
        let mut events = self.take_outbox_operator_events();
        events.extend(self.inbox.lock().await.take_operator_events());
        events
    }

    /// Inspect the messages durably held for this session.
    pub async fn waiting_for_session(
        &mut self,
        session: &SessionRef,
    ) -> Result<Vec<MessageRecord>, CustodyLinkError> {
        Ok(self.inbox.lock().await.waiting_for_session(session).await?)
    }

    fn apply_flush_event(
        &mut self,
        event: FlushEvent,
    ) -> Result<Option<SyncTime>, FlushTransitionError> {
        let effects = advance_schedule(&mut self.schedule, event)?;
        Ok(effects.into_iter().find_map(|effect| match effect {
            FlushEffect::SendBatch { mark } => Some(mark),
            FlushEffect::WakeAt { .. } | FlushEffect::WakeAtLocalDeadline { .. } => None,
        }))
    }
}
