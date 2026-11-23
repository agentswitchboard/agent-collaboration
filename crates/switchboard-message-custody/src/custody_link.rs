//! Custody's user of the admitted message lane. The driver owns the socket.

use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, ImmediateRequestCommand, LinkLaneDisposition,
    LinkLaneEvent, ReadyLinkLane, RequestStatusCommand, UntouchedCommand, WelcomeClockAnchor,
    WriteBatchDisposition,
};
use central_link_protocol::{DeliveryMessage, FreshCorrelationId, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxDecision, OutgoingWrite, PendingWrite, StoredSenderOutcome,
};
use workspace_ids::{MessageId, RequestId, SessionRef};
use workspace_model::{
    Caller,
    central_mail::{MessageBody, MessageRecord, MessageStatus},
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
    outbox_dispatch::{BatchDispatchError, FlushReport, OutboxDispatcher, PreparedWriteBatch},
    outbox_flush::{FlushEffect, FlushEvent, FlushTransitionError, OutboxFlushState, transition},
    outbox_work::{OutboxWork, OutboxWorkError},
    receipt_delivery::{ReceiptDeliveryError, ReceiptFrame, ReceiptFrameOutcome},
    recipient_inbox::{InboxCustodyError, InboxOfferResult, RecipientInbox},
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
}

/// The result custody produces after observing an admitted link event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CustodyLinkObservation {
    AdmissionChanged,
    Delivery(InboxOfferResult),
    StaleDelivery,
    DriverClosed,
}

/// Custody owns the stores, one minute scheduler and one consumer of the driver's admitted message lane.
pub struct CustodyLink {
    pub(crate) lane: Arc<dyn ReadyLinkLane>,
    pub(crate) clock: Arc<dyn RuntimeClock>,
    jitter: Arc<dyn FlushJitter>,
    outbox: OutboxWork,
    inbox: RecipientInbox,
    dispatcher: OutboxDispatcher,
    schedule: OutboxFlushState,
    last_clock_anchor: Option<WelcomeClockAnchor>,
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
            inbox,
            dispatcher: OutboxDispatcher::default(),
            schedule: OutboxFlushState::default(),
            last_clock_anchor: None,
        }
    }

    fn admitted_connection(&self) -> Option<AdmittedConnection> {
        self.lane.watch_admitted().borrow().clone()
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
        if let Some(connection) = admitted.as_ref() {
            self.last_clock_anchor = Some(connection.clock_anchor);
        }
        match route {
            SubmissionRoute::Outbox { request, payload } => {
                let now = self.last_clock_anchor.map_or_else(
                    || SyncTime::from_unix_millis(self.outbox.local_now_ms()),
                    |anchor| shared_time_at(anchor, self.clock.monotonic_now()),
                );
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
            Err(_) => {
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
        Ok(self
            .outbox
            .outbox_storage()
            .pending_for(caller)
            .await
            .map_err(OutboxWorkError::Store)?)
    }

    /// Read the sender's local history, scoped to the caller.
    pub async fn outgoing_writes(
        &mut self,
        caller: &Caller,
        limit: u16,
    ) -> Result<Vec<OutgoingWrite>, CustodyLinkError> {
        Ok(self
            .outbox
            .outbox_storage()
            .outgoing_for(caller, limit)
            .await
            .map_err(OutboxWorkError::Store)?)
    }

    /// Read the retained central delivery notice for the caller's own original request.
    pub async fn sender_outcome(
        &mut self,
        caller: &Caller,
        request: &RequestId,
    ) -> Result<Option<StoredSenderOutcome>, CustodyLinkError> {
        Ok(self
            .outbox
            .outbox_storage()
            .sender_outcome(caller, request)
            .await
            .map_err(OutboxWorkError::Store)?)
    }

    /// Drive one link event through the custody-owned inbox.
    pub async fn observe_next_event(&mut self) -> Result<CustodyLinkObservation, CustodyLinkError> {
        let event = self.lane.next_event().await;
        self.observe_event(event).await
    }

    pub(crate) async fn observe_event(
        &mut self,
        event: Option<LinkLaneEvent>,
    ) -> Result<CustodyLinkObservation, CustodyLinkError> {
        let Some(event) = event else {
            self.observe_unavailable()?;
            return Ok(CustodyLinkObservation::DriverClosed);
        };
        match event {
            LinkLaneEvent::ConnectionAdmitted(_) | LinkLaneEvent::ConnectionNotAdmitted { .. } => {
                self.refresh_flush_wake().await?;
                Ok(CustodyLinkObservation::AdmissionChanged)
            }
            LinkLaneEvent::Deliver {
                connection,
                offer,
                sender: _,
            } => {
                if !self
                    .admitted_connection()
                    .is_some_and(|admitted| admitted.connection == connection)
                {
                    return Ok(CustodyLinkObservation::StaleDelivery);
                }
                let notice_key = match offer.message() {
                    DeliveryMessage::System(record)
                        if matches!(record.body, MessageBody::DeliveryOutcome { .. }) =>
                    {
                        Some((record.target.clone(), record.message_id.clone()))
                    }
                    DeliveryMessage::System(_) | DeliveryMessage::Direct(_) => None,
                };
                let kept = self.inbox.receive_offer(offer).await?;
                if let Some((target, notice_id)) = notice_key
                    && matches!(
                        kept,
                        InboxOfferResult::Recorded {
                            decision: InboxDecision::Accepted
                        } | InboxOfferResult::AlreadyDecided {
                            decision: InboxDecision::Accepted
                        }
                    )
                {
                    self.record_sender_notice(&target, &notice_id).await?;
                }
                Ok(CustodyLinkObservation::Delivery(kept))
            }
        }
    }

    async fn record_sender_notice(
        &mut self,
        session: &SessionRef,
        notice_id: &MessageId,
    ) -> Result<(), CustodyLinkError> {
        let retained = self.inbox.waiting_for_session(session).await?;
        let notice = retained
            .iter()
            .find(|notice| &notice.message_id == notice_id)
            .and_then(retained_delivery_notice);
        if let Some((caller, notice)) = notice {
            let now_ms = self.outbox.local_now_ms();
            self.outbox
                .outbox_storage()
                .record_sender_delivery(&caller, &notice, now_ms)
                .await
                .map_err(OutboxWorkError::Store)?;
        }
        Ok(())
    }

    /// A wait for the shared minute selected by the scheduler. `None` means the lane is unavailable or idle.
    pub async fn refresh_flush_wake(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok(None);
        };
        self.last_clock_anchor = Some(admitted.clock_anchor);
        let earliest_due = self.outbox.earliest_waiting_mark().await?;
        self.apply_flush_event(FlushEvent::LinkConnected { earliest_due })?;
        Ok(self.schedule.armed_mark().map(|mark| {
            wake_for_mark(admitted.clock_anchor, mark, self.jitter.jitter_millis(mark))
        }))
    }

    /// An independent deadline for locally ending waiting writes even during a disconnect.
    pub async fn next_write_deadline(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        Ok(self.outbox.earliest_deadline().await?)
    }

    /// Wait on the injected clock for one scheduled flush, if there is work on an admitted link.
    pub async fn flush_next(&mut self) -> Result<Option<FlushReport>, CustodyLinkError> {
        let Some(wake) = self.refresh_flush_wake().await? else {
            return Ok(None);
        };
        self.clock.sleep_until(wake).await;
        Ok(Some(self.flush_writes().await?))
    }

    /// Recover claims after reopen, before any worker for this instance starts a batch.
    pub async fn recover_outbox(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let ended = self.outbox.recover_orphaned_claims().await?;
        Ok(FlushReport {
            ended,
            ..FlushReport::default()
        })
    }

    /// Run the local deadline wake and return the exact typed endings for the caller to report.
    pub async fn end_due_writes(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let ended = self.outbox.end_due_writes().await?;
        Ok(FlushReport {
            ended,
            ..FlushReport::default()
        })
    }

    /// Flush on the current armed minute, with one lane submission and member-by-member settlement.
    pub async fn flush_writes(&mut self) -> Result<FlushReport, CustodyLinkError> {
        let (mut report, batch) = self.prepare_write_turn().await?;
        if let Some(batch) = batch {
            let disposition = self.lane.submit_write_batch(batch.command()).await;
            report.append(self.settle_write_turn(batch, disposition).await?);
        }
        Ok(report)
    }

    pub(crate) async fn prepare_write_turn(
        &mut self,
    ) -> Result<(FlushReport, Option<PreparedWriteBatch>), CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.observe_unavailable()?;
            return Ok((FlushReport::default(), None));
        };
        self.last_clock_anchor = Some(admitted.clock_anchor);
        let now = shared_time_at(admitted.clock_anchor, self.clock.monotonic_now());
        let earliest_due = self.outbox.earliest_waiting_mark().await?;
        self.apply_flush_event(FlushEvent::LinkConnected { earliest_due })?;
        let Some(mark) = self.apply_flush_event(FlushEvent::Tick { now, earliest_due })? else {
            return Ok((FlushReport::default(), None));
        };

        let prepared = self
            .dispatcher
            .prepare_at_mark(
                &mut self.outbox,
                admitted.connection.clone(),
                admitted.clock_anchor,
                mark,
            )
            .await;
        let mut prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.apply_flush_event(FlushEvent::BatchEnded { earliest_due })?;
                return Err(error.into());
            }
        };
        let Some(batch) = prepared.batch else {
            self.release_flush_slot(&mut prepared.report).await?;
            return Ok((prepared.report, None));
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
            let settled = self
                .settle_write_turn(batch, WriteBatchDisposition::NotReached { cause })
                .await?;
            prepared.report.append(settled);
            return Ok((prepared.report, None));
        }
        Ok((prepared.report, Some(batch)))
    }

    pub(crate) async fn settle_write_turn(
        &mut self,
        batch: PreparedWriteBatch,
        disposition: WriteBatchDisposition,
    ) -> Result<FlushReport, CustodyLinkError> {
        let Some(mut report) = self
            .dispatcher
            .settle_batch(&mut self.outbox, batch, disposition)
            .await
        else {
            return Ok(FlushReport::default());
        };
        self.release_flush_slot(&mut report).await?;
        Ok(report)
    }

    fn observe_unavailable(&mut self) -> Result<(), FlushTransitionError> {
        if !self.schedule.batch_in_flight() {
            self.apply_flush_event(FlushEvent::LinkLost)?;
        }
        Ok(())
    }

    async fn release_flush_slot(
        &mut self,
        report: &mut FlushReport,
    ) -> Result<(), FlushTransitionError> {
        let earliest_due = self.outbox.earliest_waiting_mark().await;
        self.apply_flush_event(FlushEvent::BatchEnded {
            earliest_due: earliest_due.as_ref().ok().copied().flatten(),
        })?;
        if let Err(error) = earliest_due {
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
        Ok(self.inbox.prepare_receipts(&admitted.connection).await?)
    }

    pub(crate) async fn settle_receipt_turn(
        &mut self,
        frame: ReceiptFrame,
        disposition: LinkLaneDisposition<CorrelatedReceiptResults>,
    ) -> Result<ReceiptFrameOutcome, CustodyLinkError> {
        let current = self.admitted_connection();
        Ok(self
            .inbox
            .settle_receipts(
                frame,
                current.as_ref().map(|admitted| &admitted.connection),
                disposition,
            )
            .await?)
    }

    pub(crate) fn next_inbox_minute(
        &self,
        last_mark: Option<SyncTime>,
    ) -> Option<(SyncTime, ClockInstant)> {
        let now = self.clock.monotonic_now();
        if let Some(admitted) = self.admitted_connection() {
            let mark = crate::flush_marks::earliest_open_mark(
                shared_time_at(admitted.clock_anchor, now),
                last_mark,
            );
            Some((
                mark,
                wake_for_mark(admitted.clock_anchor, mark, self.jitter.jitter_millis(mark)),
            ))
        } else if self.inbox.needs_local_retry() {
            let local = self.outbox.local_now_ms();
            let mark = crate::flush_marks::next_mark_after(SyncTime::from_unix_millis(local));
            let remaining = mark.unix_millis().saturating_sub(local);
            Some((
                mark,
                now.after(Duration::from_millis(u64::try_from(remaining).unwrap_or(0))),
            ))
        } else {
            None
        }
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
        self.inbox.retry_uncommitted().await
    }

    /// Read typed corruption events on the operator path, independent of session queries.
    pub fn take_operator_events(&mut self) -> Vec<CustodyOperatorEvent> {
        let mut events = self.outbox.take_operator_events();
        events.extend(self.inbox.take_operator_events());
        events
    }

    /// Inspect the messages durably held for this session.
    pub async fn waiting_for_session(
        &mut self,
        session: &SessionRef,
    ) -> Result<Vec<MessageRecord>, CustodyLinkError> {
        Ok(self.inbox.waiting_for_session(session).await?)
    }

    fn apply_flush_event(
        &mut self,
        event: FlushEvent,
    ) -> Result<Option<SyncTime>, FlushTransitionError> {
        let (next, effects) = transition(self.schedule.clone(), event)?;
        self.schedule = next;
        Ok(effects.into_iter().find_map(|effect| match effect {
            FlushEffect::SendBatch { mark } => Some(mark),
            FlushEffect::WakeAt { .. } | FlushEffect::WakeAtLocalDeadline { .. } => None,
        }))
    }
}
