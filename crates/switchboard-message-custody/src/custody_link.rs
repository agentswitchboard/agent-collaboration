//! Custody's user of the admitted message lane. The driver owns the socket.

use central_link_client::{
    AdmittedConnection, LinkLaneCommand, LinkLaneEvent, ReadyLinkLane, WelcomeClockAnchor,
    WriteBatchDisposition,
};
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::sync::Arc;
use workspace_ids::SessionRef;
use workspace_model::{Caller, central_mail::MessageRecord};

use crate::{
    custody_operator::CustodyOperatorEvent,
    custody_submission::{
        Submission, SubmissionPayload, SubmissionRoute, SubmitRefusal, choose_submission_route,
        prepare_immediate_request, settle_immediate_request, submit_to_outbox,
    },
    flush_clock::{FlushJitter, shared_time_at, wake_for_mark},
    outbox_dispatch::{BatchDispatchError, FlushReport, OutboxDispatcher},
    outbox_flush::{FlushEffect, FlushEvent, FlushTransitionError, OutboxFlushState, transition},
    outbox_work::{OutboxWork, OutboxWorkError},
    receipt_delivery::{ReceiptDeliveryError, ReceiptFrameOutcome},
    recipient_inbox::{InboxCustodyError, InboxOfferResult, RecipientInbox},
};

/// A failure produced while custody uses a ready link or its durable stores.
#[derive(Debug, thiserror::Error)]
pub enum CustodyLinkError {
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
    lane: Arc<dyn ReadyLinkLane>,
    clock: Arc<dyn RuntimeClock>,
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
        let route = match choose_submission_route(payload) {
            Ok(route) => route,
            Err(refusal) => return Submission::Refused(refusal),
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
                submitted
            }
            SubmissionRoute::Immediate(payload) => {
                let Some(admitted) = admitted else {
                    return Submission::Refused(SubmitRefusal::CentralUnavailable);
                };
                let request =
                    match prepare_immediate_request(caller, payload, self.outbox.local_now_ms()) {
                        Ok(request) => request,
                        Err(refusal) => return Submission::Refused(refusal),
                    };
                let disposition = self
                    .lane
                    .submit_command(LinkLaneCommand::ImmediateRequest {
                        connection: admitted.connection,
                        request: Box::new(request.clone()),
                    })
                    .await;
                settle_immediate_request(&request, disposition)
            }
        }
    }

    /// Drive one link event through the custody-owned inbox.
    pub async fn observe_next_event(&mut self) -> Result<CustodyLinkObservation, CustodyLinkError> {
        let Some(event) = self.lane.next_event().await else {
            self.apply_flush_event(FlushEvent::LinkLost)?;
            return Ok(CustodyLinkObservation::DriverClosed);
        };
        match event {
            LinkLaneEvent::ConnectionAdmitted(_) | LinkLaneEvent::ConnectionNotAdmitted { .. } => {
                self.refresh_flush_wake().await?;
                Ok(CustodyLinkObservation::AdmissionChanged)
            }
            LinkLaneEvent::Deliver {
                connection,
                message,
                time_left,
                sender: _,
            } => {
                if !self
                    .admitted_connection()
                    .is_some_and(|admitted| admitted.connection == connection)
                {
                    return Ok(CustodyLinkObservation::StaleDelivery);
                }
                Ok(CustodyLinkObservation::Delivery(
                    self.inbox.receive_offer(*message, time_left).await?,
                ))
            }
        }
    }

    /// A wait for the shared minute selected by the scheduler. `None` means the lane is unavailable or idle.
    pub async fn refresh_flush_wake(&mut self) -> Result<Option<ClockInstant>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            self.apply_flush_event(FlushEvent::LinkLost)?;
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
        let Some(admitted) = self.admitted_connection() else {
            self.apply_flush_event(FlushEvent::LinkLost)?;
            return Ok(FlushReport::default());
        };
        self.last_clock_anchor = Some(admitted.clock_anchor);
        let now = shared_time_at(admitted.clock_anchor, self.clock.monotonic_now());
        let earliest_due = self.outbox.earliest_waiting_mark().await?;
        self.apply_flush_event(FlushEvent::LinkConnected { earliest_due })?;
        let mark = self.apply_flush_event(FlushEvent::Tick { now, earliest_due })?;
        let Some(mark) = mark else {
            return Ok(FlushReport::default());
        };

        let settled = self.send_mark(&admitted, mark).await;
        let earliest_due = self.outbox.earliest_waiting_mark().await;
        // Always release the scheduler slot, including when the store failed or the window closed.
        self.apply_flush_event(FlushEvent::BatchEnded {
            earliest_due: earliest_due.as_ref().ok().copied().flatten(),
        })?;
        let mut report = settled?;
        if let Err(error) = earliest_due {
            report.issues.push(BatchDispatchError::Work(error));
        }
        Ok(report)
    }

    async fn send_mark(
        &mut self,
        admitted: &AdmittedConnection,
        mark: SyncTime,
    ) -> Result<FlushReport, CustodyLinkError> {
        let mut prepared = self
            .dispatcher
            .prepare_at_mark(
                &mut self.outbox,
                admitted.connection.clone(),
                admitted.clock_anchor,
                mark,
            )
            .await?;
        let Some(batch) = prepared.batch else {
            return Ok(prepared.report);
        };
        let disposition = if self.admitted_connection().as_ref() != Some(admitted)
            || !self.outbox.mark_is_open(admitted.clock_anchor, mark)
        {
            WriteBatchDisposition::NotReached
        } else {
            self.lane.submit_write_batch(batch.command()).await
        };
        if let Some(settled) = self
            .dispatcher
            .settle_batch(&mut self.outbox, batch, disposition)
            .await
        {
            prepared.report.answered.extend(settled.answered);
            prepared.report.ended.extend(settled.ended);
            prepared.report.issues.extend(settled.issues);
        }
        Ok(prepared.report)
    }

    /// Submit a frame of durable inbox receipts for the current connection.
    pub async fn flush_receipts(
        &mut self,
    ) -> Result<Option<ReceiptFrameOutcome>, CustodyLinkError> {
        let Some(admitted) = self.admitted_connection() else {
            return Ok(None);
        };
        let Some(frame) = self.inbox.prepare_receipts(&admitted.connection).await? else {
            return Ok(None);
        };
        let disposition = self.lane.submit_command(frame.command()).await;
        let current = self.admitted_connection();
        Ok(Some(
            self.inbox
                .settle_receipts(
                    frame,
                    current.as_ref().map(|admitted| &admitted.connection),
                    disposition,
                )
                .await?,
        ))
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
