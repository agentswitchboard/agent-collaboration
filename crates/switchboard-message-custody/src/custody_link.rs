//! Custody's user of the admitted message lane. The driver owns the socket.

use central_link_client::{
    AdmittedConnection, ConnectionAttempt, CorrelatedReceiptResults, ImmediateRequestCommand,
    LinkLaneDisposition, ReadyLinkLane, RegisteredConnection, RequestStatusCommand,
};
use central_link_protocol::{FreshCorrelationId, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::{future::pending, sync::Arc, time::Duration};
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
    },
    flush_clock::{FlushJitter, shared_time_at, wake_for_mark},
    offer_taker::{SharedRecipientInbox, TakenEvent, admitted_for, take_event},
    receipt_delivery::{ReceiptDeliveryError, ReceiptFrame, ReceiptFrameOutcome},
    recipient_inbox::{InboxCustodyError, RecipientInbox},
};

use self::outbox_driver::{
    AppliedSenderNotice, BatchDispatchError, FlushReport, FlushTransitionError, OutboxDriver,
    OutboxWork, OutboxWorkError,
};

#[path = "custody_outbox.rs"]
pub(crate) mod outbox_driver;

const IMMEDIATE_COMMAND_WINDOW: Duration = Duration::from_secs(30);

/// A failure produced while custody uses a ready link or its durable stores.
#[derive(Debug, thiserror::Error)]
pub enum CustodyLinkError {
    #[error("the inbox handover could not complete its turn")]
    Handover(#[from] crate::handover::HandoverError),
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

/// Custody projects durable notices and keeps their inbox payloads until the sink accepts the reports.
#[derive(Default)]
pub(crate) struct SenderNoticeProjection {
    pub(crate) report: FlushReport,
    pub(crate) completable: Vec<MessageId>,
    pub(crate) errors: Vec<CustodyLinkError>,
}

pub use crate::offer_taker::CustodyLinkObservation;

/// Custody owns the stores, one minute scheduler and one consumer of the driver's admitted message lane.
/// The [custody task](crate::custody_task::custody_task) drives outbox writes and recovery and sends durable receipts on the scheduled turn.
///
/// ```compile_fail,E0599
/// use switchboard_message_custody::custody_link::CustodyLink;
///
/// async fn drain_without_the_worker(custody: &mut CustodyLink) {
///     let _ = custody.flush_writes().await;
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_message_custody::custody_link::CustodyLink;
///
/// async fn schedule_without_the_worker(custody: &mut CustodyLink) {
///     let _ = custody.flush_next().await;
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_message_custody::custody_link::CustodyLink;
///
/// async fn recover_without_the_worker(custody: &mut CustodyLink) {
///     let _ = custody.recover_outbox().await;
/// }
/// ```
///
/// ```compile_fail,E0599
/// use switchboard_message_custody::custody_link::CustodyLink;
///
/// async fn send_without_a_scheduled_turn(custody: &mut CustodyLink) {
///     let _ = custody.flush_receipts().await;
/// }
/// ```
pub struct CustodyLink {
    pub(crate) lane: Arc<dyn ReadyLinkLane>,
    pub(crate) clock: Arc<dyn RuntimeClock>,
    jitter: Arc<dyn FlushJitter>,
    outbox_driver: OutboxDriver,
    pub(crate) inbox: SharedRecipientInbox,
    pub(crate) notice_port: Option<Arc<dyn crate::handover_notice::ManagedNoticePort>>,
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
            outbox_driver: OutboxDriver::new(outbox),
            inbox: Arc::new(tokio::sync::Mutex::new(inbox)),
            notice_port: None,
        }
    }

    /// Supply the managed-session writer for the custody unit's local notice lane.
    #[must_use]
    pub fn with_notice_port(
        mut self,
        port: Arc<dyn crate::handover_notice::ManagedNoticePort>,
    ) -> Self {
        self.notice_port = Some(port);
        self
    }

    fn admitted_connection(&self) -> Option<AdmittedConnection> {
        admitted_for(self.lane.as_ref())
    }

    fn registered_connection(&self) -> Option<RegisteredConnection> {
        self.lane.watch_registered().borrow().clone()
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
        match route {
            SubmissionRoute::Outbox { request, payload } => {
                self.queue_outbox_write(self.admitted_connection(), caller, request, payload)
                    .await
            }
            SubmissionRoute::Immediate(payload) => {
                let Some(registered) = self.registered_connection() else {
                    return PreparedSubmission::Complete(Submission::Refused(
                        SubmitRefusal::CentralUnavailable,
                    ));
                };
                let request = match prepare_immediate_request(
                    caller,
                    payload,
                    self.outbox_driver.work.local_now_ms(),
                ) {
                    Ok(request) => request,
                    Err(refusal) => {
                        return PreparedSubmission::Complete(Submission::Refused(refusal));
                    }
                };
                PreparedSubmission::Immediate(ImmediateRequestCommand {
                    connection: registered.admitted().connection.clone(),
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
            .outbox_driver
            .work
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
                self.outbox_driver.work.report_point_read_error(&error);
                return PreparedRequestStatus::Complete(CustodyRequestStatus::StatusUnavailable);
            }
        }
        let Some(registered) = self.registered_connection() else {
            return PreparedRequestStatus::Complete(CustodyRequestStatus::StatusUnavailable);
        };
        let correlation_id = FreshCorrelationId::mint_at(self.outbox_driver.work.local_now_ms())
            .wire_id()
            .clone();
        PreparedRequestStatus::Remote(RequestStatusCommand {
            connection: registered.admitted().connection.clone(),
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
        let result = self
            .outbox_driver
            .work
            .outbox_storage()
            .pending_for(caller)
            .await;
        self.report_outbox_read(result)
    }

    /// Read the sender's local history, scoped to the caller.
    pub async fn outgoing_writes(
        &mut self,
        caller: &Caller,
        limit: u16,
    ) -> Result<Vec<OutgoingWrite>, CustodyLinkError> {
        let result = self
            .outbox_driver
            .work
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
            .outbox_driver
            .work
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
            self.outbox_driver.work.report_point_read_error(&error);
            OutboxWorkError::Store(error).into()
        })
    }

    pub(crate) fn take_outbox_operator_events(&mut self) -> Vec<CustodyOperatorEvent> {
        self.outbox_driver.work.take_operator_events()
    }

    /// Drive one delivery using the same save-only path as the offer task.
    pub async fn observe_next_event(&mut self) -> Result<CustodyLinkObservation, CustodyLinkError> {
        let event = self.lane.next_event().await;
        let saved = take_event(&self.inbox, event, pending()).await?;
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
                self.outbox_driver
                    .work
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
        let now_ms = self.outbox_driver.work.local_now_ms();
        let store = self.outbox_driver.work.outbox_storage();
        match store.record_sender_delivery(caller, notice, now_ms).await {
            Ok(applied) => Ok(applied),
            Err(error) => match store.sender_outcome(caller, &notice.request_id).await {
                Ok(Some(stored)) if &stored == notice => Ok(true),
                _ => Err(error),
            },
        }
    }

    pub(crate) async fn prepare_receipt_turn(
        &mut self,
        connection: &ConnectionAttempt,
    ) -> Result<Option<ReceiptFrame>, CustodyLinkError> {
        if !self
            .registered_connection()
            .is_some_and(|registered| &registered.admitted().connection == connection)
        {
            return Ok(None);
        }
        Ok(self.inbox.lock().await.prepare_receipts(connection).await?)
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
        self.inbox.lock().await.prune_past_retention().await
    }

    pub(crate) fn next_receipt_minute(
        &self,
        last_mark: Option<&(central_link_client::ConnectionAttempt, SyncTime)>,
    ) -> Option<(
        (central_link_client::ConnectionAttempt, SyncTime),
        ClockInstant,
    )> {
        let now = self.clock.monotonic_now();
        let registered = self.registered_connection()?;
        let admitted = registered.admitted();
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
        Some(((admitted.connection.clone(), mark), wake))
    }

    pub(crate) async fn next_intake_retry(&mut self) -> Option<ClockInstant> {
        self.inbox
            .lock()
            .await
            .needs_local_retry()
            .then(|| self.local_retry_mark())
    }

    pub(crate) fn local_retry_mark(&self) -> ClockInstant {
        let now = self.outbox_driver.work.local_now_ms();
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
            if let Err(error) =
                RecipientInbox::keep_shared_offer(&self.inbox, offer, pending()).await
            {
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
}
