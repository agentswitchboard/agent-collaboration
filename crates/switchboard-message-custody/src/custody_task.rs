//! The custody task runs the local deadline, minute, and receipt lanes concurrently.

use central_link_client::{CorrelatedReceiptResults, LinkLaneDisposition, WriteBatchDisposition};
use central_link_protocol::{MAX_LINK_REQUEST_BATCH_SIZE, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use std::{
    future::{Future, pending},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use switchboard_machine_core_store::PendingWrite;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};
use workspace_ids::{MessageId, SessionRef};
use workspace_model::{
    Caller,
    central_mail::{MessageRecord, MessageStatus},
};

use crate::{
    custody_link::{CustodyLink, CustodyLinkError},
    custody_operator::CustodyOperatorEvent,
    custody_status::{
        CustodyRequestStatus, MessageStatusError, PreparedRequestStatus, message_query,
        message_status_reply, remote_status,
    },
    custody_submission::{
        PreparedSubmission, Submission, SubmissionPayload, SubmissionRoute, SubmitRefusal,
        choose_submission_route, settle_immediate_request, wire_scope,
    },
    offer_taker::{TakenEvent, run_offer_taker},
    outbox_dispatch::{FlushReport, PreparedWriteBatch},
    receipt_delivery::{ReceiptFrame, ReceiptFrameOutcome},
    recipient_inbox::InboxCustodyError,
};

/// The task's result and diagnostic stream for the Host. It never changes a caller's status reply.
#[derive(Debug)]
pub enum CustodyTaskEvent {
    Flush(FlushReport),
    Receipt(ReceiptFrameOutcome),
    Operator(CustodyOperatorEvent),
    Failed(CustodyLinkError),
}

/// The composition point creates a sink for the custody task's local outcomes and diagnostics.
pub trait CustodyEventSink: Send + Sync {
    fn record_event(&self, event: CustodyTaskEvent);
}

/// The custody task creates a handle so tools can submit while the link waits for a reply.
#[derive(Clone)]
pub struct CustodyTaskHandle {
    commands: mpsc::Sender<CustodyCommand>,
}

enum CustodyCommand {
    Submit {
        caller: Caller,
        payload: SubmissionPayload,
        reply: oneshot::Sender<Submission>,
    },
    Status {
        caller: Caller,
        query: RequestStatusQuery,
        reply: oneshot::Sender<CustodyRequestStatus>,
    },
    Pending {
        caller: Caller,
        reply: oneshot::Sender<Result<Vec<PendingWrite>, CustodyLinkError>>,
    },
    Waiting {
        session: SessionRef,
        reply: oneshot::Sender<Result<Vec<MessageRecord>, CustodyLinkError>>,
    },
}

impl CustodyTaskHandle {
    pub async fn submit(&self, caller: Caller, payload: SubmissionPayload) -> Submission {
        let (reply, result) = oneshot::channel();
        let (lost_reply, unavailable) = match choose_submission_route(payload.clone()) {
            Ok(SubmissionRoute::Outbox { request, payload }) => (
                Submission::Uncertain {
                    request,
                    scope: wire_scope(payload.request_scope()),
                },
                SubmitRefusal::OutboxUnavailable,
            ),
            Ok(SubmissionRoute::Immediate(wire)) => (
                wire.write_request_id()
                    .map_or(Submission::ReadAnswerLost, |request| {
                        Submission::Uncertain {
                            request: request.clone(),
                            scope: wire.request_scope(),
                        }
                    }),
                SubmitRefusal::CentralUnavailable,
            ),
            Err(refusal) => return Submission::Refused(refusal),
        };
        if self
            .commands
            .send(CustodyCommand::Submit {
                caller,
                payload,
                reply,
            })
            .await
            .is_err()
        {
            return Submission::Refused(unavailable);
        }
        result.await.unwrap_or(lost_reply)
    }

    pub async fn request_status(
        &self,
        caller: Caller,
        query: RequestStatusQuery,
    ) -> CustodyRequestStatus {
        let (reply, result) = oneshot::channel();
        if self
            .commands
            .send(CustodyCommand::Status {
                caller,
                query,
                reply,
            })
            .await
            .is_err()
        {
            return CustodyRequestStatus::StatusUnavailable;
        }
        result
            .await
            .unwrap_or(CustodyRequestStatus::StatusUnavailable)
    }

    pub async fn message_status(
        &self,
        caller: Caller,
        message_id: MessageId,
    ) -> Result<Option<MessageStatus>, MessageStatusError> {
        let submission = self.submit(caller, message_query(message_id)?).await;
        message_status_reply(submission)
    }

    pub async fn pending_writes(
        &self,
        caller: Caller,
    ) -> Result<Vec<PendingWrite>, CustodyLinkError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CustodyCommand::Pending { caller, reply })
            .await
            .map_err(|_| CustodyLinkError::TaskClosed)?;
        result.await.map_err(|_| CustodyLinkError::TaskClosed)?
    }

    pub async fn waiting_for_session(
        &self,
        session: SessionRef,
    ) -> Result<Vec<MessageRecord>, CustodyLinkError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(CustodyCommand::Waiting { session, reply })
            .await
            .map_err(|_| CustodyLinkError::TaskClosed)?;
        result.await.map_err(|_| CustodyLinkError::TaskClosed)?
    }
}

/// Return the tool handle and the future its Host owns and drains at shutdown.
pub fn custody_task(
    custody: CustodyLink,
    stop: watch::Receiver<bool>,
    sink: Arc<dyn CustodyEventSink>,
) -> (CustodyTaskHandle, impl Future<Output = ()> + Send) {
    let (commands, receiver) = mpsc::channel(MAX_LINK_REQUEST_BATCH_SIZE);
    let task = async move {
        let (saved, offers) = mpsc::unbounded_channel();
        let (stop_offers, offers_stopped) = watch::channel(false);
        let (retry_offers, retries) = mpsc::channel(1);
        let taking = tokio::spawn(run_offer_taker(
            custody.lane.clone(),
            custody.inbox.clone(),
            offers_stopped,
            retries,
            saved,
        ));
        run_custody(custody, receiver, offers, retry_offers, stop, sink).await;
        stop_offers.send_replace(true);
        let _ = taking.await;
    };
    (CustodyTaskHandle { commands }, task)
}

type InFlight<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type WriteReply = (PreparedWriteBatch, WriteBatchDisposition);
type ReceiptReply = (ReceiptFrame, LinkLaneDisposition<CorrelatedReceiptResults>);

async fn run_custody(
    mut custody: CustodyLink,
    mut commands: mpsc::Receiver<CustodyCommand>,
    mut offers: mpsc::UnboundedReceiver<Result<TakenEvent, InboxCustodyError>>,
    retry_offers: mpsc::Sender<()>,
    mut stop: watch::Receiver<bool>,
    sink: Arc<dyn CustodyEventSink>,
) {
    let clock = custody.clock.clone();
    let lane = custody.lane.clone();
    let mut offer_stream_open = true;
    let mut writes: Option<InFlight<WriteReply>> = None;
    let mut receipts: Option<InFlight<ReceiptReply>> = None;
    let mut inbox_mark = None;
    let mut tool_requests = JoinSet::new();
    let mut retry_store_at = None;
    report_flush(&*sink, custody.recover_outbox().await);

    loop {
        let now = clock.monotonic_now();
        let (write_wake, deadline_wake) = if retry_store_at.is_some_and(|retry| now < retry) {
            (retry_store_at, None)
        } else {
            retry_store_at = None;
            match (
                custody.refresh_flush_wake().await,
                custody.next_write_deadline().await,
            ) {
                (Ok(write), Ok(deadline)) => (write, deadline),
                (Err(error), _) | (_, Err(error)) => {
                    sink.record_event(CustodyTaskEvent::Failed(error));
                    retry_store_at = Some(custody.local_retry_mark());
                    (retry_store_at, None)
                }
            }
        };
        let inbox_wake = custody.next_inbox_minute(inbox_mark).await;
        for event in custody.take_operator_events().await {
            sink.record_event(CustodyTaskEvent::Operator(event));
        }

        tokio::select! {
            () = async { let _ = stop.wait_for(|stop| *stop).await; } => break,
            command = commands.recv() => match command {
                Some(command) => handle_tool(&mut custody, &mut tool_requests, command).await,
                None => break,
            },
            event = offers.recv(), if offer_stream_open => {
                match event {
                    Some(Ok(saved)) => {
                        if let Err(error) = custody.observe_taken_event(saved).await {
                            sink.record_event(CustodyTaskEvent::Failed(error));
                        }
                    }
                    Some(Err(error)) => sink.record_event(CustodyTaskEvent::Failed(error.into())),
                    None => { offer_stream_open = false; }
                }
            },
            (ticket, disposition) = poll_job(&mut writes) => {
                writes = None;
                report_flush(&*sink, custody.settle_write_turn(ticket, disposition).await);
            },
            (frame, disposition) = poll_job(&mut receipts) => {
                receipts = None;
                match custody.settle_receipt_turn(frame, disposition).await {
                    Ok(outcome) => sink.record_event(CustodyTaskEvent::Receipt(outcome)),
                    Err(error) => sink.record_event(CustodyTaskEvent::Failed(error)),
                }
            },
            () = sleep_until(clock.as_ref(), deadline_wake) => {
                let ended = custody.end_due_writes().await;
                if ended.is_err() { retry_store_at = Some(custody.local_retry_mark()); }
                report_flush(&*sink, ended);
            },
            () = sleep_until(clock.as_ref(), write_wake), if writes.is_none() => {
                match custody.prepare_write_turn().await {
                    Ok((report, ticket)) => {
                        report_flush(&*sink, Ok(report));
                        if let Some(ticket) = ticket {
                            let lane = lane.clone();
                            writes = Some(Box::pin(async move {
                                let disposition = lane.submit_write_batch(ticket.command()).await;
                                (ticket, disposition)
                            }));
                        }
                    }
                    Err(error) => {
                        retry_store_at = Some(custody.local_retry_mark());
                        sink.record_event(CustodyTaskEvent::Failed(error));
                    }
                }
            },
            () = sleep_until(clock.as_ref(), inbox_wake.map(|(_, wake)| wake)) => {
                inbox_mark = inbox_wake.map(|(mark, _)| mark);
                let _ = retry_offers.try_send(());
                if receipts.is_none() {
                    match custody.prepare_receipt_turn().await {
                        Ok(Some(frame)) => {
                            let lane = lane.clone();
                            let send_before = clock.monotonic_now().after(Duration::from_secs(30));
                            receipts = Some(Box::pin(async move {
                                let disposition = lane.submit_receipts(frame.command(send_before)).await;
                                (frame, disposition)
                            }));
                        }
                        Ok(None) => {}
                        Err(error) => sink.record_event(CustodyTaskEvent::Failed(error)),
                    }
                }
            },
            _ = tool_requests.join_next(), if !tool_requests.is_empty() => {}
        }
    }
    tool_requests.shutdown().await;
}

async fn handle_tool(
    custody: &mut CustodyLink,
    pending_requests: &mut JoinSet<()>,
    command: CustodyCommand,
) {
    match command {
        CustodyCommand::Submit {
            caller,
            payload,
            reply,
        } => {
            if reply.is_closed() {
                return;
            }
            match custody.prepare_submission(caller, payload).await {
                PreparedSubmission::Complete(result) => {
                    let _ = reply.send(result);
                }
                PreparedSubmission::Immediate(command) => {
                    let lane = custody.lane.clone();
                    pending_requests.spawn(async move {
                        if reply.is_closed() {
                            return;
                        }
                        let request = command.request.clone();
                        let disposition = lane.submit_immediate_request(command).await;
                        let _ = reply.send(settle_immediate_request(&request, disposition));
                    });
                }
            }
        }
        CustodyCommand::Status {
            caller,
            query,
            reply,
        } => {
            if reply.is_closed() {
                return;
            }
            match custody.prepare_request_status(caller, query).await {
                PreparedRequestStatus::Complete(result) => {
                    let _ = reply.send(result);
                }
                PreparedRequestStatus::Remote(command) => {
                    let lane = custody.lane.clone();
                    pending_requests.spawn(async move {
                        if reply.is_closed() {
                            return;
                        }
                        let correlation = command.correlation_id.clone();
                        let disposition = lane.request_status(command).await;
                        let _ = reply.send(remote_status(&correlation, disposition));
                    });
                }
            }
        }
        CustodyCommand::Pending { caller, reply } => {
            let _ = reply.send(custody.pending_writes(&caller).await);
        }
        CustodyCommand::Waiting { session, reply } => {
            let _ = reply.send(custody.waiting_for_session(&session).await);
        }
    }
}

fn report_flush(sink: &dyn CustodyEventSink, result: Result<FlushReport, CustodyLinkError>) {
    match result {
        Ok(report)
            if !report.answered.is_empty()
                || !report.ended.is_empty()
                || !report.issues.is_empty() =>
        {
            sink.record_event(CustodyTaskEvent::Flush(report));
        }
        Ok(_) => {}
        Err(error) => sink.record_event(CustodyTaskEvent::Failed(error)),
    }
}

async fn poll_job<T>(job: &mut Option<InFlight<T>>) -> T {
    match job {
        Some(job) => job.await,
        None => pending().await,
    }
}

async fn sleep_until(clock: &dyn RuntimeClock, wake: Option<ClockInstant>) {
    match wake {
        Some(wake) => clock.sleep_until(wake).await,
        None => pending().await,
    }
}
