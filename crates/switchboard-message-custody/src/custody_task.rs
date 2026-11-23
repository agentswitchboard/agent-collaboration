//! The custody task runs the local deadline, minute, and receipt lanes concurrently.

use central_link_client::{
    CorrelatedReceiptResults, CorrelatedStatusReply, LinkLaneDisposition, WriteBatchDisposition,
};
use central_link_protocol::{MAX_LINK_REQUEST_BATCH_SIZE, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use std::{
    collections::BTreeSet,
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
use workspace_ids::{MessageId, RequestId, SessionRef};
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
    orphan_recovery::OrphanProbe,
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
type OrphanReply = (OrphanProbe, LinkLaneDisposition<CorrelatedStatusReply>);

enum ReadyWriteTurn {
    Settled(Box<WriteReply>),
    Deadline,
}

async fn ready_write_turn(
    clock: &dyn RuntimeClock,
    deadline: Option<ClockInstant>,
    reply: &mut Option<InFlight<WriteReply>>,
) -> ReadyWriteTurn {
    // An already available disposition decides the member before a competing cutoff.
    tokio::select! {
        biased;
        outcome = poll_job(reply) => ReadyWriteTurn::Settled(Box::new(outcome)),
        () = sleep_until(clock, deadline) => ReadyWriteTurn::Deadline,
    }
}

struct OutstandingOrphan {
    request: RequestId,
    cutoff_reported: bool,
    completion: InFlight<OrphanReply>,
}

enum ReadyOrphanTurn {
    Answer(Box<OrphanReply>),
    Scan,
}

async fn ready_orphan_turn(
    clock: &dyn RuntimeClock,
    scan_at: Option<ClockInstant>,
    reply: &mut Option<OutstandingOrphan>,
) -> ReadyOrphanTurn {
    // The lane may already have accepted and published a reply while the
    // worker was held. Inspect it before an equally ready cutoff scan.
    tokio::select! {
        biased;
        result = async {
            match reply {
                Some(probe) => probe.completion.as_mut().await,
                None => pending().await,
            }
        } => ReadyOrphanTurn::Answer(Box::new(result)),
        () = sleep_until(clock, scan_at) => ReadyOrphanTurn::Scan,
    }
}

const LOCAL_RECOVERY_RETRY: Duration = Duration::from_secs(1);

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
    let mut receipt_mark = None;
    let mut intake_retry_at = None;
    let mut tool_requests = JoinSet::new();
    let mut retry_store_at = None;
    let mut retry_cutoff_at = None;
    let mut admission = lane.watch_admitted();
    let mut admission_open = true;
    let mut orphan_reply: Option<OutstandingOrphan> = None;
    let mut orphan_scan_at = Some(clock.monotonic_now());
    let mut retry_orphans_at = None;
    let mut deferred_orphans = BTreeSet::new();
    let mut retry_settlement_at = None;
    let mut retry_notices_at = project_sender_notices(&mut custody, &*sink).await;
    let mut retention_at = next_retention(&mut custody, &*sink).await;

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
        let deadline_wake = if retry_cutoff_at.is_some_and(|retry| now < retry) {
            retry_cutoff_at
        } else {
            retry_cutoff_at = None;
            // A store read failure cannot hide the deadline of the frame the task already holds.
            deadline_wake
                .into_iter()
                .chain(custody.active_write_deadline())
                .min()
        };
        let receipt_wake = custody.next_receipt_minute(receipt_mark.as_ref());
        if intake_retry_at.is_none() {
            intake_retry_at = custody.next_intake_retry().await;
        }
        let can_flush = writes.is_none() && custody.has_startup_claim_inventory();
        for event in custody.take_operator_events().await {
            sink.record_event(CustodyTaskEvent::Operator(event));
        }

        tokio::select! {
            () = async { let _ = stop.wait_for(|stop| *stop).await; } => break,
            changed = admission.changed(), if admission_open => {
                admission_open = changed.is_ok();
                deferred_orphans.clear();
                retry_orphans_at = None;
                orphan_scan_at = Some(clock.monotonic_now());
            },
            command = commands.recv() => match command {
                Some(command) => handle_tool(&mut custody, &mut tool_requests, command, &*sink).await,
                None => break,
            },
            event = offers.recv(), if offer_stream_open => {
                match event {
                    Some(Ok(saved)) => {
                        let sender_notice_saved = saved.sender_notice_saved;
                        if let Err(error) = custody.observe_taken_event(saved).await {
                            sink.record_event(CustodyTaskEvent::Failed(error));
                        }
                        if sender_notice_saved {
                            retry_notices_at = project_sender_notices(&mut custody, &*sink).await;
                            retention_at = next_retention(&mut custody, &*sink).await;
                        }
                    }
                    Some(Err(error)) => sink.record_event(CustodyTaskEvent::Failed(error.into())),
                    None => { offer_stream_open = false; }
                }
            },
            write_turn = ready_write_turn(clock.as_ref(), deadline_wake, &mut writes) => match write_turn {
                ReadyWriteTurn::Settled(completed) => {
                    let (ticket, disposition) = *completed;
                    writes = None;
                    let mut report = FlushReport::default();
                    let result = custody.settle_write_turn(ticket, disposition, &mut report).await;
                    report_flush(&*sink, Ok(report));
                    if let Err(error) = result {
                        sink.record_event(CustodyTaskEvent::Failed(error));
                    }
                    if custody.owes_local_settlement() && retry_settlement_at.is_none() {
                        retry_settlement_at = Some(clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
                    }
                }
                ReadyWriteTurn::Deadline => {
                    let ended = custody.end_due_writes().await;
                    if ended.is_err() { retry_cutoff_at = Some(custody.local_retry_mark()); }
                    report_flush(&*sink, ended);
                }
            },
            (frame, disposition) = poll_job(&mut receipts) => {
                receipts = None;
                match custody.settle_receipt_turn(frame, disposition).await {
                    Ok(outcome) => sink.record_event(CustodyTaskEvent::Receipt(outcome)),
                    Err(error) => sink.record_event(CustodyTaskEvent::Failed(error)),
                }
                retention_at = next_retention(&mut custody, &*sink).await;
            },
            () = sleep_until(clock.as_ref(), retry_settlement_at) => {
                report_flush(&*sink, Ok(custody.retry_local_settlement().await));
                retry_settlement_at = custody.owes_local_settlement()
                    .then(|| clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
            },
            () = sleep_until(clock.as_ref(), retry_orphans_at) => {
                deferred_orphans.clear();
                retry_orphans_at = None;
                orphan_scan_at = Some(clock.monotonic_now());
            },
            orphan_turn = ready_orphan_turn(clock.as_ref(), orphan_scan_at, &mut orphan_reply) => match orphan_turn {
                ReadyOrphanTurn::Scan => {
                    match custody.prepare_recovery_turn(orphan_reply.is_none(), &deferred_orphans).await {
                        Ok(turn) => {
                            if let Some(probe) = &mut orphan_reply {
                                probe.cutoff_reported |= turn.report.ended.iter().any(|ended| ended.request_id == probe.request);
                            }
                            report_flush(&*sink, Ok(turn.report));
                            orphan_scan_at = turn.next_deadline;
                            if custody.owes_local_settlement() && retry_settlement_at.is_none() {
                                retry_settlement_at = Some(clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
                            }
                            if let Some(probe) = turn.probe {
                                let lane = lane.clone();
                                orphan_reply = Some(OutstandingOrphan {
                                    request: probe.claim.key.request_id.clone(),
                                    cutoff_reported: false,
                                    completion: Box::pin(async move {
                                        let result = lane.request_status(probe.command.clone()).await;
                                        (probe, result)
                                    }),
                                });
                            }
                        }
                        Err(error) => {
                            sink.record_event(CustodyTaskEvent::Failed(error.into()));
                            orphan_scan_at = Some(clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
                        }
                    }
                }
                ReadyOrphanTurn::Answer(completed) => {
                    let (probe, outcome) = *completed;
                    let cutoff_reported = orphan_reply.take().is_some_and(|probe| probe.cutoff_reported);
                    deferred_orphans.insert(probe.claim.key.request_id.clone());
                    report_flush(&*sink, Ok(custody.settle_recovery_probe(probe, outcome, cutoff_reported).await));
                    if retry_orphans_at.is_none() {
                        retry_orphans_at = Some(custody.local_retry_mark());
                    }
                    if custody.owes_local_settlement() && retry_settlement_at.is_none() {
                        retry_settlement_at = Some(clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
                    }
                    orphan_scan_at = Some(clock.monotonic_now());
                }
            },
            () = sleep_until(clock.as_ref(), retry_notices_at) => {
                retry_notices_at = project_sender_notices(&mut custody, &*sink).await;
                retention_at = next_retention(&mut custody, &*sink).await;
            },
            () = sleep_until(clock.as_ref(), retention_at) => {
                retention_at = match custody.prune_inbox().await {
                    Ok(()) => next_retention(&mut custody, &*sink).await,
                    Err(error) => {
                        sink.record_event(CustodyTaskEvent::Failed(error.into()));
                        Some(custody.local_retry_mark())
                    }
                };
            },
            () = sleep_until(clock.as_ref(), write_wake), if can_flush => {
                let mut report = FlushReport::default();
                let next_batch = custody.prepare_write_turn(&mut report).await;
                report_flush(&*sink, Ok(report));
                if custody.owes_local_settlement() && retry_settlement_at.is_none() {
                    retry_settlement_at = Some(clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
                }
                match next_batch {
                    Ok(ticket) => {
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
                        let recover = clock.monotonic_now().after(LOCAL_RECOVERY_RETRY);
                        orphan_scan_at = Some(orphan_scan_at.map_or(recover, |prior| prior.min(recover)));
                        sink.record_event(CustodyTaskEvent::Failed(error));
                    }
                }
            },
            () = sleep_until(clock.as_ref(), intake_retry_at) => {
                intake_retry_at = None;
                let _ = retry_offers.try_send(());
            },
            () = sleep_until(clock.as_ref(), receipt_wake.as_ref().map(|(_, wake)| *wake)) => {
                receipt_mark = receipt_wake.map(|(mark, _)| mark);
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
    sink: &dyn CustodyEventSink,
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
            let prepared = custody.prepare_request_status(caller, query).await;
            for diagnostic in custody.take_outbox_operator_events() {
                sink.record_event(CustodyTaskEvent::Operator(diagnostic));
            }
            match prepared {
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
            let result = custody.pending_writes(&caller).await;
            for diagnostic in custody.take_outbox_operator_events() {
                sink.record_event(CustodyTaskEvent::Operator(diagnostic));
            }
            let _ = reply.send(result);
        }
        CustodyCommand::Waiting { session, reply } => {
            let _ = reply.send(custody.waiting_for_session(&session).await);
        }
    }
}

fn report_flush(sink: &dyn CustodyEventSink, result: Result<FlushReport, CustodyLinkError>) {
    match result {
        Err(CustodyLinkError::InterruptedFlush { completed, error }) => {
            sink.record_event(CustodyTaskEvent::Flush(*completed));
            sink.record_event(CustodyTaskEvent::Failed(*error));
        }
        Ok(report) if !report.is_empty() => {
            sink.record_event(CustodyTaskEvent::Flush(report));
        }
        Ok(_) => {}
        Err(error) => sink.record_event(CustodyTaskEvent::Failed(error)),
    }
}

async fn project_sender_notices(
    custody: &mut CustodyLink,
    sink: &dyn CustodyEventSink,
) -> Option<ClockInstant> {
    let projected = custody.project_sender_notices().await;
    report_flush(sink, Ok(projected.report));
    let finishing = custody.complete_sender_notices(projected.completable).await;
    let retry =
        (!projected.errors.is_empty() || !finishing.is_empty()).then(|| custody.local_retry_mark());
    for error in projected.errors.into_iter().chain(finishing) {
        sink.record_event(CustodyTaskEvent::Failed(error));
    }
    retry
}

async fn next_retention(
    custody: &mut CustodyLink,
    sink: &dyn CustodyEventSink,
) -> Option<ClockInstant> {
    match custody.next_inbox_retention().await {
        Ok(wake) => wake,
        Err(error) => {
            sink.record_event(CustodyTaskEvent::Failed(error.into()));
            Some(custody.local_retry_mark())
        }
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
