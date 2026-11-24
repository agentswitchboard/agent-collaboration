//! The custody task runs the local deadline, minute, and receipt lanes concurrently.

use central_link_client::{
    CorrelatedReceiptResults, CorrelatedStatusReply, LinkLaneDisposition, WriteBatchDisposition,
};
use central_link_protocol::MAX_LINK_REQUEST_BATCH_SIZE;
use router_clock::{ClockInstant, RuntimeClock};
use std::{
    collections::BTreeSet,
    future::{Future, pending},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc, watch},
    task::JoinSet,
};
use workspace_ids::RequestId;

pub use crate::custody_admission::{
    CustodyAdmission, CustodyQueueClosed, CustodyTaskHandle, PendingCustodyReply,
};
pub use crate::custody_run_group::{CustodyRunFailure, CustodyRunGroup};

use crate::{
    custody_admission::{CustodyCommand, CustodyCommandReceiver, command_queue},
    custody_link::{CustodyLink, CustodyLinkError},
    custody_operator::CustodyOperatorEvent,
    custody_run_group::{CustodyRunState, CustodyStop, StopOnExit, retain_group, wait_for_stop},
    custody_status::{PreparedRequestStatus, remote_status},
    custody_submission::{PreparedSubmission, settle_immediate_request},
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

/// Start custody and return the handle and its retained join.
pub fn custody_task(
    custody: CustodyLink,
    sink: Arc<dyn CustodyEventSink>,
) -> (CustodyTaskHandle, CustodyRunGroup) {
    let stop = CustodyStop::new();
    let (calls, commands) = command_queue(
        MAX_LINK_REQUEST_BATCH_SIZE,
        StopOnExit::new(&stop, CustodyRunFailure::WorkerPanicked),
    );
    let (saved, offers) = mpsc::unbounded_channel();
    let (retry_offers, retries) = mpsc::channel(1);
    let offers_stopped = stop.0.subscribe();
    let worker_stopped = stop.0.subscribe();
    let offer_exit = StopOnExit::new(&stop, CustodyRunFailure::OfferTakerPanicked);
    let lane = custody.lane.clone();
    let inbox = custody.inbox.clone();
    let offer_taker = tokio::spawn(async move {
        let mut exit = offer_exit;
        run_offer_taker(lane, inbox, offers_stopped, retries, saved).await;
        exit.complete();
    });
    let remote_tools = Arc::new(Mutex::new(JoinSet::new()));
    let worker_tools = remote_tools.clone();
    let worker = tokio::spawn(async move {
        let mut tools = worker_tools.lock().await;
        run_custody(
            custody,
            commands,
            offers,
            retry_offers,
            worker_stopped,
            sink,
            &mut tools,
        )
        .await;
    });
    (calls, retain_group(stop, worker, offer_taker, remote_tools))
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
    mut commands: CustodyCommandReceiver,
    mut offers: mpsc::UnboundedReceiver<Result<TakenEvent, InboxCustodyError>>,
    retry_offers: mpsc::Sender<()>,
    mut stop: watch::Receiver<CustodyRunState>,
    sink: Arc<dyn CustodyEventSink>,
    tool_requests: &mut JoinSet<()>,
) {
    let clock = custody.clock.clone();
    let lane = custody.lane.clone();
    let mut offer_stream_open = true;
    let mut writes: Option<InFlight<WriteReply>> = None;
    let mut receipts: Option<InFlight<ReceiptReply>> = None;
    let mut receipt_mark = None;
    let mut intake_retry_at = None;
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
        if stop.borrow().stopped {
            break;
        }
        let write_wake = match custody.refresh_flush_wake().await {
            Ok(write) => write,
            Err(error) => {
                sink.record_event(CustodyTaskEvent::Failed(error));
                custody.outbox_retry_wake()
            }
        };
        let (deadline_wake, cutoff_fault) = custody.project_write_cutoff_wake().await;
        if let Some(error) = cutoff_fault {
            sink.record_event(CustodyTaskEvent::Failed(error.into()));
        }
        let receipt_wake = custody.next_receipt_minute(receipt_mark.as_ref());
        if intake_retry_at.is_none() {
            intake_retry_at = custody.next_intake_retry().await;
        }
        let can_flush = writes.is_none() && custody.has_startup_claim_inventory();
        for event in custody.take_operator_events().await {
            sink.record_event(CustodyTaskEvent::Operator(event));
        }

        tokio::select! {
            () = wait_for_stop(&mut stop) => break,
            changed = admission.changed(), if admission_open => {
                admission_open = changed.is_ok();
                deferred_orphans.clear();
                retry_orphans_at = None;
                orphan_scan_at = Some(clock.monotonic_now());
            },
            command = commands.recv() => {
                let Some(command) = command else { break };
                if stop.borrow().stopped {
                    command.refuse_unstarted();
                    break;
                }
                handle_tool(&mut custody, tool_requests, command, &*sink, &stop).await;
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
                    report_flush(&mut custody, &*sink, Ok(report));
                    if let Err(error) = result {
                        sink.record_event(CustodyTaskEvent::Failed(error));
                    }
                    if custody.owes_local_settlement() && retry_settlement_at.is_none() {
                        retry_settlement_at = Some(clock.monotonic_now().after(LOCAL_RECOVERY_RETRY));
                    }
                }
                ReadyWriteTurn::Deadline => {
                    let ended = custody.end_due_writes().await;
                    report_flush(&mut custody, &*sink, ended);
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
                let report = custody.retry_local_settlement().await;
                report_flush(&mut custody, &*sink, Ok(report));
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
                            report_flush(&mut custody, &*sink, Ok(turn.report));
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
                    let report = custody.settle_recovery_probe(probe, outcome, cutoff_reported).await;
                    report_flush(&mut custody, &*sink, Ok(report));
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
                report_flush(&mut custody, &*sink, Ok(report));
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
            () = sleep_until(clock.as_ref(), receipt_wake.as_ref().map(|(_, wake)| *wake)), if receipts.is_none() => {
                let Some(((connection, mark), _)) = receipt_wake else { continue };
                let send_before = admission.borrow().as_ref()
                    .filter(|admitted| admitted.connection == connection)
                    .map(|admitted| crate::flush_clock::send_fence_for_mark(admitted.clock_anchor, mark));
                receipt_mark = Some((connection.clone(), mark));
                let Some(send_before) = send_before else { continue };
                match custody.prepare_receipt_turn().await {
                    Ok(Some(frame)) => {
                        let command = frame.command(send_before);
                        let untouched = if command.connection != connection {
                            Some(central_link_client::UntouchedCommand::ConnectionUnavailable)
                        } else if clock.monotonic_now() >= send_before {
                            Some(central_link_client::UntouchedCommand::SendWindowElapsed)
                        } else {
                            None
                        };
                        let lane = lane.clone();
                        receipts = Some(Box::pin(async move {
                            let disposition = match untouched {
                                Some(cause) => LinkLaneDisposition::NotReached { cause },
                                None => lane.submit_receipts(command).await,
                            };
                            (frame, disposition)
                        }));
                    }
                    Ok(None) => {}
                    Err(error) => sink.record_event(CustodyTaskEvent::Failed(error)),
                }
            },
            _ = tool_requests.join_next(), if !tool_requests.is_empty() => {}
        }
    }
    commands.finish();
}

async fn handle_tool(
    custody: &mut CustodyLink,
    pending_requests: &mut JoinSet<()>,
    command: CustodyCommand,
    sink: &dyn CustodyEventSink,
    stop: &watch::Receiver<CustodyRunState>,
) {
    match command {
        CustodyCommand::Submit {
            caller,
            payload,
            reply,
            unavailable: _,
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
                    let mut stop = stop.clone();
                    pending_requests.spawn(async move {
                        if reply.is_closed() {
                            return;
                        }
                        let request = command.request.clone();
                        let disposition = tokio::select! {
                            biased;
                            () = wait_for_stop(&mut stop) => return,
                            disposition = async { lane.submit_immediate_request(command).await } => disposition,
                        };
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
                    let mut stop = stop.clone();
                    pending_requests.spawn(async move {
                        if reply.is_closed() {
                            return;
                        }
                        let correlation = command.correlation_id.clone();
                        let disposition = tokio::select! {
                            biased;
                            () = wait_for_stop(&mut stop) => return,
                            disposition = async { lane.request_status(command).await } => disposition,
                        };
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

fn report_flush(
    custody: &mut CustodyLink,
    sink: &dyn CustodyEventSink,
    result: Result<FlushReport, CustodyLinkError>,
) {
    match result {
        Err(CustodyLinkError::InterruptedFlush { completed, error }) => {
            custody.retire_receipt_protections(&completed);
            sink.record_event(CustodyTaskEvent::Flush(*completed));
            sink.record_event(CustodyTaskEvent::Failed(*error));
        }
        Ok(report) if !report.is_empty() => {
            custody.retire_receipt_protections(&report);
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
    report_flush(custody, sink, Ok(projected.report));
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
