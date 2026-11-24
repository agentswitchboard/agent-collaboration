//! Custody admission and retained shutdown through the tool and run-group APIs.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "Unmet barriers, unexpected fixture data and missing replies fail the proof."
)]

use central_link_client::LinkLaneEvent;
use central_link_protocol::RequestScope;
use router_clock::{ManualClock, RuntimeClock};
use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::Arc,
    task::Poll,
};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, NewOutboxItem, OutboxQueuedWrite, OutboxRequestStatus,
    OutboxStorage,
};
use switchboard_message_custody::{
    custody_link::{CustodyLink, CustodyLinkError},
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    custody_task::{
        CustodyQueueClosed, CustodyRunFailure, CustodyRunGroup, CustodyTaskHandle, custody_task,
    },
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
    recipient_sessions::RECIPIENT_LOOKUP_BUDGET,
};
use tokio::sync::mpsc;
use workspace_model::{
    Caller, ResolvedSender,
    central_mail::{CentralMailOperation, CentralMailRequest, NoticeCadence},
};

use super::{
    batch_replay::{NoFlushJitter, ScriptedBatchLane, new_admission},
    concurrent_inbox::HeldDirectory,
    custody_task::ObserveTask,
    faulty_outbox::{FaultyOutbox, InsertRelease, StoreFaultPlan},
    inbox_custody::{direct_offer, recipient_session},
    outbox_timing::{manual_clock, open_outbox, thread_post},
    worker_orphans::scoped_query,
    worker_turns::WorkerClock,
};

struct RunningCustody {
    root: tempfile::TempDir,
    clock: Arc<ManualClock>,
    scheduler: Arc<WorkerClock>,
    lane: Arc<ScriptedBatchLane>,
    directory: Arc<HeldDirectory>,
    faults: StoreFaultPlan,
    calls: CustodyTaskHandle,
    group: CustodyRunGroup,
}

impl RunningCustody {
    async fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("machine_local.sqlite");
        let clock = manual_clock(1_763_380_780_000);
        let scheduler = WorkerClock::observing(clock.clone());
        let machine = recipient_session().machine_id;
        let lane = Arc::new(ScriptedBatchLane::new(
            new_admission(machine.clone(), clock.clone()),
            clock.clone(),
            Default::default(),
        ));
        let faults = StoreFaultPlan::default();
        let outbox = FaultyOutbox {
            inner: Box::new(open_outbox(&path).await),
            plan: faults.clone(),
        };
        let directory = Arc::new(HeldDirectory::default());
        let inbox = RecipientInbox::new(
            InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
            machine,
            directory.clone(),
            clock.clone(),
        );
        let custody = CustodyLink::new(
            lane.clone(),
            scheduler.clone(),
            Arc::new(NoFlushJitter),
            OutboxWork::new(Box::new(outbox), clock.clone()),
            inbox,
        );
        let (events, _) = mpsc::unbounded_channel();
        let (calls, group) = custody_task(custody, Arc::new(ObserveTask(events)));
        Self {
            root,
            clock,
            scheduler,
            lane,
            directory,
            faults,
            calls,
            group,
        }
    }

    async fn stored_status(&self, item: &NewOutboxItem) -> OutboxRequestStatus {
        open_outbox(&self.root.path().join("machine_local.sqlite"))
            .await
            .request_status(&item.caller, &item.request)
            .await
            .unwrap()
    }

    fn offer_to_held_directory(&self) {
        self.lane.push_event(LinkLaneEvent::Deliver {
            arrived_at: self.clock.utc_now(),
            connection: self
                .lane
                .admitted
                .borrow()
                .as_ref()
                .unwrap()
                .connection
                .clone(),
            offer: direct_offer("019f0000-0000-7000-8000-000000000b42", "held lookup"),
            sender: ResolvedSender::Central {},
        });
    }
}

fn submission(item: &NewOutboxItem) -> SubmissionPayload {
    let OutboxQueuedWrite::Workspace(request) = &item.payload else {
        panic!("expected a workspace post");
    };
    SubmissionPayload::Workspace(request.clone())
}

async fn first_poll<T>(future: impl Future<Output = T>) -> Poll<T> {
    let mut future = pin!(future);
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

#[tokio::test]
async fn a_started_submit_finishes_at_stop_and_a_cancelled_join_can_be_retried() {
    let custody = RunningCustody::start().await;
    let item = thread_post(51);
    let (entered, release) = custody.faults.hold_next_insert_entry();
    let permit = custody.calls.reserve().await.unwrap();

    let reply = permit.submit(item.caller.clone(), submission(&item));
    entered.notified().await;
    custody.group.request_stop();
    assert!(first_poll(custody.group.join()).await.is_pending());
    assert_eq!(
        custody.stored_status(&item).await,
        OutboxRequestStatus::NotFound
    );
    release.send(InsertRelease::Continue).unwrap();

    assert!(
        matches!(reply.resolve().await, Submission::InOutbox(receipt) if receipt.id == item.request)
    );
    assert_eq!(custody.group.join().await, Ok(()));
    assert_eq!(custody.group.join().await, Ok(()));
    assert!(matches!(
        custody.stored_status(&item).await,
        OutboxRequestStatus::InOutbox { .. }
    ));
}

#[tokio::test]
async fn a_reserved_permit_committed_after_stop_gets_a_definite_closed_result() {
    let custody = RunningCustody::start().await;
    let item = thread_post(52);
    let active_item = thread_post(58);
    let permit = custody.calls.reserve().await.unwrap();
    let (entered, release) = custody.faults.hold_next_insert_entry();
    let active = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(active_item.caller.clone(), submission(&active_item));
    entered.notified().await;

    custody.group.request_stop();
    let reply = permit.submit(item.caller.clone(), submission(&item));

    assert!(matches!(
        first_poll(reply.resolve()).await,
        Poll::Ready(Submission::Refused(SubmitRefusal::OutboxUnavailable))
    ));
    assert!(matches!(
        custody.calls.reserve().await,
        Err(CustodyQueueClosed::Closed)
    ));
    release.send(InsertRelease::Continue).unwrap();
    assert!(matches!(active.resolve().await, Submission::InOutbox(_)));
    assert_eq!(custody.group.join().await, Ok(()));
    assert_eq!(
        custody.stored_status(&item).await,
        OutboxRequestStatus::NotFound
    );
}

#[tokio::test]
async fn orderly_stop_refuses_every_queued_operation_without_executing_it() {
    let custody = RunningCustody::start().await;
    let (entered, release) = custody.faults.hold_next_insert_entry();
    let current = thread_post(53);
    let active = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(current.caller.clone(), submission(&current));
    entered.notified().await;

    let queued = thread_post(54);
    let submit = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(queued.caller.clone(), submission(&queued));
    let status = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .request_status(queued.caller.clone(), scoped_query(&queued));
    let pending = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .pending_writes(queued.caller.clone());
    let waiting = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .waiting_for_session(recipient_session());
    custody.group.request_stop();
    release.send(InsertRelease::Continue).unwrap();

    assert_eq!(custody.group.join().await, Ok(()));
    assert!(matches!(active.resolve().await, Submission::InOutbox(_)));
    assert!(matches!(
        submit.resolve().await,
        Submission::Refused(SubmitRefusal::OutboxUnavailable)
    ));
    assert!(matches!(
        status.resolve().await,
        CustodyRequestStatus::StatusUnavailable
    ));
    assert!(matches!(
        pending.resolve().await,
        Err(CustodyLinkError::TaskClosed)
    ));
    assert!(matches!(
        waiting.resolve().await,
        Err(CustodyLinkError::TaskClosed)
    ));
    assert_eq!(
        custody.stored_status(&queued).await,
        OutboxRequestStatus::NotFound
    );
}

#[tokio::test]
async fn worker_panic_joins_the_offer_taker_and_leaves_a_queued_submit_uncertain() {
    let custody = RunningCustody::start().await;
    custody.offer_to_held_directory();
    custody.directory.entered.notified().await;
    let (entered, crash) = custody.faults.hold_next_insert_entry();
    let current = thread_post(55);
    let _active = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(current.caller.clone(), submission(&current));
    entered.notified().await;
    let queued = thread_post(56);
    let queued_reply = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(queued.caller.clone(), submission(&queued));

    crash.send(InsertRelease::Panic).unwrap();

    assert_eq!(
        custody.group.join().await,
        Err(CustodyRunFailure::WorkerPanicked)
    );
    assert!(
        first_poll(custody.directory.cancelled.notified())
            .await
            .is_ready()
    );
    assert!(
        matches!(queued_reply.resolve().await, Submission::Uncertain { request, scope }
        if request == queued.request && scope == scoped_query(&queued).scope)
    );
    assert_eq!(
        custody.stored_status(&queued).await,
        OutboxRequestStatus::NotFound
    );
}

#[tokio::test]
async fn offer_taker_panic_closes_admission_and_joins_the_started_worker() {
    let custody = RunningCustody::start().await;
    let item = thread_post(59);
    let (entered, release) = custody.faults.hold_next_insert_entry();
    let reply = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(item.caller.clone(), submission(&item));
    entered.notified().await;
    custody.offer_to_held_directory();
    custody.directory.entered.notified().await;

    custody.directory.crash.notify_one();
    custody.directory.cancelled.notified().await;

    assert!(matches!(
        first_poll(custody.calls.reserve()).await,
        Poll::Ready(Err(CustodyQueueClosed::Closed))
    ));
    assert!(first_poll(custody.group.join()).await.is_pending());
    release.send(InsertRelease::Continue).unwrap();
    assert_eq!(
        custody.group.join().await,
        Err(CustodyRunFailure::OfferTakerPanicked)
    );
    assert!(
        matches!(reply.resolve().await, Submission::InOutbox(receipt) if receipt.id == item.request)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn stop_reaps_remote_tool_waits_and_preserves_the_lost_reply_outcome() {
    let custody = RunningCustody::start().await;
    let (held_reply, release_drop) = custody.lane.hold_next_immediate_reply();
    let request = thread_post(57).request;
    let pending_reply = custody.calls.reserve().await.unwrap().submit(
        Caller::Session(recipient_session()),
        SubmissionPayload::Mail(CentralMailRequest {
            request_id: request.clone(),
            operation: CentralMailOperation::SetNoticeCadence {
                cadence: NoticeCadence::M15,
            },
        }),
    );
    held_reply.entered.notified().await;

    custody.group.request_stop();
    // The remote future's destructor occupies the only runtime worker.
    // This test's own thread can observe whether join published early.
    held_reply.dropping.notified().await;
    let joined_while_dropping = first_poll(custody.group.join()).await;
    release_drop.send(()).unwrap();

    assert!(joined_while_dropping.is_pending());
    assert_eq!(custody.group.join().await, Ok(()));
    assert!(first_poll(held_reply.ended.notified()).await.is_ready());
    assert!(
        matches!(pending_reply.resolve().await, Submission::Uncertain {
        request: lost, scope: RequestScope::Organization
    } if lost == request)
    );
}

#[tokio::test]
async fn stop_cancels_remote_tools_while_an_admitted_outbox_insert_is_held() {
    let custody = RunningCustody::start().await;
    let (immediate, release_immediate_drop) = custody.lane.hold_next_immediate_reply();
    let remote_request = thread_post(60);
    let reply = custody.calls.reserve().await.unwrap().submit(
        Caller::Session(recipient_session()),
        SubmissionPayload::Mail(CentralMailRequest {
            request_id: remote_request.request.clone(),
            operation: CentralMailOperation::SetNoticeCadence {
                cadence: NoticeCadence::M15,
            },
        }),
    );
    immediate.entered.notified().await;
    let status = custody.lane.hold_next_status();
    let status_reply = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .request_status(remote_request.caller.clone(), scoped_query(&remote_request));
    status.entered.notified().await;
    let item = thread_post(61);
    let (enter_store, write_store) = custody.faults.hold_next_insert_entry();
    let local_reply = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(item.caller.clone(), submission(&item));
    enter_store.notified().await;

    custody.group.request_stop();
    release_immediate_drop.send(()).unwrap();
    // Both lane answers are ready before either stopped task can be polled.
    immediate.answer.notify_one();
    status
        .accepted
        .send_replace(Some(custody.clock.monotonic_now()));

    assert!(matches!(reply.resolve().await, Submission::Uncertain {
        request, scope: RequestScope::Organization
    } if request == remote_request.request));
    assert!(matches!(
        status_reply.resolve().await,
        CustodyRequestStatus::StatusUnavailable
    ));
    assert!(first_poll(custody.group.join()).await.is_pending());
    assert_eq!(
        custody.stored_status(&item).await,
        OutboxRequestStatus::NotFound
    );
    write_store.send(InsertRelease::Continue).unwrap();
    assert!(
        matches!(local_reply.resolve().await, Submission::InOutbox(receipt) if receipt.id == item.request)
    );
    assert_eq!(custody.group.join().await, Ok(()));
}

#[tokio::test]
async fn a_recipient_lookup_timer_cannot_report_a_parked_worker() {
    let custody = RunningCustody::start().await;
    let post = thread_post(62);
    let (entered, release) = custody.faults.hold_next_insert_entry();
    let reply = custody
        .calls
        .reserve()
        .await
        .unwrap()
        .submit(post.caller.clone(), submission(&post));
    entered.notified().await;
    let checkpoint = custody.scheduler.checkpoint();
    custody.offer_to_held_directory();
    custody.directory.entered.notified().await;
    let lookup_deadline = custody.clock.monotonic_now().after(RECIPIENT_LOOKUP_BUDGET);
    custody.clock.not_due_check_now(lookup_deadline).await;

    assert!(
        first_poll(custody.scheduler.wait_for_armed_timer(checkpoint, None))
            .await
            .is_pending()
    );
    custody.group.request_stop();
    release.send(InsertRelease::Continue).unwrap();
    assert!(matches!(reply.resolve().await, Submission::InOutbox(_)));
    assert_eq!(custody.group.join().await, Ok(()));
}
