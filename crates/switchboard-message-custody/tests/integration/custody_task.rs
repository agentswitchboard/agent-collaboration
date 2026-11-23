use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RegisteredConnection, RequestStatusCommand, SubmitReceiptsCommand, WriteBatchCommand,
    WriteBatchDisposition,
};
use router_clock::ClockInstant;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use switchboard_machine_core_store::{InboxStore, MachineLocalStore, OutboxQueuedWrite};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::{CustodyEventSink, CustodyTaskEvent, custody_task},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use tokio::sync::{Notify, mpsc, watch};
use workspace_ids::{MachineId, MessageId};
use workspace_model::central_mail::MessageStatus;

use super::{
    batch_replay::{KnownRecipient, NoFlushJitter, ScriptedBatchLane, new_admission},
    outbox_timing::{manual_clock, open_outbox, thread_post},
};

struct HoldBatchLane {
    peer: Arc<ScriptedBatchLane>,
    submitted: Notify,
    release: Notify,
}

impl ReadyLinkLane for HoldBatchLane {
    fn watch_admitted(&self) -> watch::Receiver<Option<AdmittedConnection>> {
        self.peer.watch_admitted()
    }
    fn watch_registered(&self) -> watch::Receiver<Option<RegisteredConnection>> {
        self.peer.watch_registered()
    }

    fn submit_write_batch(
        &self,
        command: WriteBatchCommand,
    ) -> Pin<Box<dyn Future<Output = WriteBatchDisposition> + Send + '_>> {
        Box::pin(async move {
            self.submitted.notify_one();
            self.release.notified().await;
            self.peer.submit_write_batch(command).await
        })
    }

    fn submit_receipts(
        &self,
        command: SubmitReceiptsCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedReceiptResults>> + Send + '_>>
    {
        self.peer.submit_receipts(command)
    }
    fn request_status(
        &self,
        command: RequestStatusCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedStatusReply>> + Send + '_>> {
        self.peer.request_status(command)
    }
    fn submit_immediate_request(
        &self,
        command: ImmediateRequestCommand,
    ) -> Pin<Box<dyn Future<Output = LinkLaneDisposition<CorrelatedRequestReply>> + Send + '_>>
    {
        self.peer.submit_immediate_request(command)
    }
    fn next_event(&self) -> Pin<Box<dyn Future<Output = Option<LinkLaneEvent>> + Send + '_>> {
        self.peer.next_event()
    }
}

/// A test worker records custody events for assertions on its flush report.
pub(super) struct ObserveTask(pub(super) mpsc::UnboundedSender<CustodyTaskEvent>);

impl CustodyEventSink for ObserveTask {
    fn record_event(&self, event: CustodyTaskEvent) {
        let _ = self.0.send(event);
    }
}

#[tokio::test]
async fn an_awaiting_batch_does_not_hold_an_immediate_status_read() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(1_763_380_780_000);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let peer = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        Default::default(),
    ));
    let lane = Arc::new(HoldBatchLane {
        peer: peer.clone(),
        submitted: Notify::new(),
        release: Notify::new(),
    });
    let outbox = OutboxWork::new(Box::new(open_outbox(&path).await), clock.clone());
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        machine,
        Arc::new(KnownRecipient),
        clock.clone(),
    );
    let custody = CustodyLink::new(
        lane.clone(),
        clock.clone(),
        Arc::new(NoFlushJitter),
        outbox,
        inbox,
    );
    let (stop, stopped) = watch::channel(false);
    let (sender, mut events) = mpsc::unbounded_channel();
    let (calls, task) = custody_task(custody, stopped, Arc::new(ObserveTask(sender)));
    let running = tokio::spawn(task);
    let item = thread_post(98);
    let OutboxQueuedWrite::Workspace(request) = item.payload else {
        panic!("thread post fixture required");
    };
    assert!(matches!(
        calls
            .submit(item.caller.clone(), SubmissionPayload::Workspace(request))
            .await,
        Submission::InOutbox(_)
    ));
    clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    clock.advance(Duration::from_secs(20)).unwrap();
    lane.submitted.notified().await;

    let message_id: MessageId = "019f0000-0000-7000-8000-000000000099".parse().unwrap();
    assert_eq!(
        calls.message_status(item.caller, message_id).await,
        Ok(Some(MessageStatus::NotFound {})),
        "an open batch cannot withhold the immediate read"
    );
    assert_eq!(peer.take_calls().len(), 1);
    assert!(
        peer.batch_commands().is_empty(),
        "the scripted batch still waits on the test barrier"
    );

    lane.release.notify_one();
    let observed = events.recv().await.unwrap();
    assert!(matches!(observed, CustodyTaskEvent::Flush(report)
        if matches!(report.answered.as_slice(), [answer] if answer.request == item.request)));
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_caller_gone_before_task_admission_leaves_no_outbox_row() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(1_763_380_780_000);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        Default::default(),
    ));
    let outbox = OutboxWork::new(Box::new(open_outbox(&path).await), clock.clone());
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        machine,
        Arc::new(KnownRecipient),
        clock.clone(),
    );
    let custody = CustodyLink::new(lane.clone(), clock, Arc::new(NoFlushJitter), outbox, inbox);
    let (stop, stopped) = watch::channel(false);
    let (sender, _events) = mpsc::unbounded_channel();
    let (calls, task) = custody_task(custody, stopped, Arc::new(ObserveTask(sender)));
    let item = thread_post(99);
    let OutboxQueuedWrite::Workspace(request) = item.payload else {
        panic!("thread post fixture required");
    };

    let mut submission =
        Box::pin(calls.submit(item.caller.clone(), SubmissionPayload::Workspace(request)));
    assert!(
        poll_fn(|context| Poll::Ready(submission.as_mut().poll(context)))
            .await
            .is_pending()
    );
    drop(submission);
    let running = tokio::spawn(task);

    assert!(
        calls.pending_writes(item.caller).await.unwrap().is_empty(),
        "a caller lost before admission cannot leave a row"
    );
    assert!(lane.batch_commands().is_empty());
    stop.send_replace(true);
    running.await.unwrap();
}
