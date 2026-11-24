//! The writer's original deadline fences the first byte and independently ends a held claim.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "Fixture setup and clock steps fail at the unexpected operation."
)]

use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LinkLaneDisposition, LinkLaneEvent, ReadyLinkLane,
    RegisteredConnection, RequestStatusCommand, SubmitReceiptsCommand, WriteBatchCommand,
    WriteBatchDisposition,
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalOutbox, MachineLocalStore, OutboxCancellation, OutboxQueuedWrite,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_submission::{Submission, SubmissionPayload},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use tokio::sync::watch;
use workspace_ids::{MachineId, RequestId};

use super::{
    batch_replay::{
        KnownRecipient, NoFlushJitter, ScriptedBatch, ScriptedBatchLane, new_admission,
    },
    outbox_timing::{manual_clock, thread_post},
    worker_orphans::{flush_event, start_worker},
    worker_turns::{ObservedWorker, WorkerClock},
};

const LAST_MARK_MS: i64 = 1_763_380_800_000;
const AGENT_WINDOW: Duration = Duration::from_secs(15 * 60);

struct ElapsedSettlementLane {
    peer: ScriptedBatchLane,
    clock: Arc<ManualClock>,
    settlement_delay: Duration,
}

impl ReadyLinkLane for ElapsedSettlementLane {
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
            assert!(
                self.clock.monotonic_now() < command.send_before,
                "fixture handoff must precede the exclusive fence"
            );
            let reply = self.peer.submit_write_batch(command).await;
            self.clock.advance(self.settlement_delay).unwrap();
            reply
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

async fn selection_fixture(
    start_ms: i64,
    settlement_delay: Duration,
    lost: BTreeSet<RequestId>,
) -> (
    tempfile::TempDir,
    CustodyLink,
    Arc<ElapsedSettlementLane>,
    MachineId,
    Arc<WorkerClock>,
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(start_ms);
    let observed = WorkerClock::observing(clock.clone());
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let peer = ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        clock.clone(),
        lost,
    );
    peer.admitted.send_replace(None);
    let outbox_store = MachineLocalStore::open(&path).await.unwrap();
    let lane = Arc::new(ElapsedSettlementLane {
        peer,
        clock: clock.clone(),
        settlement_delay,
    });
    let outbox = OutboxWork::new(
        Box::new(MachineLocalOutbox::new(outbox_store)),
        clock.clone(),
    );
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
        machine.clone(),
        Arc::new(KnownRecipient),
        clock.clone(),
    );
    let custody = CustodyLink::new(
        lane.clone(),
        observed.clone(),
        Arc::new(NoFlushJitter),
        outbox,
        inbox,
    );
    (root, custody, lane, machine, observed)
}

async fn enqueue_post(custody: &mut CustodyLink, seed: u16) {
    let item = thread_post(seed);
    let OutboxQueuedWrite::Workspace(request) = item.payload else {
        panic!("fixture must supply a thread post");
    };
    assert!(matches!(
        custody
            .submit(item.caller, SubmissionPayload::Workspace(request))
            .await,
        Submission::InOutbox(_)
    ));
}

#[tokio::test]
async fn the_available_mark_claims_and_sends_a_write_with_ten_seconds_left() {
    let until_mark = AGENT_WINDOW - Duration::from_secs(10);
    let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
    let (_root, mut custody, lane, machine, observed) =
        selection_fixture(start, Duration::ZERO, BTreeSet::new()).await;
    let item = thread_post(91);
    enqueue_post(&mut custody, 91).await;
    lane.clock.advance(until_mark).unwrap();
    lane.peer
        .admitted
        .send_replace(Some(new_admission(machine, lane.clock.clone())));

    let mut worker = ObservedWorker::start(custody, observed.clone());
    observed.wait_after(0).await;
    let flush = worker.take_reports().unwrap();
    assert!(
        matches!(flush.answered.as_slice(), [answer] if answer.request == item.request),
        "a write with ten seconds left must be claimed and sent"
    );
    let commands = lane.peer.batch_commands();
    let [batch] = commands.as_slice() else {
        panic!("the eligible near-cutoff write must leave in a frame");
    };
    assert_eq!(
        batch.send_before,
        ClockInstant::ZERO.after(until_mark + Duration::from_secs(3))
    );
    worker.stop().await;
}

#[tokio::test]
async fn a_send_at_the_last_eligible_millisecond_ends_unconfirmed_at_its_deadline() {
    let until_mark = AGENT_WINDOW - Duration::from_millis(1);
    let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
    let item = thread_post(92);
    let (_root, mut custody, lane, machine, _observed) = selection_fixture(
        start,
        Duration::from_millis(1),
        [item.request.clone()].into(),
    )
    .await;
    enqueue_post(&mut custody, 92).await;
    lane.clock.advance(until_mark).unwrap();
    lane.peer
        .admitted
        .send_replace(Some(new_admission(machine, lane.clock.clone())));

    let (_calls, group, mut events) = start_worker(custody);
    let flush = flush_event(&mut events, "the first frame's exact cutoff").await;
    let commands = lane.peer.batch_commands();
    assert_eq!(
        commands.len(),
        1,
        "the last eligible millisecond must still send one frame"
    );
    let batch = commands.first().unwrap();
    assert_eq!(
        batch.send_before,
        ClockInstant::ZERO.after(until_mark + Duration::from_millis(1))
    );
    assert!(flush.answered.is_empty());
    assert!(
        matches!(flush.ended.as_slice(), [ending] if ending.request_id == item.request && ending.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
    );
    group.request_stop();
    group.join().await.unwrap();
}

#[tokio::test]
async fn a_checked_answer_before_the_original_deadline_settles_the_write() {
    let until_mark = AGENT_WINDOW - Duration::from_millis(1);
    let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
    let (_root, mut custody, lane, machine, observed) =
        selection_fixture(start, Duration::from_secs(1), BTreeSet::new()).await;
    let item = thread_post(93);
    enqueue_post(&mut custody, 93).await;
    lane.clock.advance(until_mark).unwrap();
    lane.peer
        .admitted
        .send_replace(Some(new_admission(machine, lane.clock.clone())));

    let mut worker = ObservedWorker::start(custody, observed.clone());
    observed.wait_after(0).await;
    let flush = worker.take_reports().unwrap();
    assert!(matches!(flush.answered.as_slice(), [answer] if answer.request == item.request));
    assert!(
        worker
            .advance(Duration::from_secs(1))
            .await
            .unwrap()
            .ended
            .is_empty()
    );
    worker.stop().await;
}

#[tokio::test]
async fn an_untouched_result_ready_at_the_fence_uses_the_members_original_cutoff() {
    for (remaining, closes_in, expected) in [
        (
            Duration::from_millis(1),
            Duration::from_millis(1),
            OutboxCancellation::NotConfirmedWithin15Minutes,
        ),
        (
            Duration::from_secs(10),
            Duration::from_secs(3),
            OutboxCancellation::NotSentWithin15Minutes,
        ),
    ] {
        let until_mark = AGENT_WINDOW - remaining;
        let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
        let (_root, mut custody, lane, machine, observed) =
            selection_fixture(start, closes_in, BTreeSet::new()).await;
        let item = thread_post(94);
        enqueue_post(&mut custody, 94).await;
        lane.peer
            .decide_next_batch(Some(ScriptedBatch::UntouchedAtFence));
        lane.clock.advance(until_mark).unwrap();
        lane.peer
            .admitted
            .send_replace(Some(new_admission(machine, lane.clock.clone())));

        let mut worker = ObservedWorker::start(custody, observed.clone());
        observed.wait_after(0).await;
        let flush = worker.take_reports().unwrap();
        assert!(flush.answered.is_empty() && flush.issues.is_empty());
        let endings = if remaining == closes_in {
            flush.ended
        } else {
            assert!(
                flush.ended.is_empty(),
                "the mark close cannot end a still-eligible write"
            );
            worker.advance(remaining - closes_in).await.unwrap().ended
        };
        assert!(
            matches!(endings.as_slice(), [ended] if ended.request_id == item.request && ended.reason == expected),
            "the driver-held original deadline and earlier mark close must have different endings: {endings:?}"
        );
        assert!(
            worker
                .advance(Duration::from_secs(1))
                .await
                .unwrap()
                .ended
                .is_empty()
        );
        worker.stop().await;
    }
}
