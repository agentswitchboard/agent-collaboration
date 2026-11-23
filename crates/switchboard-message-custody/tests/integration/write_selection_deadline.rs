//! The writer's original deadline includes the lane's whole settlement window.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "Fixture setup and clock steps fail at the unexpected operation."
)]

use central_link_client::{
    AdmittedConnection, CorrelatedReceiptResults, CorrelatedRequestReply, CorrelatedStatusReply,
    ImmediateRequestCommand, LINK_LANE_SETTLEMENT_LIMIT, LinkLaneDisposition, LinkLaneEvent,
    ReadyLinkLane, RegisteredConnection, RequestStatusCommand, SubmitReceiptsCommand,
    WriteBatchCommand, WriteBatchDisposition,
};
use central_link_protocol::{RequestScope, RequestStatusQuery};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use std::{collections::BTreeSet, future::Future, pin::Pin, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalOutbox, MachineLocalStore, OutboxCancellation, OutboxQueuedWrite,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use tokio::sync::watch;
use workspace_ids::{MachineId, RequestId};

use super::{
    batch_replay::{KnownRecipient, NoFlushJitter, ScriptedBatchLane, new_admission},
    outbox_timing::{manual_clock, thread_post},
};

const LAST_MARK_MS: i64 = 1_763_380_800_000;
const AGENT_WINDOW: Duration = Duration::from_secs(15 * 60);

struct ElapsedSettlementLane {
    peer: ScriptedBatchLane,
    clock: Arc<ManualClock>,
    settlement_delay: Duration,
    outbox_commits: watch::Receiver<u64>,
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
                "fixture first byte must respect the exclusive fence"
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
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(start_ms);
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let peer = ScriptedBatchLane::new(new_admission(machine.clone(), clock.clone()), lost);
    peer.admitted.send_replace(None);
    let outbox_store = MachineLocalStore::open(&path).await.unwrap();
    let lane = Arc::new(ElapsedSettlementLane {
        peer,
        clock: clock.clone(),
        settlement_delay,
        outbox_commits: outbox_store.subscribe_commits(),
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
    let custody = CustodyLink::new(lane.clone(), clock, Arc::new(NoFlushJitter), outbox, inbox);
    (root, custody, lane, machine)
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
async fn the_only_available_mark_cannot_claim_a_write_with_one_second_left() {
    // Queue at 12:00:01, admit the only usable connection at 12:15:00.
    let until_mark = AGENT_WINDOW - Duration::from_secs(1);
    let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
    let (_root, mut custody, lane, machine) =
        selection_fixture(start, Duration::ZERO, BTreeSet::new()).await;
    enqueue_post(&mut custody, 91).await;
    lane.clock.advance(until_mark).unwrap();
    lane.peer
        .admitted
        .send_replace(Some(new_admission(machine, lane.clock.clone())));

    let committed_after_enqueue = *lane.outbox_commits.borrow();
    let flush = custody.flush_writes().await.unwrap();
    assert_eq!(
        *lane.outbox_commits.borrow(),
        committed_after_enqueue,
        "an ineligible row must not be claimed then returned, even if no frame leaves"
    );
    assert!(
        lane.peer.batch_commands().is_empty(),
        "a write without settlement time must stay off the lane"
    );
    assert!(flush.answered.is_empty() && flush.ended.is_empty());
    let item = thread_post(91);
    let OutboxQueuedWrite::Workspace(request) = &item.payload else {
        panic!("fixture must supply a thread post");
    };
    let query = RequestStatusQuery {
        request_id: item.request.clone(),
        scope: RequestScope::Workspace(request.workspace_id.clone().unwrap()),
    };
    assert!(matches!(
        custody.request_status(item.caller, query).await,
        CustodyRequestStatus::InOutbox { .. }
    ));
    lane.clock.advance(Duration::from_secs(1)).unwrap();
    let ended = custody.end_due_writes().await.unwrap();
    assert!(
        matches!(ended.ended.as_slice(), [ending] if ending.request_id == item.request && ending.reason == OutboxCancellation::NotSentWithin15Minutes)
    );
}

#[tokio::test]
async fn a_send_at_the_last_eligible_millisecond_ends_unconfirmed_at_its_deadline() {
    let until_mark = AGENT_WINDOW - LINK_LANE_SETTLEMENT_LIMIT - Duration::from_millis(1);
    let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
    let item = thread_post(92);
    let (_root, mut custody, lane, machine) = selection_fixture(
        start,
        LINK_LANE_SETTLEMENT_LIMIT,
        [item.request.clone()].into(),
    )
    .await;
    enqueue_post(&mut custody, 92).await;
    lane.clock.advance(until_mark).unwrap();
    lane.peer
        .admitted
        .send_replace(Some(new_admission(machine, lane.clock.clone())));

    let flush = custody.flush_writes().await.unwrap();
    let commands = lane.peer.batch_commands();
    let [batch] = commands.as_slice() else {
        panic!("the last eligible millisecond must still send one frame");
    };
    assert_eq!(
        batch.send_before,
        ClockInstant::ZERO.after(until_mark + Duration::from_millis(1))
    );
    assert!(flush.answered.is_empty() && flush.ended.is_empty());
    assert!(custody.end_due_writes().await.unwrap().ended.is_empty());
    assert_eq!(
        custody.next_write_deadline().await.unwrap(),
        Some(ClockInstant::ZERO.after(AGENT_WINDOW))
    );
    lane.clock.advance(Duration::from_millis(1)).unwrap();
    let ended = custody.end_due_writes().await.unwrap();
    assert!(
        matches!(ended.ended.as_slice(), [ending] if ending.request_id == item.request && ending.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
    );
}

#[tokio::test]
async fn a_checked_answer_before_the_original_deadline_settles_the_write() {
    let until_mark = AGENT_WINDOW - LINK_LANE_SETTLEMENT_LIMIT - Duration::from_millis(1);
    let start = LAST_MARK_MS - i64::try_from(until_mark.as_millis()).unwrap();
    let (_root, mut custody, lane, machine) =
        selection_fixture(start, Duration::from_secs(1), BTreeSet::new()).await;
    let item = thread_post(93);
    enqueue_post(&mut custody, 93).await;
    lane.clock.advance(until_mark).unwrap();
    lane.peer
        .admitted
        .send_replace(Some(new_admission(machine, lane.clock.clone())));

    let flush = custody.flush_writes().await.unwrap();
    assert!(matches!(flush.answered.as_slice(), [answer] if answer.request == item.request));
    assert!(custody.next_write_deadline().await.unwrap().is_none());
    lane.clock
        .advance(
            ClockInstant::ZERO
                .after(AGENT_WINDOW)
                .saturating_duration_since(lane.clock.monotonic_now()),
        )
        .unwrap();
    assert!(custody.end_due_writes().await.unwrap().ended.is_empty());
}
