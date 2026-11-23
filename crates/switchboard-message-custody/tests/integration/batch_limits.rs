#![expect(
    clippy::unwrap_used,
    reason = "A bad fixture or missing peer batch must fail its test at the point of observation."
)]

use central_link_client::{LINK_LANE_FRAME_BYTES, LinkLaneEvent, LinkLocalFault};
use central_link_protocol::{DataFrame, FrameDirection, LinkFrame, encode_text};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use std::{path::Path, sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    InboxStore, MachineLocalStore, OutboxAccepted, OutboxInsert, OutboxQueuedWrite,
    OutboxRequestStatus, OutboxStorage, OutboxWriteOutcome,
};
use switchboard_message_custody::{
    custody_link::{CustodyLink, CustodyLinkObservation},
    custody_operator::CustodyOperatorEvent,
    custody_submission::{Submission, SubmissionPayload, SubmitRefusal},
    custody_task::{CustodyTaskEvent, custody_task},
    outbox_work::OutboxWork,
    recipient_inbox::RecipientInbox,
};
use tokio::sync::{mpsc, watch};
use workspace_ids::MachineId;
use workspace_model::{Addressee, ChangePosition, PostText, WorkspaceOperation, WorkspaceRequest};

use super::{
    batch_replay::{KnownRecipient, NoFlushJitter, ScriptedBatchLane, new_admission},
    custody_task::ObserveTask,
    outbox_timing::{manual_clock, open_outbox, thread_post},
    worker_turns::{ObservedWorker, WorkerClock},
};

const MARK_MS: i64 = 1_763_380_800_000;

pub(super) async fn observed_limits_fixture() -> (
    tempfile::TempDir,
    CustodyLink,
    Arc<ScriptedBatchLane>,
    Arc<WorkerClock>,
) {
    let clock = WorkerClock::observing(manual_clock(MARK_MS - 20_000));
    let (root, custody, lane) =
        limits_fixture_with_clock(clock.manual.clone(), clock.clone()).await;
    (root, custody, lane, clock)
}

pub(super) async fn limits_fixture() -> (
    tempfile::TempDir,
    CustodyLink,
    Arc<ScriptedBatchLane>,
    Arc<ManualClock>,
) {
    let clock = manual_clock(MARK_MS - 20_000);
    let (root, custody, lane) = limits_fixture_with_clock(clock.clone(), clock.clone()).await;
    (root, custody, lane, clock)
}

pub(super) async fn limits_fixture_with_clock(
    clock: Arc<ManualClock>,
    observed_clock: Arc<dyn RuntimeClock>,
) -> (tempfile::TempDir, CustodyLink, Arc<ScriptedBatchLane>) {
    let root = tempfile::tempdir().unwrap();
    let database = root.path().join("machine_local.sqlite");
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let lane = Arc::new(ScriptedBatchLane::new(
        new_admission(machine.clone(), clock.clone()),
        clock.clone(),
        Default::default(),
    ));
    let custody = custody_with_store(&database, lane.clone(), observed_clock, machine).await;
    (root, custody, lane)
}

pub(super) async fn custody_with_store(
    database: &Path,
    lane: Arc<ScriptedBatchLane>,
    clock: Arc<dyn RuntimeClock>,
    machine: MachineId,
) -> CustodyLink {
    let outbox = OutboxWork::new(Box::new(open_outbox(database).await), clock.clone());
    let inbox = RecipientInbox::new(
        InboxStore::new(MachineLocalStore::open(database).await.unwrap()),
        machine,
        Arc::new(KnownRecipient),
        clock.clone(),
    );
    CustodyLink::new(lane, clock, Arc::new(NoFlushJitter), outbox, inbox)
}

async fn queue_post(
    custody: &mut CustodyLink,
    seed: u16,
    update: impl FnOnce(&mut WorkspaceRequest),
) -> Submission {
    let item = thread_post(seed);
    let OutboxQueuedWrite::Workspace(mut request) = item.payload else {
        return Submission::Refused(SubmitRefusal::OutboxUnavailable);
    };
    update(&mut request);
    custody
        .submit(item.caller, SubmissionPayload::Workspace(request))
        .await
}

#[tokio::test]
async fn a_single_overbudget_write_is_refused_before_entering_the_store() {
    let (_root, mut custody, lane, _) = limits_fixture().await;
    let submission = queue_post(&mut custody, 1, |request| {
        let WorkspaceOperation::ThreadPost { addressees, .. } = &mut request.operation else {
            return;
        };
        *addressees = vec![
            Addressee::Actor {
                actor_identity_id: "019f0000-0000-7000-8000-000000000071".parse().unwrap(),
            };
            20_000
        ];
    })
    .await;

    assert!(matches!(
        submission,
        Submission::Refused(SubmitRefusal::TooLargeToSend)
    ));
    assert!(
        custody
            .pending_writes(&thread_post(1).caller)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(lane.batch_commands().is_empty());
}

#[tokio::test]
async fn the_byte_budget_splits_frames_without_deferring_the_suffix_to_the_next_mark() {
    let (_root, mut custody, lane, clock) = observed_limits_fixture().await;
    for seed in (1..=20).rev() {
        let submitted = queue_post(&mut custody, seed, |request| {
            let WorkspaceOperation::ThreadPost { body, .. } = &mut request.operation else {
                return;
            };
            *body = PostText::try_from("x".repeat(64 * 1024)).unwrap();
        })
        .await;
        assert!(matches!(submitted, Submission::InOutbox(_)));
    }
    let mut worker = ObservedWorker::start(custody, clock);
    let first = worker.advance(Duration::from_secs(20)).await.unwrap();
    assert_eq!(
        first
            .answered
            .iter()
            .map(|write| write.request.clone())
            .collect::<Vec<_>>(),
        (1..=20)
            .map(|seed| thread_post(seed).request)
            .collect::<Vec<_>>()
    );
    let frames = lane.batch_commands();
    let lengths: Vec<_> = frames
        .iter()
        .map(|frame| frame.requests.requests().len())
        .collect();
    assert_eq!(lengths, [15, 5]);
    for command in frames {
        let frame = LinkFrame::Data(DataFrame::Batch {
            requests: command.requests,
        });
        assert!(
            encode_text(&frame, FrameDirection::ToCentral)
                .unwrap()
                .len()
                <= LINK_LANE_FRAME_BYTES
        );
    }
    worker.stop().await;
}

#[tokio::test]
async fn a_write_expiring_inside_the_mark_does_not_shorten_the_later_frames_fence() {
    let (root, mut custody, lane, clock) = observed_limits_fixture().await;
    let mut fixture_store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let aged_write = thread_post(1);
    assert_eq!(
        fixture_store
            .insert(&aged_write, MARK_MS - 899_000)
            .await
            .unwrap(),
        OutboxInsert::Stored
    );
    let submitted = queue_post(&mut custody, 2, |_| {}).await;
    assert!(matches!(submitted, Submission::InOutbox(_)));

    let mut worker = ObservedWorker::start(custody, clock);
    assert_eq!(
        worker
            .advance(Duration::from_secs(20))
            .await
            .unwrap()
            .answered
            .len(),
        2
    );
    let commands = lane.batch_commands();
    assert_eq!(
        commands.len(),
        2,
        "early and later cutoffs must use separate frames"
    );
    let early = commands.first().unwrap();
    let later = commands.last().unwrap();
    assert_eq!(
        early.send_before,
        ClockInstant::ZERO.after(Duration::from_secs(21))
    );
    assert_eq!(
        later.send_before,
        ClockInstant::ZERO.after(Duration::from_secs(23))
    );
    assert_eq!(early.requests.requests().len(), 1);
    assert_eq!(later.requests.requests().len(), 1);
    worker.stop().await;
}

#[tokio::test]
async fn an_orphaned_claim_arms_the_flush_after_the_last_waiting_row_disappears() {
    let (root, custody, lane, clock) = observed_limits_fixture().await;
    let mut write_store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let original = thread_post(91);
    let queued_at = MARK_MS - 20_000;
    assert_eq!(
        write_store.insert(&original, queued_at).await.unwrap(),
        OutboxInsert::Stored
    );
    let claim = write_store
        .claim_next_batch(
            MARK_MS,
            1,
            queued_at,
            queued_at,
            &std::collections::BTreeSet::new(),
        )
        .await
        .unwrap();
    assert!(
        matches!(claim.healthy.writes.as_slice(), [write] if write.request == original.request)
    );
    assert!(
        write_store
            .earliest_waiting()
            .await
            .unwrap()
            .healthy
            .is_none()
    );

    let mut worker = ObservedWorker::start(custody, clock.clone());
    clock.wait_after(0).await;
    assert!(
        matches!(
            lane.take_calls().as_slice(),
            [super::batch_replay::ScriptedCommand::RequestStatus(command)]
                if command.caller == original.caller && command.query.request_id == original.request
        ),
        "Central's scoped miss must precede the orphan's return to waiting"
    );
    assert!(
        clock
            .manual
            .wait_registrations_since(0)
            .iter()
            .filter(|wait| !wait.dropped
                && wait.deadline == ClockInstant::ZERO.after(Duration::from_secs(20)))
            .count()
            >= 2,
        "an orphan must arm a flush even with no waiting rows"
    );
    let flush = worker.advance(Duration::from_secs(20)).await.unwrap();
    assert!(matches!(flush.answered.as_slice(), [answer] if answer.request == original.request));
    assert_eq!(lane.batch_commands().len(), 1);
    worker.stop().await;
}

#[tokio::test]
async fn an_offer_consumer_fault_is_visible_on_the_operator_path_only() {
    let (_root, mut custody, lane, _) = limits_fixture().await;
    let connection = lane.admitted.borrow().as_ref().unwrap().connection.clone();
    lane.admitted.send_replace(None);
    lane.push_event(LinkLaneEvent::ConnectionFaulted {
        connection: connection.clone(),
        cause: LinkLocalFault::DeliveryConsumerStalled,
    });

    assert_eq!(
        custody.observe_next_event().await.unwrap(),
        CustodyLinkObservation::ConnectionFaulted {
            connection: connection.clone(),
            cause: LinkLocalFault::DeliveryConsumerStalled,
        }
    );
    assert_eq!(
        custody.take_operator_events().await,
        [CustodyOperatorEvent::LinkFaulted {
            connection,
            cause: LinkLocalFault::DeliveryConsumerStalled,
        }]
    );
    assert!(lane.admitted.borrow().is_none());
    assert!(lane.take_calls().is_empty());
}

#[tokio::test]
async fn a_flush_end_prunes_only_terminal_writes_outside_sixty_days() {
    let (root, custody, _, clock) = limits_fixture().await;
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let retention_boundary = MARK_MS - 60 * 24 * 60 * 60 * 1_000;
    let mut old = thread_post(100);
    old.flush_at_ms = retention_boundary - 1;
    let mut recent = thread_post(101);
    recent.flush_at_ms = retention_boundary + 1;
    for write in [&old, &recent] {
        let ended_at = write.flush_at_ms;
        assert_eq!(
            store.insert(write, ended_at).await.unwrap(),
            OutboxInsert::Stored
        );
        let batch = store
            .claim_next_batch(
                ended_at,
                1,
                ended_at,
                ended_at,
                &std::collections::BTreeSet::new(),
            )
            .await
            .unwrap();
        assert!(
            matches!(batch.healthy.writes.as_slice(), [claimed] if claimed.request == write.request)
        );
        let decision = OutboxWriteOutcome::from_accepted(
            &write.payload,
            OutboxAccepted::ThreadPost {
                post_id: "019f0000-0000-7000-8000-000000000081".parse().unwrap(),
                position: ChangePosition::try_from(1_u64).unwrap(),
                actors: Vec::new(),
            },
        )
        .unwrap();
        store
            .record_outcome(&write.request, &decision, ended_at)
            .await
            .unwrap();
    }
    let (stop, stopped) = watch::channel(false);
    let (sender, mut events) = mpsc::unbounded_channel();
    let (calls, task) = custody_task(custody, stopped, Arc::new(ObserveTask(sender)));
    let running = tokio::spawn(task);
    let fresh = thread_post(1);
    let OutboxQueuedWrite::Workspace(request) = fresh.payload else {
        panic!("thread post fixture required");
    };
    assert!(matches!(
        calls
            .submit(fresh.caller, SubmissionPayload::Workspace(request))
            .await,
        Submission::InOutbox(_)
    ));
    clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    clock.advance(Duration::from_secs(20)).unwrap();
    loop {
        if matches!(events.recv().await.unwrap(), CustodyTaskEvent::Flush(report)
            if matches!(report.answered.as_slice(), [answer] if answer.request == fresh.request))
        {
            break;
        }
    }
    stop.send_replace(true);
    running.await.unwrap();
    assert_eq!(
        store
            .request_status(&old.caller, &old.request)
            .await
            .unwrap(),
        OutboxRequestStatus::NotFound
    );
    assert!(matches!(
        store
            .request_status(&recent.caller, &recent.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(_)
    ));
}
