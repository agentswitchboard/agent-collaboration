//! The worker drains ordered frames while the shared minute window and the original deadlines permit.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A malformed fixture or missing result fails the behavior it was set up to prove."
)]

use chrono::{DateTime, Utc};
use router_clock::{ClockInstant, ManualClock, RuntimeClock};
use runtime_primitives::{Clock, SyncTime};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use switchboard_machine_core_store::{OutboxCancellation, OutboxQueuedWrite, OutboxStorage};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::CustodyTaskEvent,
    outbox_dispatch::FlushReport,
};
use tokio::sync::{mpsc, watch};
use workspace_ids::RequestId;
use workspace_model::{Caller, Timestamp};

use super::{
    batch_limits::{limits_fixture, limits_fixture_with_clock},
    batch_replay::{
        ScriptedBatch, ScriptedBatchLane, ScriptedCommand, ScriptedMemberFate, new_admission,
    },
    faulted_overlay::FaultCampaign,
    faulty_outbox::{FaultSite, FaultStage},
    outbox_timing::{manual_clock, open_outbox, thread_post},
    worker_orphans::{flush_event, scoped_query, start_worker},
};

const FIRST_MARK: Duration = Duration::from_secs(20);
const SECOND_MARK: Duration = Duration::from_secs(80);
const MARK_MS: i64 = 1_763_380_800_000;

/// These tests observe second-mark sleepers while delegating time to their one manual clock.
struct NextMarkClock {
    source: Arc<ManualClock>,
    pending_wakes: watch::Sender<usize>,
}

struct MarkWait<'clock>(&'clock watch::Sender<usize>);

impl Drop for MarkWait<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|wakes| *wakes -= 1);
    }
}

impl Clock for NextMarkClock {
    fn now(&self) -> SyncTime {
        self.source.now()
    }
}

impl RuntimeClock for NextMarkClock {
    fn utc_now(&self) -> DateTime<Utc> {
        self.source.utc_now()
    }

    fn monotonic_now(&self) -> ClockInstant {
        self.source.monotonic_now()
    }

    fn sleep_until(&self, deadline: ClockInstant) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let sleeping = self.source.sleep_until(deadline);
        if deadline != ClockInstant::ZERO.after(SECOND_MARK) {
            return sleeping;
        }
        Box::pin(async move {
            self.pending_wakes.send_modify(|wakes| *wakes += 1);
            let _wait = MarkWait(&self.pending_wakes);
            sleeping.await;
        })
    }
}

impl NextMarkClock {
    fn observing_next_mark() -> Arc<Self> {
        Arc::new(Self {
            source: manual_clock(MARK_MS - 20_000),
            pending_wakes: watch::channel(0).0,
        })
    }

    async fn suffix_is_deferred(&self) {
        // The receipt wait is always armed for this minute. A second live wait
        // proves the worker scheduled another outbox frame on that later mark.
        let mut wakes = self.pending_wakes.subscribe();
        assert!(wakes.wait_for(|wakes| *wakes == 2).await.is_ok());
    }
}

async fn observed_mark_fixture() -> (
    tempfile::TempDir,
    CustodyLink,
    Arc<ScriptedBatchLane>,
    Arc<NextMarkClock>,
) {
    let clock = NextMarkClock::observing_next_mark();
    let (root, custody, lane) =
        limits_fixture_with_clock(clock.source.clone(), clock.clone()).await;
    (root, custody, lane, clock)
}

async fn flush_or_deferral(
    events: &mut mpsc::UnboundedReceiver<CustodyTaskEvent>,
    clock: &NextMarkClock,
) -> Option<FlushReport> {
    tokio::select! {
        biased;
        report = flush_event(events, "the worker's next flush") => Some(report),
        () = clock.suffix_is_deferred() => None,
    }
}

pub(super) async fn queue_posts(custody: &mut CustodyLink, count: u16) -> Vec<RequestId> {
    let mut ids = Vec::new();
    for seed in 1..=count {
        let post = thread_post(seed);
        let OutboxQueuedWrite::Workspace(request) = post.payload else {
            panic!("fixture requires a thread post");
        };
        assert!(matches!(
            custody
                .submit(post.caller, SubmissionPayload::Workspace(request))
                .await,
            Submission::InOutbox(_)
        ));
        ids.push(post.request);
    }
    ids
}

fn answer_ids(report: FlushReport) -> Vec<RequestId> {
    assert!(report.ended.is_empty() && report.issues.is_empty());
    report
        .answered
        .into_iter()
        .map(|answer| answer.request)
        .collect()
}

fn frame_counts(lane: &ScriptedBatchLane) -> Vec<usize> {
    lane.batch_commands()
        .iter()
        .map(|frame| frame.requests.requests().len())
        .collect()
}

#[tokio::test]
async fn two_hundred_prompt_writes_drain_in_four_ordered_frames_at_one_mark() {
    let (_root, mut custody, lane, clock) = observed_mark_fixture().await;
    let queued = queue_posts(&mut custody, 200).await;
    let (_calls, stop, mut events, running) = start_worker(custody);
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
        .await;
    clock.source.advance(FIRST_MARK).unwrap();

    let mut answered = Vec::new();
    for count in [64, 64, 64, 8] {
        let report = flush_or_deferral(&mut events, &clock).await;
        assert!(
            report.is_some(),
            "the worker deferred the prompt suffix after {} of 200 writes",
            answered.len()
        );
        let frame = answer_ids(report.unwrap());
        assert_eq!(frame.len(), count);
        answered.extend(frame);
    }
    assert_eq!(answered, queued);
    assert_eq!(frame_counts(&lane), [64, 64, 64, 8]);
    assert_eq!(
        lane.command_instants(),
        [ClockInstant::ZERO.after(FIRST_MARK); 4]
    );
    assert_eq!(clock.monotonic_now(), ClockInstant::ZERO.after(FIRST_MARK));
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn the_sixty_fifth_write_is_answered_in_the_same_mark() {
    let (_root, mut custody, lane, clock) = limits_fixture().await;
    let queued = queue_posts(&mut custody, 65).await;
    clock.advance(FIRST_MARK).unwrap();

    assert_eq!(answer_ids(custody.flush_writes().await.unwrap()), queued);
    assert_eq!(frame_counts(&lane), [64, 1]);
}

#[tokio::test]
async fn the_exclusive_mark_close_leaves_the_slow_frames_suffix_for_the_next_mark() {
    let (_root, mut custody, lane, clock) = observed_mark_fixture().await;
    let queued = queue_posts(&mut custody, 65).await;
    let (prefix, suffix) = queued.split_at_checked(64).unwrap();
    let gate = lane.hold_next_batch();
    let (_calls, stop, mut events, running) = start_worker(custody);
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
        .await;
    clock.source.advance(FIRST_MARK).unwrap();

    gate.entered.notified().await;
    assert_eq!(frame_counts(&lane), [64], "only one frame may be live");
    clock.source.advance(Duration::from_secs(3)).unwrap();
    gate.release.notify_one();
    let first = answer_ids(flush_event(&mut events, "the first frame's answers").await);
    assert_eq!(first, prefix);
    assert!(
        flush_or_deferral(&mut events, &clock).await.is_none(),
        "no second frame may start at the closing edge"
    );
    assert_eq!(
        frame_counts(&lane),
        [64],
        "no second frame may start at the closing edge"
    );

    clock
        .source
        .advance(SECOND_MARK - FIRST_MARK - Duration::from_secs(3))
        .unwrap();
    let second = answer_ids(flush_event(&mut events, "the suffix on the next mark").await);
    assert_eq!(second, suffix);
    assert_eq!(frame_counts(&lane), [64, 1]);
    assert_eq!(
        lane.command_instants(),
        [
            ClockInstant::ZERO.after(FIRST_MARK),
            ClockInstant::ZERO.after(SECOND_MARK)
        ]
    );
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_lost_member_waits_for_the_next_mark_while_later_frames_drain() {
    let (_root, mut custody, lane, clock) = limits_fixture().await;
    let queued = queue_posts(&mut custody, 130).await;
    let (lost, suffix) = queued.split_first().unwrap();
    lane.decide_next_batch(Some(ScriptedBatch::Members(vec![
        ScriptedMemberFate::RecordedAnswerLost,
    ])));
    clock.advance(FIRST_MARK).unwrap();

    assert_eq!(answer_ids(custody.flush_writes().await.unwrap()), suffix);
    assert_eq!(frame_counts(&lane), [64, 64, 2]);
    clock.advance(SECOND_MARK - FIRST_MARK).unwrap();
    assert_eq!(
        answer_ids(custody.flush_writes().await.unwrap()),
        std::slice::from_ref(lost)
    );
    assert_eq!(frame_counts(&lane), [64, 64, 2, 1]);
}

#[tokio::test]
async fn a_transient_untouched_intake_refusal_leaves_the_write_eligible_on_its_mark() {
    check_untouched_intake(1).await;
}

pub(super) async fn check_untouched_intake(count: u16) {
    let (_root, mut custody, peer, clock) = observed_mark_fixture().await;
    let queued = queue_posts(&mut custody, count).await;
    peer.decide_next_batch(Some(ScriptedBatch::Untouched));
    let (_calls, stop, mut events, running) = start_worker(custody);
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
        .await;
    clock.source.advance(FIRST_MARK).unwrap();
    let retry_delay = Duration::from_millis(10);
    let retry = ClockInstant::ZERO.after(FIRST_MARK + retry_delay);
    tokio::select! {
        () = clock.source.not_due_check_now(retry) => {}
        () = clock.suffix_is_deferred() => panic!("an untouched write was deferred to a later mark"),
    }
    assert!(peer.batch_commands().is_empty());
    clock.source.advance(retry_delay).unwrap();
    let retried = flush_or_deferral(&mut events, &clock).await;
    assert!(
        retried.is_some(),
        "the proven-untouched write must remain eligible in the mark"
    );
    assert_eq!(answer_ids(retried.unwrap()), queued);
    assert_eq!(peer.command_instants(), [retry]);
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_transient_waiting_read_fault_keeps_the_old_suffix_owed_inside_the_mark() {
    check_waiting_read_fault(65).await;
}

pub(super) async fn check_waiting_read_fault(count: u16) {
    let clock = NextMarkClock::observing_next_mark();
    let mut campaign = FaultCampaign::start_with_clock(clock.source.clone(), clock.clone()).await;
    let queued = queue_posts(&mut campaign.custody, count).await;
    let (prefix, suffix) = queued.split_at_checked(64).unwrap();
    let gate = campaign.peer.hold_next_batch();
    let (_calls, stop, mut events, running) = start_worker(campaign.custody);
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
        .await;
    clock.source.advance(FIRST_MARK).unwrap();
    gate.entered.notified().await;
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(SECOND_MARK))
        .await;
    campaign
        .faults
        .arm(FaultSite::EarliestWaiting, FaultStage::BeforeCall);
    gate.release.notify_one();
    let report = flush_event(&mut events, "the first frame and its failed residual read").await;
    assert_eq!(report.issues.len(), 1);
    assert_eq!(
        report
            .answered
            .into_iter()
            .map(|answer| answer.request)
            .collect::<Vec<_>>(),
        prefix
    );

    let retry_delay = Duration::from_millis(10);
    let retry = ClockInstant::ZERO.after(FIRST_MARK + retry_delay);
    tokio::select! {
        () = clock.source.not_due_check_now(retry) => {}
        () = clock.suffix_is_deferred() => panic!("the read fault consumed the rest of the open mark"),
    }
    clock.source.advance(retry_delay).unwrap();
    let continued = flush_or_deferral(&mut events, &clock).await;
    assert!(
        continued.is_some(),
        "an unknown read must leave the original mark owed"
    );
    assert_eq!(answer_ids(continued.unwrap()), suffix);
    assert_eq!(
        campaign.peer.command_instants(),
        [ClockInstant::ZERO.after(FIRST_MARK), retry]
    );
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_claim_returning_after_the_mark_close_never_reaches_the_lane() {
    check_closed_claim(0).await;
}

pub(super) async fn check_closed_claim(after_close_ms: u64) {
    let clock = NextMarkClock::observing_next_mark();
    let mut campaign = FaultCampaign::start_with_clock(clock.source.clone(), clock.clone()).await;
    let queued = queue_posts(&mut campaign.custody, 1).await;
    let stored_claim = campaign.faults.hold_next_claim_commit();
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    let (calls, stop, mut events, running) = start_worker(campaign.custody);
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
        .await;
    clock.source.advance(FIRST_MARK).unwrap();
    stored_claim.committed.notified().await;
    let item = thread_post(1);
    assert_eq!(
        store
            .request_status(&item.caller, &item.request)
            .await
            .unwrap(),
        switchboard_machine_core_store::OutboxRequestStatus::Sent,
    );
    assert!(campaign.peer.batch_commands().is_empty());
    let elapsed_since_mark = Duration::from_secs(3) + Duration::from_millis(after_close_ms);
    clock.source.advance(elapsed_since_mark).unwrap();
    stored_claim.release.notify_one();
    assert!(
        flush_or_deferral(&mut events, &clock).await.is_none(),
        "custody must return a claim completed at the closed fence before submitting it"
    );
    assert!(campaign.peer.batch_commands().is_empty());
    assert!(matches!(
        calls
            .request_status(item.caller.clone(), scoped_query(&item))
            .await,
        CustodyRequestStatus::InOutbox { .. }
    ));

    clock
        .source
        .advance(SECOND_MARK - FIRST_MARK - elapsed_since_mark)
        .unwrap();
    let report = flush_event(&mut events, "the returned claim on its next mark").await;
    assert_eq!(answer_ids(report), queued);
    assert_eq!(
        campaign.peer.command_instants(),
        [ClockInstant::ZERO.after(SECOND_MARK)]
    );
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_write_admitted_during_a_rebased_drain_waits_for_its_own_receipt_mark() {
    check_rebased_admission(65).await;
}

pub(super) async fn check_rebased_admission(old_count: u16) {
    let (_root, mut custody, lane, clock) = observed_mark_fixture().await;
    let queued_before_welcome = queue_posts(&mut custody, old_count).await;
    let (prefix, old_suffix) = queued_before_welcome.split_at_checked(64).unwrap();
    let fresh = thread_post(old_count + 1);
    let Caller::Session(owner) = &fresh.caller else {
        panic!("fixture requires a session caller");
    };
    let mut backwards = new_admission(owner.machine_id.clone(), clock.source.clone());
    backwards.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(MARK_MS - 140_000).unwrap();
    lane.admitted.send_replace(Some(backwards));
    let gate = lane.hold_next_batch();
    let (calls, stop, mut events, running) = start_worker(custody);
    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
        .await;
    clock.source.advance(FIRST_MARK).unwrap();
    gate.entered.notified().await;

    let OutboxQueuedWrite::Workspace(post) = fresh.payload else {
        panic!("fixture requires a thread post");
    };
    let submission = calls
        .submit(fresh.caller, SubmissionPayload::Workspace(post))
        .await;
    assert!(matches!(submission, Submission::InOutbox(receipt)
        if receipt.id == fresh.request && receipt.flush_at == SyncTime::from_unix_millis(MARK_MS - 60_000)));

    gate.release.notify_one();
    assert_eq!(
        answer_ids(flush_event(&mut events, "the first rebased frame").await),
        prefix
    );
    let continued = flush_or_deferral(&mut events, &clock).await;
    assert!(
        continued.is_some(),
        "the old suffix is still owed on the open rebased mark"
    );
    assert_eq!(
        answer_ids(continued.unwrap()),
        old_suffix,
        "a new receipt cannot be pulled earlier by the previous Welcome's frontier"
    );
    assert_eq!(frame_counts(&lane), [64, old_suffix.len()]);

    clock
        .source
        .not_due_check_now(ClockInstant::ZERO.after(SECOND_MARK))
        .await;
    clock.source.advance(SECOND_MARK - FIRST_MARK).unwrap();
    assert_eq!(
        answer_ids(flush_event(&mut events, "the new receipt's own mark").await),
        [fresh.request]
    );
    assert_eq!(frame_counts(&lane), [64, old_suffix.len(), 1]);
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_near_cutoff_orphan_rejoins_the_waiting_writes_before_three_frames_drain() {
    let (root, mut custody, lane, clock) = limits_fixture().await;
    let orphan = thread_post(130);
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let created_at = MARK_MS - 890_000;
    store.insert(&orphan, created_at).await.unwrap();
    let claim = store
        .claim_next_batch(MARK_MS, 1, created_at, created_at, &Default::default())
        .await
        .unwrap();
    assert!(matches!(claim.healthy.writes.as_slice(), [write] if write.request == orphan.request));
    let mut queued = queue_posts(&mut custody, 129).await;
    queued.insert(0, orphan.request.clone());
    let query = scoped_query(&orphan);
    assert!(custody.recover_outbox().await.unwrap().issues.is_empty());
    assert!(
        matches!(
            custody.request_status(orphan.caller, query.clone()).await,
            CustodyRequestStatus::InOutbox { .. }
        ),
        "the scoped miss returns the orphan with thirty seconds left"
    );
    assert!(
        matches!(lane.take_calls().as_slice(), [ScriptedCommand::RequestStatus(command)]
        if command.query == query)
    );

    clock.advance(FIRST_MARK).unwrap();
    assert_eq!(answer_ids(custody.flush_writes().await.unwrap()), queued);
    assert_eq!(frame_counts(&lane), [64, 64, 2]);
}

#[tokio::test]
async fn a_driver_held_write_ends_once_at_cutoff_and_a_later_reply_updates_only_status() {
    for untouched in [false, true] {
        let (root, mut custody, lane, clock) = limits_fixture().await;
        let aging = thread_post(2);
        let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
        store.insert(&aging, MARK_MS - 899_000).await.unwrap();
        let suffix = queue_posts(&mut custody, 1).await;
        if untouched {
            lane.decide_next_batch(Some(ScriptedBatch::Untouched));
        }
        let gate = lane.hold_next_batch();
        let (calls, stop, mut events, running) = start_worker(custody);
        clock
            .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK))
            .await;
        clock.advance(FIRST_MARK).unwrap();

        gate.entered.notified().await;
        clock
            .not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK + Duration::from_secs(1)))
            .await;
        clock.advance(Duration::from_secs(1)).unwrap();
        let ending = flush_event(
            &mut events,
            "an unconfirmed ending while the driver still owns the ticket",
        )
        .await;
        assert!(
            matches!(ending.ended.as_slice(), [ended] if ended.request_id == aging.request
                && ended.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
        );
        assert!(ending.answered.is_empty() && ending.issues.is_empty());
        let query = scoped_query(&aging);
        let unconfirmed = CustodyRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes,
        };
        assert_eq!(
            calls
                .request_status(aging.caller.clone(), query.clone())
                .await,
            unconfirmed
        );

        gate.release.notify_one();
        if untouched {
            let retry_delay = Duration::from_millis(10);
            let retry = clock.monotonic_now().after(retry_delay);
            clock.not_due_check_now(retry).await;
            clock.advance(retry_delay).unwrap();
        }
        assert_eq!(
            answer_ids(flush_event(&mut events, "only the later frame's typed answer").await),
            suffix
        );
        let final_status = if untouched {
            unconfirmed
        } else {
            CustodyRequestStatus::Answered(lane.decisions().get(&aging.request).unwrap().clone())
        };
        assert_eq!(
            calls.request_status(aging.caller, query).await,
            final_status
        );
        assert!(
            events.try_recv().is_err(),
            "the ending is reported only once"
        );
        assert_eq!(
            frame_counts(&lane),
            if untouched { vec![1] } else { vec![1, 1] }
        );
        stop.send_replace(true);
        running.await.unwrap();
    }
}
