//! The worker drains ordered frames while the shared minute window and the original deadlines permit.

#![expect(
    clippy::panic,
    reason = "A malformed fixture or missing result fails the behavior it was set up to prove."
)]

use router_clock::{ClockInstant, RuntimeClock};
use std::time::Duration;
use switchboard_machine_core_store::{OutboxCancellation, OutboxQueuedWrite, OutboxStorage};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    outbox_dispatch::FlushReport,
};
use workspace_ids::RequestId;

use super::{
    batch_limits::limits_fixture,
    batch_replay::{ScriptedBatch, ScriptedBatchLane, ScriptedCommand, ScriptedMemberFate},
    outbox_timing::{open_outbox, thread_post},
    worker_orphans::{flush_event, progress, scoped_query, start_worker},
};

const FIRST_MARK: Duration = Duration::from_secs(20);
const SECOND_MARK: Duration = Duration::from_secs(80);
const MARK_MS: i64 = 1_763_380_800_000;

async fn queue_posts(custody: &mut CustodyLink, count: u16) -> Vec<RequestId> {
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
    let (_root, mut custody, lane, clock) = limits_fixture().await;
    let queued = queue_posts(&mut custody, 200).await;
    let (_calls, stop, mut events, running) = start_worker(custody);
    progress(
        clock.not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK)),
        "the first mark wake",
    )
    .await;
    clock.advance(FIRST_MARK).unwrap();

    let mut answered = Vec::new();
    for count in [64, 64, 64, 8] {
        let frame =
            answer_ids(flush_event(&mut events, "each prompt frame on the original mark").await);
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
    let (_root, mut custody, lane, clock) = limits_fixture().await;
    let queued = queue_posts(&mut custody, 65).await;
    let (prefix, suffix) = queued.split_at_checked(64).unwrap();
    let gate = lane.hold_next_batch();
    let (_calls, stop, mut events, running) = start_worker(custody);
    progress(
        clock.not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK)),
        "the first mark wake",
    )
    .await;
    clock.advance(FIRST_MARK).unwrap();

    progress(gate.entered.notified(), "the held first frame").await;
    assert_eq!(frame_counts(&lane), [64], "only one frame may be live");
    clock.advance(Duration::from_secs(3)).unwrap();
    let closed_at = clock.monotonic_now();
    let waits_after_close = clock.wait_checkpoint();
    gate.release.notify_one();
    let first = answer_ids(flush_event(&mut events, "the first frame's answers").await);
    assert_eq!(first, prefix);
    let next_wake = ClockInstant::ZERO.after(SECOND_MARK);
    let deferred = progress(
        async {
            loop {
                if lane.command_instants().len() > 1 {
                    break false;
                }
                let waits = clock
                    .wait_registrations_since(waits_after_close)
                    .into_iter()
                    .filter(|wait| {
                        wait.deadline == next_wake
                            && wait.first_polled_at == Some(closed_at)
                            && !wait.dropped
                    })
                    .count();
                // The receipt timer alone does not show that the suffix was deferred.
                if waits == 2 {
                    break true;
                }
                tokio::task::yield_now().await;
            }
        },
        "the untouched suffix's next mark",
    )
    .await;
    assert!(deferred, "no second frame may start at the closing edge");
    assert_eq!(
        frame_counts(&lane),
        [64],
        "no second frame may start at the closing edge"
    );

    clock
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
        progress(
            clock.not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK)),
            "the first mark wake",
        )
        .await;
        clock.advance(FIRST_MARK).unwrap();

        progress(gate.entered.notified(), "the active driver ticket").await;
        progress(
            clock.not_due_check_now(ClockInstant::ZERO.after(FIRST_MARK + Duration::from_secs(1))),
            "the held write's original deadline",
        )
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
