//! A fresh receipt remains a lower bound through successive backward welcomes.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "The fixture and its observed frames must be complete."
)]

use router_clock::{ClockInstant, RuntimeClock};
use runtime_primitives::SyncTime;
use std::time::Duration;
use switchboard_machine_core_store::OutboxQueuedWrite;
use switchboard_message_custody::custody_submission::{Submission, SubmissionPayload};
use workspace_model::{Caller, Timestamp};

use super::{
    batch_limits::observed_limits_fixture, batch_replay::new_admission, mark_drain::queue_posts,
    outbox_timing::thread_post, worker_orphans::flush_event, worker_turns::ObservedWorker,
};

const MARK_MS: i64 = 1_763_380_800_000;
const MINUTE_MS: i64 = 60_000;

#[tokio::test]
async fn a_second_backward_welcome_preserves_a_fresh_waiting_receipt() {
    check_repeated_welcome(false, 1).await;
}

#[tokio::test]
async fn a_claim_at_the_named_mark_preserves_the_receipt_of_its_waiting_suffix() {
    check_repeated_welcome(true, 2).await;
}

pub(super) async fn check_repeated_welcome(visit_named_mark: bool, older_suffix: u16) {
    let (root, mut custody, lane, observed) = observed_limits_fixture().await;
    let clock = observed.manual.clone();
    let covered_count = if visit_named_mark { 128 } else { 64 };
    let old_ids = queue_posts(&mut custody, covered_count + older_suffix).await;
    let fresh = thread_post(covered_count + older_suffix + 1);
    let Caller::Session(owner) = &fresh.caller else {
        panic!("the fixture has a session caller");
    };
    let mut first_welcome = new_admission(owner.machine_id.clone(), clock.clone());
    first_welcome.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(MARK_MS - 2 * MINUTE_MS - 20_000).unwrap();
    lane.admitted.send_replace(Some(first_welcome));
    let first_gate = lane.hold_next_batch();
    let mut worker = ObservedWorker::start(custody, observed.clone());
    worker
        .wait_for_timer(0, ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    clock.advance(Duration::from_secs(20)).unwrap();
    first_gate.entered.notified().await;

    let OutboxQueuedWrite::Workspace(post) = fresh.payload else {
        panic!("the fixture creates a thread post");
    };
    let first_mark = MARK_MS - 2 * MINUTE_MS;
    let named_mark = first_mark + MINUTE_MS;
    assert!(matches!(
        worker.calls.submit(fresh.caller.clone(), SubmissionPayload::Workspace(post)).await,
        Submission::InOutbox(receipt)
            if receipt.id == fresh.request && receipt.flush_at == SyncTime::from_unix_millis(named_mark)
    ));

    let occupying_gate = if visit_named_mark {
        let named_gate = lane.hold_next_batch();
        clock.advance(Duration::from_secs(3)).unwrap();
        first_gate.release.notify_one();
        let flushed = flush_event(&mut worker.events, "the first frame to settle").await;
        assert_eq!(flushed.answered.len(), 64);
        clock.advance(Duration::from_secs(57)).unwrap();
        named_gate.entered.notified().await;
        named_gate
    } else {
        first_gate
    };

    // Rewind to a distinct, unused mark while the older frame still owns
    // its slot. The fresh receipt has not yet left in either history.
    let earlier_mark = first_mark - MINUTE_MS;
    let mut second_welcome = new_admission(owner.machine_id.clone(), clock.clone());
    second_welcome.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(earlier_mark - 1_000).unwrap();
    let before_welcome = observed.checkpoint();
    lane.admitted.send_replace(Some(second_welcome));
    worker
        .wait_for_timer(
            before_welcome,
            clock.monotonic_now().after(Duration::from_secs(1)),
        )
        .await;
    occupying_gate.release.notify_one();
    let flushed = flush_event(&mut worker.events, "the held older frame to settle").await;
    assert_eq!(flushed.answered.len(), 64);

    let earlier = worker.advance(Duration::from_secs(1)).await.unwrap();
    let (_, remaining) = old_ids
        .split_at_checked(usize::from(covered_count))
        .unwrap();
    assert_eq!(
        earlier
            .answered
            .into_iter()
            .map(|answer| answer.request)
            .collect::<Vec<_>>(),
        remaining,
        "a second backward welcome cannot send a waiting write earlier than its own receipt"
    );
    let identified_frames: Vec<Vec<_>> = lane
        .batch_commands()
        .iter()
        .map(|frame| {
            frame
                .requests
                .requests()
                .iter()
                .map(|request| request.payload().write_request_id().unwrap().clone())
                .collect()
        })
        .collect();
    assert_eq!(
        identified_frames.concat(),
        old_ids,
        "only the original older IDs have left"
    );

    // If we visited the named mark before rewinding, it already belongs to
    // the older frame. The first reusable window is the following minute.
    let first_available = if visit_named_mark {
        named_mark + MINUTE_MS
    } else {
        named_mark
    };
    let sent = worker
        .advance(Duration::from_millis(
            u64::try_from(first_available - earlier_mark).unwrap(),
        ))
        .await
        .unwrap();
    assert!(matches!(sent.answered.as_slice(), [reply] if reply.request == fresh.request));
    let frames = lane.batch_commands();
    let last_frame = frames.last().unwrap();
    assert!(matches!(last_frame.requests.requests(), [request]
        if request.payload().write_request_id() == Some(&fresh.request)));
    worker.stop().await;
    drop(root);
}
