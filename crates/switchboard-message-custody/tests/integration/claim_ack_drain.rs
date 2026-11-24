//! Recover a claim whose acknowledgement was lost while its original mark is open.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A failed fixture operation invalidates the proof."
)]

use router_clock::ClockInstant;
use std::time::Duration;
use switchboard_machine_core_store::{
    OutboxQueuedWrite, OutboxRequestStatus, OutboxStorage, PendingWriteState,
};
use switchboard_message_custody::{
    custody_submission::{Submission, SubmissionPayload},
    custody_task::CustodyTaskEvent,
};

use super::{
    batch_replay::{ScriptedBatch, ScriptedMemberFate},
    faulted_overlay::{FaultCampaign, MARK_MS, START_MS},
    faulty_outbox::{FaultSite, FaultStage},
    outbox_timing::{manual_clock, open_outbox, thread_post},
    worker_turns::{ObservedWorker, WorkerClock},
};

#[tokio::test]
async fn an_empty_retry_does_not_consume_the_mark_before_a_lost_claim_ack_is_recovered() {
    check_empty_retry_before_claim_recovery(2_500).await;
}

// Each case owns a fresh SQLite store and real custody worker. Only the
// wall-clock throughput check is disabled, so host load cannot fail it.
#[hegel::test(test_cases = 12, suppress_health_check = [hegel::HealthCheck::TooSlow])]
#[hegel::explicit_test_case(first_cutoff_gap_ms = 1_100_i64, fail_first_return = false)]
#[hegel::explicit_test_case(first_cutoff_gap_ms = 2_999_i64, fail_first_return = true)]
fn generated_lost_claim_ack_parks_and_recovers_inside_the_original_mark(case: hegel::TestCase) {
    let first_cutoff_gap_ms = case.draw(
        hegel::generators::integers::<i64>()
            .min_value(1_100)
            .max_value(2_999),
    );
    let fail_first_return = case.draw(hegel::generators::booleans());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_lost_claim_ack_history(
            first_cutoff_gap_ms,
            fail_first_return,
        ));
}

pub(super) async fn check_empty_retry_before_claim_recovery(first_cutoff_gap_ms: i64) {
    run_lost_claim_ack_history(first_cutoff_gap_ms, false).await;
}

async fn run_lost_claim_ack_history(first_cutoff_gap_ms: i64, fail_first_return: bool) {
    let clock = WorkerClock::observing(manual_clock(START_MS));
    let mut fixture = FaultCampaign::start_with_clock(clock.manual.clone(), clock.clone()).await;
    let mut store = open_outbox(&fixture.root.path().join("machine_local.sqlite")).await;
    let older = thread_post(21);
    let suffix = thread_post(22);
    store
        .insert(&older, MARK_MS - 900_000 + first_cutoff_gap_ms)
        .await
        .unwrap();
    let OutboxQueuedWrite::Workspace(post) = suffix.payload else {
        panic!("the fixture creates a thread post");
    };
    assert!(
        matches!(fixture.custody.submit(suffix.caller.clone(), SubmissionPayload::Workspace(post)).await,
        Submission::InOutbox(receipt) if receipt.id == suffix.request)
    );

    fixture
        .peer
        .decide_next_batch(Some(ScriptedBatch::Members(vec![
            ScriptedMemberFate::LostBeforeCentral,
        ])));
    let first_frame = fixture.peer.hold_next_batch();
    let mut worker = ObservedWorker::start(fixture.custody, clock.clone());
    let mark = Duration::from_secs(20);
    worker
        .wait_for_timer(0, ClockInstant::ZERO.after(mark))
        .await;
    clock.manual.advance(mark).unwrap();
    first_frame.entered.notified().await;
    fixture
        .faults
        .arm(FaultSite::ClaimBatch, FaultStage::AfterSuccess);
    first_frame.release.notify_one();
    assert!(matches!(
        worker.events.recv().await,
        Some(CustodyTaskEvent::Failed(_))
    ));
    assert_eq!(
        fixture.faults.fired(),
        [(FaultSite::ClaimBatch, FaultStage::AfterSuccess)]
    );
    assert_eq!(
        store
            .request_status(&suffix.caller, &suffix.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Sent
    );

    // Visit the short write retry before the orphan scan's one-second wake.
    // The older row is still waiting but ineligible, and the suffix is Sent.
    let empty_claim = fixture.faults.hold_next_claim_commit();
    let earlier_delay = Duration::from_millis(10);
    clock
        .manual
        .not_due_check_now(ClockInstant::ZERO.after(mark + earlier_delay))
        .await;
    clock.manual.advance(earlier_delay).unwrap();
    empty_claim.committed.notified().await;
    assert_eq!(fixture.peer.batch_commands().len(), 1);
    assert!(
        store
            .pending_for(&older.caller)
            .await
            .unwrap()
            .iter()
            .any(|row| row.request_id == older.request
                && matches!(row.state, PendingWriteState::SentAnswerLost { .. }))
    );
    assert_eq!(
        store
            .request_status(&suffix.caller, &suffix.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Sent
    );
    let before_release = clock.checkpoint();
    empty_claim.release.notify_one();
    let orphan_wake = mark + Duration::from_secs(1);
    worker
        .wait_for_timer(before_release, ClockInstant::ZERO.after(orphan_wake))
        .await;

    if fail_first_return {
        fixture
            .faults
            .arm(FaultSite::ReturnNotTransmitted, FaultStage::BeforeCall);
    }
    let scanned = worker
        .advance(Duration::from_secs(1) - earlier_delay)
        .await
        .unwrap();
    let (recovered, recovery_wake) = if fail_first_return {
        assert_eq!(scanned.issues.len(), 1);
        assert!(scanned.answered.is_empty());
        assert_eq!(fixture.peer.batch_commands().len(), 1);
        assert_eq!(
            store
                .request_status(&suffix.caller, &suffix.request)
                .await
                .unwrap(),
            OutboxRequestStatus::Sent
        );
        assert_eq!(
            fixture.faults.fired(),
            [
                (FaultSite::ClaimBatch, FaultStage::AfterSuccess),
                (FaultSite::ReturnNotTransmitted, FaultStage::BeforeCall),
            ]
        );
        let delay = Duration::from_secs(1);
        (worker.advance(delay).await.unwrap(), orphan_wake + delay)
    } else {
        assert!(scanned.issues.is_empty());
        (scanned, orphan_wake)
    };
    assert!(
        matches!(recovered.answered.as_slice(), [answer] if answer.request == suffix.request),
        "recovering a local claim must leave its original ID owed inside the open mark"
    );
    let frames = fixture.peer.batch_commands();
    let identities: Vec<_> = frames
        .iter()
        .flat_map(|frame| {
            frame
                .requests
                .requests()
                .iter()
                .map(|request| request.payload().write_request_id().unwrap())
        })
        .collect();
    assert_eq!(identities, [&older.request, &suffix.request]);
    assert_eq!(
        fixture.peer.command_instants(),
        [
            ClockInstant::ZERO.after(mark),
            ClockInstant::ZERO.after(recovery_wake)
        ]
    );
    assert!(fixture.peer.take_calls().is_empty());
    worker.stop().await;
}
