//! The custody worker preserves other cutoff obligations while one active cutoff fails.

#![expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A fixture that cannot complete makes the cutoff assertions invalid."
)]

use central_link_protocol::ReplyPayload;
use router_clock::{ClockInstant, RuntimeClock};
use std::time::Duration;
use switchboard_machine_core_store::{
    OutboxAccepted, OutboxCancellation, OutboxInsert, OutboxQueuedWrite, OutboxRequestKey,
    OutboxRequestStatus, OutboxStorage, OutgoingOutcome,
};
use switchboard_message_custody::{
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::CustodyTaskEvent,
};
use workspace_model::{ItemRef, WorkspaceAnswer, WorkspaceReply, WorkspaceResult};

use super::{
    batch_limits::limits_fixture,
    batch_replay::{ScriptedBatch, ScriptedMemberFate},
    faulted_overlay::{FaultCampaign, MARK_MS},
    faulty_outbox::{FaultSite, FaultStage},
    outbox_timing::{open_outbox, thread_post},
    worker_orphans::{flush_event, scoped_query, start_worker},
};

#[tokio::test]
async fn a_pre_cutoff_member_hidden_in_a_held_batch_agrees_in_both_reads_after_settlement() {
    check_hidden_member_cutoff(1).await;
}

pub(super) async fn check_hidden_member_cutoff(before_cutoff_ms: u64) {
    let (root, mut custody, peer, clock) = limits_fixture().await;
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let first = thread_post(11);
    let unanswered = thread_post(12);
    for write in [&first, &unanswered] {
        assert_eq!(
            store.insert(write, MARK_MS - 899_000).await.unwrap(),
            OutboxInsert::Stored
        );
    }
    let following = thread_post(13);
    let OutboxQueuedWrite::Workspace(request) = following.payload else {
        panic!("fixture requires a thread post");
    };
    assert!(matches!(
        custody
            .submit(following.caller, SubmissionPayload::Workspace(request))
            .await,
        Submission::InOutbox(_)
    ));
    peer.decide_next_batch(Some(ScriptedBatch::Members(vec![
        ScriptedMemberFate::Answered,
        ScriptedMemberFate::LostBeforeCentral,
    ])));
    let gate = peer.hold_next_batch();
    let (calls, stop, mut events, running) = start_worker(custody);
    let cutoff_ms = 21_000;
    let accepted = Duration::from_millis(cutoff_ms - before_cutoff_ms);
    clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    clock.advance(accepted).unwrap();
    gate.entered.notified().await;
    assert_eq!(
        peer.command_instants(),
        [ClockInstant::ZERO.after(accepted)]
    );
    let checked = peer.decisions().get(&first.request).unwrap().clone();
    assert!(!peer.decisions().contains_key(&unanswered.request));

    let cutoff = ClockInstant::ZERO.after(Duration::from_millis(cutoff_ms));
    clock.not_due_check_now(cutoff).await;
    clock
        .advance(Duration::from_millis(before_cutoff_ms))
        .unwrap();
    let at_cutoff = flush_event(&mut events, "both still-held members at their cutoff").await;
    assert_eq!(clock.monotonic_now(), cutoff);
    assert!(matches!(at_cutoff.ended.as_slice(), [left, right]
        if left.request_id == first.request && right.request_id == unanswered.request
            && left.reason == OutboxCancellation::NotConfirmedWithin15Minutes
            && right.reason == OutboxCancellation::NotConfirmedWithin15Minutes));
    assert!(at_cutoff.answered.is_empty());
    let unconfirmed = CustodyRequestStatus::Ended {
        reason: OutboxCancellation::NotConfirmedWithin15Minutes,
    };
    for write in [&first, &unanswered] {
        assert_eq!(
            calls
                .request_status(write.caller.clone(), scoped_query(write))
                .await,
            unconfirmed
        );
    }
    assert!(
        store
            .outgoing_for(&first.caller, 10)
            .await
            .unwrap()
            .is_empty()
    );

    clock.advance(Duration::from_millis(1)).unwrap();
    gate.release.notify_one();
    let later = flush_event(
        &mut events,
        "the next frame without duplicate cutoff notices",
    )
    .await;
    assert!(later.ended.is_empty());
    assert!(matches!(later.answered.as_slice(), [answer] if answer.request == following.request));
    assert_eq!(
        calls
            .request_status(first.caller.clone(), scoped_query(&first))
            .await,
        CustodyRequestStatus::Answered(checked.clone()),
        "checked A must replace the visible unconfirmed result after the batch returns",
    );
    assert_eq!(
        calls
            .request_status(unanswered.caller.clone(), scoped_query(&unanswered))
            .await,
        unconfirmed
    );

    let history = store.outgoing_for(&first.caller, 10).await.unwrap();
    let result = history
        .iter()
        .find(|write| write.request_id == first.request)
        .unwrap();
    assert_eq!(result.answered_at_ms, None);
    assert!(matches!((&result.outcome, checked), (
        OutgoingOutcome::Answered(OutboxAccepted::ThreadPost { post_id, position, actors }),
        ReplyPayload::Workspace(WorkspaceReply {
            result: WorkspaceResult::Done(WorkspaceAnswer::Created { item: ItemRef::Post(peer_id), position: peer_position }),
            actors: peer_actors,
        })
    ) if *post_id == peer_id && *position == peer_position && *actors == peer_actors));
    assert!(
        history
            .iter()
            .all(|write| write.request_id != unanswered.request)
    );
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn an_active_cutoff_fault_leaves_other_active_and_waiting_cutoffs_armed() {
    check_active_cutoff_fault(1_000).await;
}

pub(super) async fn check_active_cutoff_fault(gap_ms: u32) {
    let campaign = FaultCampaign::start().await;
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    let first = thread_post(41);
    let later_active = thread_post(42);
    let mut waiting_now = thread_post(43);
    let mut waiting_later = thread_post(44);
    waiting_now.flush_at_ms += 60_000;
    waiting_later.flush_at_ms += 60_000;
    for (write, cutoff_after_mark) in [
        (&first, 5_000),
        (&later_active, 5_000 + i64::from(gap_ms)),
        (&waiting_now, 5_000),
        (&waiting_later, 5_000 + i64::from(gap_ms)),
    ] {
        assert_eq!(
            store
                .insert(write, MARK_MS + cutoff_after_mark - 900_000)
                .await
                .unwrap(),
            OutboxInsert::Stored,
        );
    }

    let gate = campaign.peer.hold_next_batch();
    let (calls, stop, mut events, running) = start_worker(campaign.custody);
    let clock = campaign.clock;
    clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(20)))
        .await;
    clock.advance(Duration::from_secs(20)).unwrap();
    gate.entered.notified().await;
    let first_due = ClockInstant::ZERO.after(Duration::from_secs(25));
    clock.not_due_check_now(first_due).await;
    campaign
        .faults
        .arm(FaultSite::SentWithoutAnswer, FaultStage::BeforeCall);

    clock.advance(Duration::from_secs(5)).unwrap();
    assert_eq!(clock.monotonic_now(), first_due);
    let first_event = events.recv().await.unwrap();
    assert!(
        matches!(&first_event, CustodyTaskEvent::Flush(report)
            if matches!(report.ended.as_slice(), [ended] if ended.request_id == waiting_now.request
                && ended.reason == OutboxCancellation::NotSentWithin15Minutes)),
        "the independently due waiting write must end even when the active scan fails: {first_event:?}"
    );
    assert!(matches!(
        events.recv().await,
        Some(CustodyTaskEvent::Failed(_))
    ));
    assert_eq!(
        calls
            .request_status(first.caller.clone(), scoped_query(&first))
            .await,
        CustodyRequestStatus::Sent
    );

    let retry_at = first_due.after(Duration::from_millis(1));
    clock.not_due_check_now(retry_at).await;
    clock.advance(Duration::from_millis(1)).unwrap();
    let retried = flush_event(&mut events, "the independent retry of the failed cutoff").await;
    assert!(
        matches!(retried.ended.as_slice(), [ended] if ended.request_id == first.request
        && ended.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
    );

    let next_due = first_due.after(Duration::from_millis(u64::from(gap_ms)));
    clock.not_due_check_now(next_due).await;
    clock
        .advance(Duration::from_millis(u64::from(gap_ms - 1)))
        .unwrap();
    let later = flush_event(
        &mut events,
        "the next independent active and waiting cutoffs",
    )
    .await;
    assert_eq!(clock.monotonic_now(), next_due);
    assert!(matches!(later.ended.as_slice(), [active, waiting]
        if active.request_id == later_active.request && active.reason == OutboxCancellation::NotConfirmedWithin15Minutes
            && waiting.request_id == waiting_later.request && waiting.reason == OutboxCancellation::NotSentWithin15Minutes));
    assert!(later.answered.is_empty());
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn the_public_drain_ends_a_later_waiting_write_while_the_frame_is_held() {
    check_public_waiting_cutoff(1_000).await;
}

pub(super) async fn check_public_waiting_cutoff(gap_ms: u32) {
    let (root, mut custody, peer, clock) = limits_fixture().await;
    let mut store = open_outbox(&root.path().join("machine_local.sqlite")).await;
    let active = thread_post(31);
    let mut waiting = thread_post(32);
    waiting.flush_at_ms += 60_000;
    for (write, cutoff_after_mark) in [(&active, 1_000), (&waiting, 1_000 + i64::from(gap_ms))] {
        assert_eq!(
            store
                .insert(write, MARK_MS + cutoff_after_mark - 900_000)
                .await
                .unwrap(),
            OutboxInsert::Stored,
        );
    }
    let active_key = OutboxRequestKey {
        request_id: active.request.clone(),
        scope: active.payload.request_scope(),
    };
    let waiting_key = OutboxRequestKey {
        request_id: waiting.request.clone(),
        scope: waiting.payload.request_scope(),
    };
    let gate = peer.hold_next_batch();
    clock.advance(Duration::from_secs(20)).unwrap();
    let flush = tokio::spawn(async move { custody.flush_writes().await });
    gate.entered.notified().await;
    clock
        .not_due_check_now(ClockInstant::ZERO.after(Duration::from_secs(21)))
        .await;

    clock.advance(Duration::from_secs(1)).unwrap();
    let active_ending = OutboxRequestStatus::Ended {
        reason: OutboxCancellation::NotConfirmedWithin15Minutes,
    };
    while store
        .scoped_request_status(&active.caller, &active_key)
        .await
        .unwrap()
        != active_ending
    {}
    // The test uses one current-thread executor. This notification comes from
    // a poll of the pending reply. The same poll examines the deadline branch
    // before giving control back to the observer.
    gate.observe_pending_poll().await;
    let gap = Duration::from_millis(u64::from(gap_ms));
    let waiting_due = ClockInstant::ZERO.after(Duration::from_secs(21) + gap);
    assert!(
        clock.has_not_due_check_now(waiting_due),
        "the public drain must arm the later waiting cutoff while the reply stays held"
    );

    clock.advance(gap).unwrap();
    let waiting_ending = OutboxRequestStatus::Ended {
        reason: OutboxCancellation::NotSentWithin15Minutes,
    };
    while store
        .scoped_request_status(&waiting.caller, &waiting_key)
        .await
        .unwrap()
        != waiting_ending
    {}
    assert_eq!(clock.monotonic_now(), waiting_due);
    gate.release.notify_one();
    let report = flush.await.unwrap().unwrap();
    assert!(matches!(report.ended.as_slice(), [first, second]
        if first.request_id == active.request && second.request_id == waiting.request));
    assert_eq!(peer.batch_commands().len(), 1);
}
