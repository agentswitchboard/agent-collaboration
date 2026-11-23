use central_link_protocol::{ReplyPayload, RequestScope, RequestStatusAnswer, RequestStatusQuery};
use router_clock::{ClockInstant, RuntimeClock};
use std::{sync::Arc, time::Duration};
use switchboard_machine_core_store::{
    NewOutboxItem, OutboxCancellation, OutboxQueuedWrite, OutboxStorage,
};
use switchboard_message_custody::{
    custody_link::CustodyLink,
    custody_status::CustodyRequestStatus,
    custody_submission::{Submission, SubmissionPayload},
    custody_task::{CustodyTaskEvent, CustodyTaskHandle, custody_task},
    outbox_dispatch::FlushReport,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use workspace_model::RefusalReason;

use super::{
    batch_replay::{ScriptedBatch, ScriptedCommand, ScriptedMemberFate, post_created},
    custody_task::ObserveTask,
    faulted_overlay::{FaultCampaign, START_MS},
    faulty_outbox::{FaultSite, FaultStage},
    mark_waits::NextMarkClock,
    outbox_timing::{manual_clock, open_outbox, thread_post},
};

pub(super) fn start_worker(
    custody: CustodyLink,
) -> (
    CustodyTaskHandle,
    watch::Sender<bool>,
    mpsc::UnboundedReceiver<CustodyTaskEvent>,
    JoinHandle<()>,
) {
    let (stop, stopped) = watch::channel(false);
    let (sender, events) = mpsc::unbounded_channel();
    let (calls, worker) = custody_task(custody, stopped, Arc::new(ObserveTask(sender)));
    (calls, stop, events, tokio::spawn(worker))
}

#[tokio::test]
async fn a_lost_return_ack_excludes_that_request_while_an_unrelated_batch_is_live() {
    for (site, first_disposition) in [
        (FaultSite::ReturnNotTransmitted, ScriptedBatch::Untouched),
        (
            FaultSite::ReturnUnanswered,
            ScriptedBatch::Members(vec![ScriptedMemberFate::RecordedAnswerLost]),
        ),
    ] {
        let campaign = FaultCampaign::start().await;
        let first = thread_post(607);
        let second = thread_post(608);
        let clock = campaign.clock.clone();
        let peer = campaign.peer.clone();
        let faults = campaign.faults.clone();
        faults.arm(site, FaultStage::AfterSuccess);
        faults.fail_scoped_reads(true);
        peer.decide_next_batch(Some(first_disposition));
        let (calls, stop, mut events, running) = start_worker(campaign.custody);
        let OutboxQueuedWrite::Workspace(first_payload) = &first.payload else {
            panic!("expected a post");
        };
        assert!(matches!(
            calls
                .submit(
                    first.caller.clone(),
                    SubmissionPayload::Workspace(first_payload.clone())
                )
                .await,
            Submission::InOutbox(_)
        ));
        let first_mark = ClockInstant::ZERO.after(Duration::from_secs(20));
        clock.not_due_check_now(first_mark).await;
        clock.advance(Duration::from_secs(20)).unwrap();
        let report = flush_event(&mut events, "the ambiguous committed return").await;
        assert_eq!(report.issues.len(), 1);
        assert!(report.answered.is_empty());
        assert!(faults.fired().contains(&(site, FaultStage::AfterSuccess)));
        let OutboxQueuedWrite::Workspace(second_payload) = &second.payload else {
            panic!("expected a post");
        };
        assert!(matches!(
            calls
                .submit(
                    second.caller.clone(),
                    SubmissionPayload::Workspace(second_payload.clone())
                )
                .await,
            Submission::InOutbox(_)
        ));
        let held = peer.hold_next_batch();
        let next_mark = ClockInstant::ZERO.after(Duration::from_secs(80));
        clock.not_due_check_now(next_mark).await;
        clock.advance(Duration::from_secs(60)).unwrap();
        held.entered.notified().await;
        let sent = peer.batch_commands().pop().unwrap();
        assert!(
            matches!(sent.requests.requests(), [member] if member.payload().write_request_id() == Some(&second.request)),
            "a pending return must exclude A from the newer claim while B stays eligible"
        );

        let retry = ClockInstant::ZERO.after(Duration::from_secs(81));
        clock.not_due_check_now(retry).await;
        faults.fail_scoped_reads(false);
        clock.advance(Duration::from_secs(1)).unwrap();
        assert_eq!(peer.batch_commands().last(), Some(&sent));
        assert!(matches!(
            calls
                .request_status(first.caller.clone(), scoped_query(&first))
                .await,
            CustodyRequestStatus::InOutbox { .. }
        ));
        held.release.notify_one();
        let answered = loop {
            let report = flush_event(&mut events, "B's checked settlement").await;
            if !report.answered.is_empty() {
                break report.answered;
            }
        };
        assert!(matches!(answered.as_slice(), [answer] if answer.request == second.request));
        let replayed = loop {
            let report =
                flush_event(&mut events, "A's replay after B releases the frame slot").await;
            if !report.answered.is_empty() {
                break report.answered;
            }
        };
        assert!(matches!(replayed.as_slice(), [answer] if answer.request == first.request));
        stop.send_replace(true);
        running.await.unwrap();
    }
}

#[derive(Clone, Copy)]
enum OrphanArrivalOrder {
    HeldAfterEarlyAnswer,
    ExactDeadline,
    CutoffReportedFirst,
    EarlyStampPublishedAfterCutoff,
}

#[tokio::test]
async fn an_orphan_status_uses_its_accepted_instant_and_never_notifies_twice() {
    use OrphanArrivalOrder::*;
    for order in [
        HeldAfterEarlyAnswer,
        ExactDeadline,
        CutoffReportedFirst,
        EarlyStampPublishedAfterCutoff,
    ] {
        let campaign = FaultCampaign::start().await;
        let item = thread_post(609);
        seed_orphan(&campaign, &item, START_MS - 895_000).await;
        let clock = campaign.clock.clone();
        let peer = campaign.peer.clone();
        let faults = campaign.faults.clone();
        let query = scoped_query(&item);
        let central_answer = post_created(109);
        let refusal = RefusalReason::RequestIdReused;
        let (central_status, expected) = if matches!(order, CutoffReportedFirst) {
            (
                RequestStatusAnswer::Refused(central_link_protocol::RequestRefusal::Workspace(
                    refusal.clone(),
                )),
                CustodyRequestStatus::Refused(
                    switchboard_machine_core_store::OutboxRefusal::Central(
                        switchboard_machine_core_store::OutboxCentralRefusal::Workspace(refusal),
                    ),
                ),
            )
        } else {
            (
                RequestStatusAnswer::Answered(central_answer.clone()),
                CustodyRequestStatus::Answered(central_answer.clone()),
            )
        };
        peer.script_status_answer(item.caller.clone(), query.clone(), central_status);
        let status_gate = peer.hold_next_status();
        let (calls, stop, mut events, running) = start_worker(campaign.custody);
        status_gate.entered.notified().await;
        let cutoff = ClockInstant::ZERO.after(Duration::from_secs(5));
        clock.not_due_check_now(cutoff).await;
        if matches!(order, CutoffReportedFirst | EarlyStampPublishedAfterCutoff) {
            clock.advance(Duration::from_secs(5)).unwrap();
            let ending = flush_event(&mut events, "the cutoff before any answer").await;
            assert!(
                matches!(ending.ended.as_slice(), [ended] if ended.request_id == item.request
                && ended.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
            );
            assert!(ending.answered.is_empty());
            assert_eq!(
                calls
                    .request_status(item.caller.clone(), query.clone())
                    .await,
                CustodyRequestStatus::Ended {
                    reason: OutboxCancellation::NotConfirmedWithin15Minutes
                }
            );
            clock.advance(Duration::from_millis(1)).unwrap();
            let stamp = if matches!(order, EarlyStampPublishedAfterCutoff) {
                ClockInstant::ZERO.after(Duration::from_secs(4))
            } else {
                clock.monotonic_now()
            };
            status_gate.accepted.send_replace(Some(stamp));
        } else {
            let local_hold = faults.hold_next_scoped_read();
            let local_lookup = tokio::spawn({
                let calls = calls.clone();
                let caller = item.caller.clone();
                let query = query.clone();
                async move { calls.request_status(caller, query).await }
            });
            local_hold.entered.notified().await;
            clock.advance(Duration::from_secs(4)).unwrap();
            if matches!(order, HeldAfterEarlyAnswer) {
                status_gate
                    .accepted
                    .send_replace(Some(clock.monotonic_now()));
            }
            clock.advance(Duration::from_secs(1)).unwrap();
            if matches!(order, ExactDeadline) {
                status_gate
                    .accepted
                    .send_replace(Some(clock.monotonic_now()));
            }
            local_hold.release.notify_one();
            assert_eq!(local_lookup.await.unwrap(), CustodyRequestStatus::Sent);
            let settled = flush_event(&mut events, "the timestamp-ordered result").await;
            if matches!(order, HeldAfterEarlyAnswer) {
                assert!(
                    settled.ended.is_empty(),
                    "a published answer strictly before cutoff must win the ready-at-once race"
                );
                assert!(
                    matches!(settled.answered.as_slice(), [answer] if answer.request == item.request && answer.reply == central_answer)
                );
            } else {
                assert!(
                    settled.answered.is_empty(),
                    "an answer exactly at cutoff is late for the typed report"
                );
                assert!(
                    matches!(settled.ended.as_slice(), [ended] if ended.request_id == item.request
                    && ended.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
                );
            }
        }
        async {
            while calls
                .request_status(item.caller.clone(), query.clone())
                .await
                != expected
            {
                tokio::task::yield_now().await;
            }
        }
        .await;
        assert!(
            events.try_recv().is_err(),
            "the checked answer and cutoff cannot both notify"
        );
        assert!(peer.batch_commands().is_empty());
        stop.send_replace(true);
        running.await.unwrap();
    }
}

#[expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A malformed test write cannot exercise scoped orphan custody."
)]
pub(super) fn scoped_query(item: &NewOutboxItem) -> RequestStatusQuery {
    let OutboxQueuedWrite::Workspace(request) = &item.payload else {
        panic!("fixture requires a thread post");
    };
    RequestStatusQuery {
        request_id: item.request.clone(),
        scope: RequestScope::Workspace(request.workspace_id.clone().unwrap()),
    }
}

#[expect(
    clippy::unwrap_used,
    reason = "A fixture that could not open or seed SQLite cannot prove orphan behavior."
)]
async fn seed_orphan(campaign: &FaultCampaign, item: &NewOutboxItem, created_at: i64) {
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    store.insert(item, created_at).await.unwrap();
    let claim = store
        .claim_next_batch(
            item.flush_at_ms,
            1,
            created_at,
            created_at,
            &std::collections::BTreeSet::new(),
        )
        .await
        .unwrap();
    assert!(
        matches!(claim.healthy.writes.as_slice(), [claimed] if claimed.request == item.request)
    );
}

#[expect(
    clippy::panic,
    reason = "The test asserts the exact next worker outcome."
)]
pub(super) async fn flush_event(
    events: &mut mpsc::UnboundedReceiver<CustodyTaskEvent>,
    indication: &str,
) -> FlushReport {
    match events.recv().await {
        Some(CustodyTaskEvent::Flush(report)) => report,
        seen => panic!("expected {indication}, saw {seen:?}"),
    }
}

#[tokio::test]
async fn a_failed_startup_orphan_read_retries_offline_and_ends_at_the_original_deadline() {
    let campaign = FaultCampaign::start().await;
    campaign.disconnect();
    let item = thread_post(601);
    seed_orphan(&campaign, &item, START_MS - 895_000).await;
    campaign
        .faults
        .arm(FaultSite::SentWithoutAnswer, FaultStage::BeforeCall);
    let clock = campaign.clock.clone();
    let peer = campaign.peer.clone();
    let (calls, stop, mut events, running) = start_worker(campaign.custody);
    assert!(matches!(
        events.recv().await,
        Some(CustodyTaskEvent::Failed(_))
    ));
    let retry_at = ClockInstant::ZERO.after(Duration::from_secs(1));
    clock.not_due_check_now(retry_at).await;
    clock.advance(Duration::from_secs(1)).unwrap();

    let cutoff = ClockInstant::ZERO.after(Duration::from_secs(5));
    clock.not_due_check_now(cutoff).await;
    let query = scoped_query(&item);
    clock.advance(Duration::from_secs(3)).unwrap();
    assert_eq!(
        calls
            .request_status(item.caller.clone(), query.clone())
            .await,
        CustodyRequestStatus::Sent
    );
    clock.advance(Duration::from_secs(1)).unwrap();
    let ending = flush_event(&mut events, "one offline unconfirmed ending").await;
    assert!(
        matches!(ending.ended.as_slice(), [ended] if ended.request_id == item.request
        && ended.caller == item.caller && ended.reason == OutboxCancellation::NotConfirmedWithin15Minutes)
    );
    assert_eq!(
        calls.request_status(item.caller, query).await,
        CustodyRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes
        }
    );
    assert!(peer.batch_commands().is_empty());
    assert!(peer.take_calls().is_empty());
    stop.send_replace(true);
    running.await.unwrap();
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn a_failed_untouched_return_keeps_its_first_attempt_unsent_offline() {
    let campaign = FaultCampaign::start().await;
    let item = thread_post(605);
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    store.insert(&item, START_MS - 800_000).await.unwrap();
    drop(store);
    let clock = campaign.clock.clone();
    let peer = campaign.peer.clone();
    peer.decide_next_batch(Some(ScriptedBatch::Untouched));
    campaign
        .faults
        .arm(FaultSite::ReturnNotTransmitted, FaultStage::BeforeCall);
    let (calls, stop, mut events, running) = start_worker(campaign.custody);

    let mark = ClockInstant::ZERO.after(Duration::from_secs(20));
    clock.not_due_check_now(mark).await;
    clock.advance(Duration::from_secs(20)).unwrap();
    let attempted = flush_event(&mut events, "the failed definite return").await;
    assert_eq!(attempted.issues.len(), 1);
    peer.admitted.send_replace(None);
    let retry = ClockInstant::ZERO.after(Duration::from_secs(21));
    clock.not_due_check_now(retry).await;
    clock.advance(Duration::from_secs(1)).unwrap();
    let cutoff = ClockInstant::ZERO.after(Duration::from_secs(100));
    clock.not_due_check_now(cutoff).await;
    assert!(matches!(
        calls
            .request_status(item.caller.clone(), scoped_query(&item))
            .await,
        CustodyRequestStatus::InOutbox { .. }
    ));
    clock.advance(Duration::from_secs(79)).unwrap();
    let ended = flush_event(&mut events, "one definite not-sent ending").await;
    assert!(
        matches!(ended.ended.as_slice(), [one] if one.request_id == item.request
        && one.reason == OutboxCancellation::NotSentWithin15Minutes)
    );
    assert!(peer.batch_commands().is_empty());
    assert!(peer.take_calls().is_empty());
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_recovered_orphan_uses_the_scoped_status_answer_without_retransmission() {
    for answer in [
        RequestStatusAnswer::Answered(post_created(99)),
        RequestStatusAnswer::Refused(central_link_protocol::RequestRefusal::Workspace(
            RefusalReason::RequestIdReused,
        )),
    ] {
        let campaign = FaultCampaign::start().await;
        let item = thread_post(602);
        seed_orphan(&campaign, &item, START_MS - 10_000).await;
        let query = scoped_query(&item);
        campaign
            .peer
            .script_status_answer(item.caller.clone(), query.clone(), answer.clone());
        let peer = campaign.peer.clone();
        let (calls, stop, mut events, running) = start_worker(campaign.custody);
        let report = flush_event(&mut events, "checked scoped status settlement").await;
        assert!(
            matches!(report.answered.as_slice(), [settled] if settled.request == item.request && settled.caller == item.caller)
        );
        let status = calls
            .request_status(item.caller.clone(), query.clone())
            .await;
        match answer {
            RequestStatusAnswer::Answered(expected) => {
                assert_eq!(status, CustodyRequestStatus::Answered(expected))
            }
            RequestStatusAnswer::Refused(_) => {
                assert!(matches!(status, CustodyRequestStatus::Refused(_)))
            }
            RequestStatusAnswer::NotFound => panic!("the peer must know a result"),
        }
        assert!(matches!(peer.take_calls().as_slice(),
            [ScriptedCommand::RequestStatus(call)] if call.caller == item.caller && call.query == query));
        assert!(
            peer.batch_commands().is_empty(),
            "a known orphan must never be transmitted again"
        );
        stop.send_replace(true);
        running.await.unwrap();
    }
}

#[tokio::test]
async fn a_lost_local_claim_ack_is_returned_as_untouched_without_a_status_probe() {
    let next_mark = ClockInstant::ZERO.after(Duration::from_secs(80));
    let observer = NextMarkClock::watching(manual_clock(START_MS), next_mark);
    let campaign = FaultCampaign::start_with_clock(observer.source.clone(), observer.clone()).await;
    let item = thread_post(606);
    let OutboxQueuedWrite::Workspace(request) = &item.payload else {
        panic!("fixture requires a thread post");
    };
    let clock = campaign.clock.clone();
    let peer = campaign.peer.clone();
    let database = campaign.root.path().join("machine_local.sqlite");
    let returned = campaign.faults.hold_next_claim_return_commit();
    campaign
        .faults
        .arm(FaultSite::ClaimBatch, FaultStage::AfterSuccess);
    let (calls, stop, mut events, running) = start_worker(campaign.custody);
    assert!(matches!(
        calls
            .submit(
                item.caller.clone(),
                SubmissionPayload::Workspace(request.clone())
            )
            .await,
        Submission::InOutbox(_)
    ));
    let mark = ClockInstant::ZERO.after(Duration::from_secs(20));
    clock.not_due_check_now(mark).await;
    clock.advance(Duration::from_secs(20)).unwrap();
    assert!(matches!(
        events.recv().await,
        Some(CustodyTaskEvent::Failed(_))
    ));
    assert!(peer.batch_commands().is_empty());
    let retry = ClockInstant::ZERO.after(Duration::from_secs(21));
    clock.not_due_check_now(retry).await;
    clock.advance(Duration::from_secs(1)).unwrap();
    returned.committed.notified().await;
    let mut reopened = open_outbox(&database).await;
    let pending = reopened.pending_for(&item.caller).await.unwrap();
    assert!(
        matches!(pending.as_slice(), [one] if one.request_id == item.request &&
        matches!(one.state, switchboard_machine_core_store::PendingWriteState::Unsent { .. })),
        "a commit acknowledgment lost before the batch existed must restore the first write as unsent: {pending:?}"
    );
    assert!(
        peer.take_calls().is_empty(),
        "only claims found at startup ask Central for status"
    );
    assert!(peer.batch_commands().is_empty());
    returned.release.notify_one();
    let report = tokio::select! {
        report = flush_event(&mut events, "one healthy fresh settlement on the same mark") => Some(report),
        () = observer.suffix_is_deferred() => None,
    };
    assert!(
        report.is_some(),
        "an unsent return remains eligible on the open mark"
    );
    let report = report.unwrap();
    assert!(matches!(report.answered.as_slice(), [one] if one.request == item.request));
    assert_eq!(peer.batch_commands().len(), 1);
    assert_eq!(peer.command_instants(), [retry]);
    stop.send_replace(true);
    running.await.unwrap();
}

#[tokio::test]
async fn a_known_member_settles_locally_while_the_other_member_replays() {
    for stage in [FaultStage::BeforeCall, FaultStage::AfterSuccess] {
        let campaign = FaultCampaign::start().await;
        let ordered = [thread_post(603), thread_post(604)];
        let clock = campaign.clock.clone();
        let peer = campaign.peer.clone();
        let faults = campaign.faults.clone();
        peer.decide_next_batch(Some(ScriptedBatch::Members(vec![
            ScriptedMemberFate::Answered,
            ScriptedMemberFate::RecordedAnswerLost,
        ])));
        faults.arm(FaultSite::RecordOutcome, stage);
        let (calls, stop, mut events, running) = start_worker(campaign.custody);
        for item in &ordered {
            let OutboxQueuedWrite::Workspace(request) = &item.payload else {
                panic!("fixture requires a thread post");
            };
            assert!(matches!(
                calls
                    .submit(
                        item.caller.clone(),
                        SubmissionPayload::Workspace(request.clone())
                    )
                    .await,
                Submission::InOutbox(_)
            ));
        }
        let mark = ClockInstant::ZERO.after(Duration::from_secs(20));
        clock.not_due_check_now(mark).await;
        clock.advance(Duration::from_secs(20)).unwrap();
        let first = flush_event(&mut events, "partial batch settlement").await;
        assert_eq!(first.issues.len(), 1);
        if stage == FaultStage::BeforeCall {
            assert!(first.answered.is_empty());
            let retry = ClockInstant::ZERO.after(Duration::from_secs(21));
            clock.not_due_check_now(retry).await;
            clock.advance(Duration::from_secs(1)).unwrap();
            let local = flush_event(&mut events, "A's known local answer before B's replay").await;
            assert!(
                matches!(local.answered.as_slice(), [answer] if answer.request == ordered[0].request)
            );
        } else {
            assert!(
                matches!(first.answered.as_slice(), [answer] if answer.request == ordered[0].request),
                "after a lost commit ack the exact checked answer must still be reported"
            );
        }
        assert!(matches!(
            calls
                .request_status(ordered[0].caller.clone(), scoped_query(&ordered[0]))
                .await,
            CustodyRequestStatus::Answered(ReplyPayload::Workspace(_))
        ));
        let next = ClockInstant::ZERO.after(Duration::from_secs(80));
        clock.not_due_check_now(next).await;
        clock
            .advance(Duration::from_secs(if stage == FaultStage::BeforeCall {
                59
            } else {
                60
            }))
            .unwrap();
        let second = flush_event(&mut events, "B's replay settlement").await;
        assert!(
            matches!(second.answered.as_slice(), [answer] if answer.request == ordered[1].request)
        );
        let commands = peer.batch_commands();
        assert_eq!(commands.len(), 2);
        let replay = commands[1].requests.requests();
        assert!(
            matches!(replay, [member] if member.payload().write_request_id() == Some(&ordered[1].request))
        );
        assert!(
            peer.take_calls().is_empty(),
            "live evidence needs no startup status probe"
        );
        stop.send_replace(true);
        running.await.unwrap();
    }
}
