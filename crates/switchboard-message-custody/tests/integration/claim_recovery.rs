//! A subsequent frame can outlive the custody worker that selected it.

use router_clock::ClockInstant;
use std::time::Duration;
use switchboard_machine_core_store::{OutboxRequestStatus, OutboxStorage, PendingWriteState};
use workspace_model::Caller;

use super::{
    batch_limits::{custody_with_store, limits_fixture},
    batch_replay::{ScriptedBatch, ScriptedCommand, ScriptedMemberFate, new_admission},
    mark_drain::queue_posts,
    outbox_timing::{open_outbox, thread_post},
    worker_orphans::{flush_event, scoped_query, start_worker},
};

#[tokio::test]
async fn restart_recovers_the_second_frames_claim_using_its_original_request_id() {
    check_subsequent_claim_recovery(65).await;
}

#[expect(
    clippy::panic,
    clippy::unwrap_used,
    reason = "A missing fixture observation must fail the recovery proof."
)]
pub(super) async fn check_subsequent_claim_recovery(count: u16) {
    let (root, mut custody, peer, clock) = limits_fixture().await;
    let original = queue_posts(&mut custody, count).await;
    let (first_ids, orphan_ids) = original.split_at_checked(64).unwrap();
    let item = thread_post(65);
    let Caller::Session(owner) = &item.caller else {
        panic!("the worker's original caller is a fixture session");
    };
    let first_gate = peer.hold_next_batch();
    let (_calls, _stop, mut events, running) = start_worker(custody);
    let mark = Duration::from_secs(20);
    clock
        .not_due_check_now(ClockInstant::ZERO.after(mark))
        .await;
    clock.advance(mark).unwrap();
    first_gate.entered.notified().await;
    let second_gate = peer.hold_next_batch();
    peer.decide_next_batch(Some(ScriptedBatch::Members(vec![
        ScriptedMemberFate::LostBeforeCentral;
        orphan_ids.len()
    ])));
    first_gate.release.notify_one();
    let settled = flush_event(
        &mut events,
        "the first frame settles before the second claim",
    )
    .await;
    assert_eq!(
        settled
            .answered
            .into_iter()
            .map(|answer| answer.request)
            .collect::<Vec<_>>(),
        first_ids
    );
    second_gate.entered.notified().await;
    let db = root.path().join("machine_local.sqlite");
    let mut store = open_outbox(&db).await;
    assert_eq!(
        store
            .request_status(&item.caller, &item.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Sent
    );

    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    peer.admitted
        .send_replace(Some(new_admission(owner.machine_id.clone(), clock.clone())));
    let mut reopened =
        custody_with_store(&db, peer.clone(), clock.clone(), owner.machine_id.clone()).await;
    assert!(reopened.recover_outbox().await.unwrap().issues.is_empty());
    let probes = peer.take_calls();
    let queried: Vec<_> = probes
        .iter()
        .map(|probe| match probe {
            ScriptedCommand::RequestStatus(command) => &command.query.request_id,
            ScriptedCommand::ImmediateRequest(_) => panic!("recovery must use scoped status"),
        })
        .collect();
    assert_eq!(queried, orphan_ids.iter().collect::<Vec<_>>());
    assert!(probes.iter().any(|probe|
        matches!(probe, ScriptedCommand::RequestStatus(command) if command.query == scoped_query(&item))));
    assert!(
        store
            .pending_for(&item.caller)
            .await
            .unwrap()
            .into_iter()
            .any(|pending| pending.request_id == item.request
                && matches!(pending.state, PendingWriteState::SentAnswerLost { .. }))
    );

    clock.advance(Duration::from_secs(60)).unwrap();
    let replayed = reopened.flush_writes().await.unwrap();
    assert!(replayed.ended.is_empty() && replayed.issues.is_empty());
    assert_eq!(
        replayed
            .answered
            .into_iter()
            .map(|answer| answer.request)
            .collect::<Vec<_>>(),
        orphan_ids
    );
    let commands = peer.batch_commands();
    let [_, original_claim, replay] = commands.as_slice() else {
        panic!("the first, lost, and replay frames must be distinct");
    };
    for frame in [original_claim, replay] {
        assert_eq!(
            frame
                .requests
                .requests()
                .iter()
                .map(|write| write.payload().write_request_id().unwrap())
                .collect::<Vec<_>>(),
            orphan_ids.iter().collect::<Vec<_>>(),
        );
    }
}
