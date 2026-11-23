//! Focused PR 307 recovery scenarios using the current batch settlement boundary.

use central_link_client::{
    BatchResultsError, CheckedBatchResults, CorrelatedRequestReply, LINK_LANE_SETTLEMENT_LIMIT,
    ReadyLinkLane, WriteBatchDisposition, WriteBatchMemberDisposition,
};
use router_clock::RuntimeClock;
use runtime_primitives::SyncTime;
use std::time::Duration;
use switchboard_machine_core_store::{OutboxStorage, PendingWriteState};
use switchboard_message_custody::{
    outbox_dispatch::{BatchDispatchError, OutboxDispatcher},
    outbox_work::OutboxWork,
};

use super::{
    batch_replay::{ScriptedBatch, ScriptedMemberFate, post_created},
    faulted_overlay::{FaultCampaign, MARK_MS, START_MS},
    faulty_outbox::{FaultSite, FaultStage, FaultyOutbox},
    outbox_timing::{open_outbox, thread_post},
};

#[tokio::test]
async fn a_failed_next_claim_carries_the_member_settled_before_it() {
    let campaign = FaultCampaign::start().await;
    let first = thread_post(2_902);
    let mut second = thread_post(2_903);
    second.flush_at_ms += 60_000;
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    for item in [&first, &second] {
        store.insert(item, START_MS).await.unwrap();
    }
    let mut work = OutboxWork::new(
        Box::new(FaultyOutbox {
            inner: Box::new(store),
            plan: campaign.faults.clone(),
        }),
        campaign.clock.clone(),
    );
    let admitted = campaign.peer.admitted.borrow().clone().unwrap();
    let mut dispatcher = OutboxDispatcher::default();
    campaign.clock.advance(Duration::from_secs(20)).unwrap();
    let batch = dispatcher
        .prepare_at_mark(
            &mut work,
            admitted.connection.clone(),
            admitted.clock_anchor,
            SyncTime::from_unix_millis(MARK_MS),
        )
        .await
        .unwrap()
        .batch
        .unwrap();
    let result = campaign.peer.submit_write_batch(batch.command()).await;
    campaign
        .faults
        .arm(FaultSite::RecordOutcome, FaultStage::BeforeCall);
    let pending = dispatcher
        .settle_batch(&mut work, batch, result)
        .await
        .unwrap();
    assert!(pending.answered.is_empty());
    assert_eq!(pending.issues.len(), 1);

    campaign.clock.advance(Duration::from_secs(60)).unwrap();
    campaign
        .faults
        .arm(FaultSite::ClaimBatch, FaultStage::BeforeCall);
    let failed = dispatcher
        .prepare_at_mark(
            &mut work,
            admitted.connection,
            admitted.clock_anchor,
            SyncTime::from_unix_millis(second.flush_at_ms),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            failed,
            BatchDispatchError::Interrupted { completed, error }
                if matches!(*error, BatchDispatchError::Work(_))
                    && matches!(completed.answered.as_slice(), [answer] if answer.request == first.request)
        ),
        "a later failed claim must carry the earlier completed member's typed report"
    );
    assert_eq!(campaign.peer.batch_commands().len(), 1);
    assert!(campaign.faults.is_drained());
}

#[tokio::test]
async fn a_lone_sent_row_at_startup_is_replayed_after_a_restore_fault() {
    for stage in [FaultStage::BeforeCall, FaultStage::AfterSuccess] {
        let mut campaign = FaultCampaign::start().await;
        let mut item = thread_post(2_900);
        item.flush_at_ms = MARK_MS - 60_000;
        let created_at_ms = MARK_MS - 90_000;
        let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
        store.insert(&item, created_at_ms).await.unwrap();
        let claim = store
            .claim_next_batch(item.flush_at_ms, 1, item.flush_at_ms, item.flush_at_ms)
            .await
            .unwrap();
        assert_eq!(claim.healthy.writes.len(), 1);
        drop(store);
        campaign.accept_seeded(&item, created_at_ms);

        campaign.faults.arm(FaultSite::SentWithoutAnswer, stage);
        assert!(campaign.custody.recover_outbox().await.is_err());
        assert_eq!(
            campaign.faults.fired(),
            [(FaultSite::SentWithoutAnswer, stage)]
        );
        campaign.custody.recover_outbox().await.unwrap();
        campaign.heal_and_drain().await;
        let commands = campaign.peer.batch_commands();
        assert_eq!(
            commands.len(),
            1,
            "startup must release the lone claim exactly once"
        );
        assert_eq!(
            commands[0].requests.requests()[0]
                .payload()
                .write_request_id(),
            Some(&item.request)
        );
    }
}

#[tokio::test]
async fn an_untouched_first_attempt_remains_unsent() {
    let mut campaign = FaultCampaign::start().await;
    campaign.submit(None).await;
    campaign
        .next_mark(Some(ScriptedBatch::Untouched))
        .await
        .unwrap();
    assert!(
        matches!(
            campaign.pending().await[0].state,
            PendingWriteState::Unsent { .. }
        ),
        "a frame that never reached the wire leaves a first write unsent"
    );
    assert!(campaign.peer.decisions().is_empty());
    campaign.heal_and_drain().await;
}

#[tokio::test]
async fn an_untouched_replay_preserves_the_earlier_possibly_recorded_write() {
    let mut campaign = FaultCampaign::start().await;
    let item = campaign.submit(None).await;
    campaign
        .next_mark(Some(ScriptedBatch::Members(vec![
            ScriptedMemberFate::RecordedAnswerLost,
        ])))
        .await
        .unwrap();
    campaign.disconnect();
    campaign.reconnect();
    campaign
        .next_mark(Some(ScriptedBatch::Untouched))
        .await
        .unwrap();
    assert!(
        matches!(
            campaign.pending().await[0].state,
            PendingWriteState::SentAnswerLost { .. }
        ),
        "Central holds the first attempt even when the replay was untouched"
    );
    assert!(campaign.peer.decisions().contains_key(&item.request));
    let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
    assert!(
        store
            .sent_without_answer()
            .await
            .unwrap()
            .healthy
            .is_empty(),
        "the untouched replay must relinquish its claim without erasing earlier uncertainty"
    );
    campaign.heal_and_drain().await;
}

#[tokio::test]
async fn a_reply_after_timeout_cannot_settle_the_replay() {
    for fault in [
        None,
        Some(FaultStage::BeforeCall),
        Some(FaultStage::AfterSuccess),
    ] {
        let campaign = FaultCampaign::start().await;
        let item = thread_post(2_901);
        let mut store = open_outbox(&campaign.root.path().join("machine_local.sqlite")).await;
        store.insert(&item, START_MS).await.unwrap();
        let mut work = OutboxWork::new(
            Box::new(FaultyOutbox {
                inner: Box::new(store),
                plan: campaign.faults.clone(),
            }),
            campaign.clock.clone(),
        );
        let admitted = campaign.peer.admitted.borrow().clone().unwrap();
        let mut dispatcher = OutboxDispatcher::default();
        campaign.clock.advance(Duration::from_secs(20)).unwrap();
        let first = dispatcher
            .prepare_at_mark(
                &mut work,
                admitted.connection.clone(),
                admitted.clock_anchor,
                SyncTime::from_unix_millis(MARK_MS),
            )
            .await
            .unwrap()
            .batch
            .unwrap();
        campaign
            .peer
            .decide_next_batch(Some(ScriptedBatch::Members(vec![
                ScriptedMemberFate::RecordedAnswerLost,
            ])));
        let timed_out = campaign.peer.submit_write_batch(first.command()).await;
        campaign.clock.advance(LINK_LANE_SETTLEMENT_LIMIT).unwrap();
        if let Some(stage) = fault {
            campaign.faults.arm(FaultSite::ReturnUnanswered, stage);
        }
        let report = dispatcher
            .settle_batch(&mut work, first.clone(), timed_out)
            .await
            .unwrap();
        assert_eq!(report.issues.len(), usize::from(fault.is_some()));
        if let Some(stage) = fault {
            assert_eq!(
                campaign.faults.fired(),
                [(FaultSite::ReturnUnanswered, stage)]
            );
        }

        let next_mark_ms = MARK_MS + 60_000;
        let until_next_ms = next_mark_ms - campaign.clock.utc_now().timestamp_millis();
        campaign
            .clock
            .advance(Duration::from_millis(u64::try_from(until_next_ms).unwrap()))
            .unwrap();
        let replay = dispatcher
            .prepare_at_mark(
                &mut work,
                admitted.connection,
                admitted.clock_anchor,
                SyncTime::from_unix_millis(next_mark_ms),
            )
            .await
            .unwrap()
            .batch
            .unwrap();
        let first_command = first.command();
        let replay_command = replay.command();
        let original = &first_command.requests.requests()[0];
        let retried = &replay_command.requests.requests()[0];
        assert_eq!(retried.payload().write_request_id(), Some(&item.request));
        assert_eq!(retried.caller(), original.caller());
        assert_eq!(retried.payload(), original.payload());
        assert_ne!(retried.correlation_id(), original.correlation_id());
        let late = WriteBatchMemberDisposition::Answered {
            reply: CorrelatedRequestReply {
                correlation_id: original.correlation_id().clone(),
                result: Box::new(post_created(999)),
            },
        };
        assert_eq!(
            CheckedBatchResults::new(&replay_command.requests, [late.clone()]),
            Err(BatchResultsError::WrongMember { index: 0 })
        );
        let old_results = CheckedBatchResults::new(&first_command.requests, [late]).unwrap();
        assert!(
            dispatcher
                .settle_batch(
                    &mut work,
                    first,
                    WriteBatchDisposition::MemberResults(old_results)
                )
                .await
                .is_none(),
            "an older checked result cannot settle or release the live replay"
        );
        let current = campaign.peer.submit_write_batch(replay_command).await;
        let answered = dispatcher
            .settle_batch(&mut work, replay, current)
            .await
            .unwrap();
        assert!(answered.issues.is_empty());
        assert_eq!(answered.answered.len(), 1);
        assert_eq!(answered.answered[0].request, item.request);
        assert_eq!(
            answered.answered[0].reply,
            campaign.peer.decisions()[&item.request]
        );
    }
}
