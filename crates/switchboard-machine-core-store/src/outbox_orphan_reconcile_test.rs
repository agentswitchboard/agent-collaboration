use super::{MARK, accepted_answer, caller, item, open, sender_outcomes::direct_write, uuid_v7};
use crate::{
    MachineLocalOutbox, OutboxCancellation, OutboxQueuedWrite, OutboxRequestKey,
    OutboxRequestStatus, OutboxScope, OutboxStorage, OutboxStoreError, OutboxWriteOutcome,
    OutboxWriteTarget, UnansweredOutboxClaim,
};
use router_clock::{ManualClock, RuntimeClock};
use std::{collections::BTreeSet, time::Duration};

const FIFTEEN_MINUTES_MS: i64 = 15 * 60 * 1_000;

#[tokio::test]
async fn unanswered_claims_keep_their_reconciliation_projection_across_two_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let post = item(510, "original post", MARK);
    let mut direct = direct_write(511);
    direct.caller = caller(2);
    let waiting = item(512, "never claimed", MARK + 60_000);
    let post_created = MARK - 5_000;
    let direct_created = MARK - 1_000;
    {
        let mut store = MachineLocalOutbox::new(open(&directory).await);
        store.insert(&post, post_created).await.unwrap();
        store.insert(&direct, direct_created).await.unwrap();
        let now = clock.utc_now().timestamp_millis();
        let first = store
            .claim_next_batch(MARK, 2, now, now)
            .await
            .unwrap()
            .expect_clean();
        assert_eq!(first.writes.len(), 2);
        clock.advance(Duration::from_millis(10)).unwrap();
        let now = clock.utc_now().timestamp_millis();
        store
            .return_unanswered(std::slice::from_ref(&direct.request), now)
            .await
            .unwrap()
            .expect_clean();
        let replay = store
            .claim_next_batch(MARK, 1, now, now)
            .await
            .unwrap()
            .expect_clean();
        assert_eq!(replay.writes[0].request, direct.request);
        store.insert(&waiting, now).await.unwrap();
    }

    let workspace_id = uuid_v7(60_001).parse().unwrap();
    let OutboxQueuedWrite::SendMessage { message, .. } = &direct.payload else {
        panic!("a direct-message fixture")
    };
    let expected = [
        UnansweredOutboxClaim {
            key: OutboxRequestKey {
                request_id: post.request.clone(),
                scope: OutboxScope::Workspace(workspace_id),
            },
            caller: post.caller.clone(),
            target: OutboxWriteTarget::Thread {
                workspace_id: Some(uuid_v7(60_001).parse().unwrap()),
                thread_id: uuid_v7(60_002).parse().unwrap(),
            },
            deadline_ms: post_created + FIFTEEN_MINUTES_MS,
            payload: post.payload.clone(),
        },
        UnansweredOutboxClaim {
            key: OutboxRequestKey {
                request_id: direct.request.clone(),
                scope: OutboxScope::Organization,
            },
            caller: direct.caller.clone(),
            target: OutboxWriteTarget::Session {
                to: message.to.clone(),
            },
            deadline_ms: direct_created + FIFTEEN_MINUTES_MS,
            payload: direct.payload.clone(),
        },
    ];

    for _ in 0..2 {
        clock.advance(Duration::from_secs(1)).unwrap();
        let mut store = MachineLocalOutbox::new(open(&directory).await);
        let now = clock.utc_now().timestamp_millis();
        assert!(
            store
                .end_overdue_orphans(now, &BTreeSet::new())
                .await
                .unwrap()
                .expect_clean()
                .is_empty()
        );
        let restored = store.sent_without_answer().await.unwrap().expect_clean();
        assert_eq!(restored, expected);
        for claim in &restored {
            assert_eq!(
                store
                    .scoped_request_status(&claim.caller, &claim.key)
                    .await
                    .unwrap(),
                OutboxRequestStatus::Sent,
            );
        }
    }
}

#[tokio::test]
async fn overdue_orphans_end_at_the_original_deadline_without_ending_live_claims() {
    let directory = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let deadline = MARK + 1_000;
    let created_at = deadline - FIFTEEN_MINUTES_MS;
    let sent = item(520, "abandoned first attempt", MARK);
    let resent = item(521, "abandoned replay", MARK);
    let live = item(522, "live batch", MARK);
    let later = item(523, "still within its own deadline", MARK);
    let waiting = item(524, "never claimed", MARK);
    {
        let mut store = MachineLocalOutbox::new(open(&directory).await);
        for write in [&sent, &resent, &live] {
            store.insert(write, created_at).await.unwrap();
        }
        store.insert(&later, created_at + 1).await.unwrap();
        let first = store
            .claim_next_batch(MARK, 4, MARK, MARK)
            .await
            .unwrap()
            .expect_clean();
        assert_eq!(first.writes.len(), 4);
        clock.advance(Duration::from_millis(1)).unwrap();
        let now = clock.utc_now().timestamp_millis();
        store
            .return_unanswered(std::slice::from_ref(&resent.request), now)
            .await
            .unwrap()
            .expect_clean();
        let retry = store
            .claim_next_batch(MARK, 1, now, now)
            .await
            .unwrap()
            .expect_clean();
        assert_eq!(retry.writes[0].request, resent.request);
        store.insert(&waiting, created_at).await.unwrap();
    }
    let mut store = MachineLocalOutbox::new(open(&directory).await);
    let live_tickets = BTreeSet::from([live.request.clone()]);
    assert!(matches!(
        store.end_overdue_orphans(i64::MAX, &live_tickets).await,
        Err(OutboxStoreError::InvalidTimestamp {
            field: "updated_at"
        }),
    ));
    clock.advance(Duration::from_millis(998)).unwrap();
    assert!(
        store
            .end_overdue_orphans(clock.utc_now().timestamp_millis(), &live_tickets)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );

    clock.advance(Duration::from_millis(1)).unwrap();
    let ended = store
        .end_overdue_orphans(clock.utc_now().timestamp_millis(), &live_tickets)
        .await
        .unwrap()
        .expect_clean();

    let reason = OutboxCancellation::NotConfirmedWithin15Minutes;
    assert_eq!(
        ended
            .into_iter()
            .map(|write| (write.request_id, write.reason))
            .collect::<Vec<_>>(),
        [
            (sent.request.clone(), reason),
            (resent.request.clone(), reason)
        ],
    );
    for write in [&sent, &resent] {
        assert_eq!(
            store
                .request_status(&write.caller, &write.request)
                .await
                .unwrap(),
            OutboxRequestStatus::Ended { reason }
        );
    }
    let remaining = store.sent_without_answer().await.unwrap().expect_clean();
    assert_eq!(
        remaining
            .iter()
            .map(|claim| (&claim.key.request_id, claim.deadline_ms))
            .collect::<Vec<_>>(),
        [(&live.request, deadline), (&later.request, deadline + 1)],
    );
    assert_eq!(remaining[0].payload, live.payload);
    assert_eq!(
        store
            .request_status(&waiting.caller, &waiting.request)
            .await
            .unwrap(),
        OutboxRequestStatus::InOutbox { flush_at_ms: MARK }
    );
    let mut database = open(&directory).await;
    let live_updated_at: i64 =
        sqlx::query_scalar("SELECT updated_at FROM outbox_items WHERE request_id = ?")
            .bind(live.request.as_str())
            .fetch_one(&mut database.connection)
            .await
            .unwrap();
    assert_eq!(live_updated_at, MARK);
}

#[tokio::test]
async fn late_permanent_answer_settles_the_reopened_claim_using_its_projection() {
    let directory = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(chrono::DateTime::from_timestamp_millis(MARK).unwrap());
    let write = item(530, "post awaiting a reconciled answer", MARK);
    {
        let mut store = MachineLocalOutbox::new(open(&directory).await);
        store.insert(&write, MARK - 1_000).await.unwrap();
        let first = store
            .claim_next_batch(MARK, 1, MARK, MARK)
            .await
            .unwrap()
            .expect_clean();
        assert_eq!(first.writes[0].request, write.request);
    }
    clock.advance(Duration::from_secs(30)).unwrap();
    let now = clock.utc_now().timestamp_millis();
    let mut store = MachineLocalOutbox::new(open(&directory).await);
    assert!(
        store
            .end_overdue_orphans(now, &BTreeSet::new())
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    let orphan = store
        .sent_without_answer()
        .await
        .unwrap()
        .expect_clean()
        .pop()
        .unwrap();
    let central_answer = accepted_answer();
    let settlement =
        OutboxWriteOutcome::from_accepted(&orphan.payload, central_answer.clone()).unwrap();

    store
        .record_outcome(&orphan.key.request_id, &settlement, now)
        .await
        .unwrap();

    assert_eq!(
        store
            .scoped_request_status(&orphan.caller, &orphan.key)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(central_answer)
    );
    assert!(
        store
            .sent_without_answer()
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
}
