use super::{MARK, accepted, caller, item, open, sender_outcomes::direct_write, uuid_v7};
use crate::{
    MacOutbox, MachineLocalStore, OutboxAccepted, OutboxCentralRefusal, OutboxRefusal,
    OutboxRequestStatus, OutboxScope, OutboxStoreError, OutboxWriteOutcome, OutboxWriteTarget,
    PendingWriteState, StoredOutboxWrite, StoredSenderOutcome,
};
use workspace_model::{ItemRef, RefusalReason, Timestamp, central_mail::MessageOutcome};

async fn return_and_reclaim(
    outbox: &mut MacOutbox<'_>,
    claims: &[StoredOutboxWrite],
) -> Vec<StoredOutboxWrite> {
    let ids: Vec<_> = claims.iter().map(|claim| claim.request.clone()).collect();
    assert!(
        outbox
            .return_unanswered(&ids, MARK + 1)
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    outbox
        .claim_next_batch(MARK, 2, MARK + 1, MARK + 1)
        .await
        .unwrap()
        .expect_clean()
        .writes
}

#[tokio::test]
async fn swapped_same_kind_settlements_leave_both_claims_recoverable() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = open(&directory).await;
    let first = direct_write(410);
    let second = direct_write(411);
    let mut outbox = MacOutbox::new(&mut database);
    for write in [&first, &second] {
        outbox.insert(write, MARK - 1_000).await.unwrap();
    }
    let claims = outbox
        .claim_next_batch(MARK, 2, MARK, MARK)
        .await
        .unwrap()
        .expect_clean()
        .writes;
    let settlements: Vec<_> = claims
        .iter()
        .zip([412, 413])
        .map(|(claim, seed)| {
            OutboxWriteOutcome::from_accepted(
                &claim.payload,
                OutboxAccepted::SendMessage {
                    message_id: uuid_v7(seed).parse().unwrap(),
                },
            )
            .unwrap()
        })
        .collect();

    for (claim, swapped) in claims.iter().zip(settlements.iter().rev()) {
        assert!(matches!(
            outbox.record_outcome(&claim.request, swapped, MARK + 1).await,
            Err(OutboxStoreError::InvalidOutcome { request }) if request == claim.request
        ));
        assert_eq!(
            outbox
                .request_status(&claim.caller, &claim.request)
                .await
                .unwrap(),
            OutboxRequestStatus::Sent
        );
    }
    let pending = outbox.pending_for(&first.caller).await.unwrap();
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|write| matches!(&write.state,
        PendingWriteState::SentAnswerLost { body } if body.as_str() == "hello")));
    assert_eq!(return_and_reclaim(&mut outbox, &claims).await, claims);

    // A correctly paired terminal result settles after the rejected swaps.
    outbox
        .record_outcome(&claims[0].request, &settlements[0], MARK + 2)
        .await
        .unwrap();
    assert!(matches!(
        outbox
            .request_status(&first.caller, &first.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(_)
    ));
}

#[tokio::test]
async fn not_found_from_another_workspace_cannot_settle_the_claim() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = open(&directory).await;
    let write = item(420, "retain the original post", MARK);
    let mut outbox = MacOutbox::new(&mut database);
    outbox.insert(&write, MARK - 1_000).await.unwrap();
    let claims = outbox
        .claim_next_batch(MARK, 1, MARK, MARK)
        .await
        .unwrap()
        .expect_clean()
        .writes;
    let claim = &claims[0];
    let OutboxWriteTarget::Thread {
        workspace_id: Some(workspace_id),
        thread_id,
    } = claim.payload.content().unwrap().target
    else {
        panic!("a claimed thread post")
    };
    let wrong = OutboxScope::Workspace(uuid_v7(421).parse().unwrap());
    let item = ItemRef::Thread(thread_id);
    assert!(
        OutboxWriteOutcome::from_workspace_not_found(&claim.payload, &wrong, item.clone()).is_err()
    );
    let contextless =
        OutboxRefusal::Central(OutboxCentralRefusal::Workspace(RefusalReason::NotFound {
            item: item.clone(),
        }));
    assert!(OutboxWriteOutcome::from_refusal(&claim.payload, contextless).is_err());
    assert_eq!(
        outbox
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Sent
    );
    assert!(
        matches!(&outbox.pending_for(&write.caller).await.unwrap()[0].state,
        PendingWriteState::SentAnswerLost { body } if body.as_str() == "retain the original post")
    );
    assert_eq!(return_and_reclaim(&mut outbox, &claims).await, claims);
    let compatible = OutboxWriteOutcome::from_workspace_not_found(
        &claim.payload,
        &OutboxScope::Workspace(workspace_id),
        item,
    )
    .unwrap();
    outbox
        .record_outcome(&claim.request, &compatible, MARK + 2)
        .await
        .unwrap();
    assert!(matches!(
        outbox
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Refused(_)
    ));
}

type ClockSnapshot = (String, String, String, Option<String>, i64, i64, i64, i64);

async fn clock_snapshot(database: &mut MachineLocalStore) -> Vec<ClockSnapshot> {
    sqlx::query_as(
        "SELECT request_id, item_state, payload_json, outcome_json, created_at, updated_at,
         flush_at_ms, expires_at_ms FROM outbox_items ORDER BY request_id",
    )
    .fetch_all(&mut database.connection)
    .await
    .unwrap()
}

fn invalid_timestamp<T: std::fmt::Debug>(result: Result<T, OutboxStoreError>, field: &'static str) {
    assert!(
        matches!(result, Err(OutboxStoreError::InvalidTimestamp { field: invalid }) if invalid == field),
        "expected invalid timestamp for {field}, got {result:?}"
    );
}

#[tokio::test]
async fn invalid_incoming_outbox_clocks_leave_enqueued_and_claimed_rows_intact() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = open(&directory).await;
    let claimed = item(430, "healthy claim", MARK);
    let waiting = item(431, "healthy waiting", MARK);
    let fresh = item(432, "never inserted", MARK);
    let mut outbox = MacOutbox::new(&mut database);
    for write in [&claimed, &waiting] {
        outbox.insert(write, MARK).await.unwrap();
    }
    assert_eq!(
        outbox
            .claim_next_batch(MARK, 1, MARK, MARK)
            .await
            .unwrap()
            .expect_clean()
            .writes[0]
            .request,
        claimed.request
    );
    let before = clock_snapshot(&mut database).await;
    let mut outbox = MacOutbox::new(&mut database);

    invalid_timestamp(
        outbox
            .return_not_transmitted(&claimed.request, i64::MAX)
            .await,
        "updated_at",
    );
    invalid_timestamp(
        outbox
            .return_unanswered(std::slice::from_ref(&claimed.request), i64::MAX)
            .await,
        "updated_at",
    );
    invalid_timestamp(
        outbox
            .record_outcome(&claimed.request, &accepted(&claimed.payload), i64::MAX)
            .await,
        "updated_at",
    );
    invalid_timestamp(
        outbox.claim_next_batch(MARK, 1, i64::MAX, i64::MAX).await,
        "updated_at",
    );
    invalid_timestamp(
        outbox.claim_next_batch(i64::MAX, 1, MARK, MARK).await,
        "mark_ms",
    );
    invalid_timestamp(outbox.end_overdue(i64::MAX).await, "updated_at");
    invalid_timestamp(
        outbox.delete_ended_before_retention(i64::MAX).await,
        "now_ms",
    );
    invalid_timestamp(outbox.insert(&fresh, i64::MIN).await, "created_at");
    let invalid_mark = crate::NewOutboxItem {
        flush_at_ms: i64::MAX,
        ..fresh
    };
    invalid_timestamp(outbox.insert(&invalid_mark, MARK).await, "flush_at_ms");
    assert_eq!(clock_snapshot(&mut database).await, before);
}

#[tokio::test]
async fn invalid_incoming_sender_outcome_clock_preserves_the_claim() {
    let directory = tempfile::tempdir().unwrap();
    let mut database = open(&directory).await;
    let write = direct_write(440);
    let mut outbox = MacOutbox::new(&mut database);
    outbox.insert(&write, MARK).await.unwrap();
    outbox
        .claim_next_batch(MARK, 1, MARK, MARK)
        .await
        .unwrap()
        .expect_clean();
    let before = clock_snapshot(&mut database).await;
    let notice = StoredSenderOutcome {
        inbox_message_id: uuid_v7(441).parse().unwrap(),
        message_id: uuid_v7(442).parse().unwrap(),
        request_id: write.request,
        reported_at: Timestamp::UNIX_EPOCH,
        outcome: MessageOutcome::DeliveryNotConfirmed,
    };
    let mut outbox = MacOutbox::new(&mut database);
    invalid_timestamp(
        outbox
            .record_sender_delivery(&caller(1), &notice, i64::MAX)
            .await,
        "updated_at",
    );
    assert_eq!(clock_snapshot(&mut database).await, before);
}
