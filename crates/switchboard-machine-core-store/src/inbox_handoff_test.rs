use super::super::{
    InboxDecision, InboxOfferExpiry, InboxRefusalReason, InboxStore, read_delivery,
    tests::{fixture_expiry, message},
};
use super::{
    DirectHandoff, DirectHandoffChange, HandoffCommit, InboxStoreError, NoticeAttemptId,
    SeenReceiptState,
};
use crate::{MachineLocalStore, RowCorruptionReason};
use uuid::Uuid;
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageDelivery, MessageRecord},
};

const ARRIVAL: i64 = 1_000;

fn at(millis: i64) -> Timestamp {
    Timestamp::from_timestamp_millis(millis).unwrap()
}

fn attempt() -> NoticeAttemptId {
    NoticeAttemptId::try_from("019f0000-0000-7000-8000-000000000011".to_owned()).unwrap()
}

async fn keep(store: &mut InboxStore, delivery: &MessageRecord) {
    store
        .record_delivery(
            delivery,
            InboxDecision::Accepted,
            ARRIVAL,
            fixture_expiry(ARRIVAL),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn inbox_page_is_recent_ordered_limited_and_notice_selection_only_returns_waiting_auto() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let expired = message(1);
    let waiting = message(2);
    let completed = message(3);
    let mut queued = message(4);
    if let MessageBody::Direct { delivery, .. } = &mut queued.body {
        *delivery = MessageDelivery::Queue;
    }
    for (row, arrival) in [
        (&expired, ARRIVAL),
        (&waiting, ARRIVAL + 1),
        (&completed, ARRIVAL + 2),
        (&queued, ARRIVAL + 3),
    ] {
        store
            .record_delivery(
                row,
                InboxDecision::Accepted,
                arrival,
                fixture_expiry(arrival),
            )
            .await
            .unwrap();
    }
    store
        .record_handoff(&completed.message_id, ARRIVAL + 3)
        .await
        .unwrap();
    let target = &waiting.target;
    let page = store
        .direct_inbox_messages(target, 2)
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(page, [queued.clone(), completed.clone()]);
    let page = store
        .direct_inbox_messages(target, 20)
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(page, [queued, completed, waiting.clone(), expired.clone()]);

    let rows = store
        .scan_waiting_notice_handoffs()
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(
        rows.into_iter()
            .map(|row| row.message_id)
            .collect::<Vec<_>>(),
        [expired.message_id, waiting.message_id]
    );
}

#[tokio::test]
async fn current_pending_and_completed_direct_rows_have_distinct_reopen_meanings() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let waiting = message(1);
    let completed = message(2);
    let mut system = message(3);
    system.body = MessageBody::Notice { items: vec![] };
    let refused = message(4);
    keep(&mut store, &waiting).await;
    keep(&mut store, &completed).await;
    store
        .record_handoff(&completed.message_id, ARRIVAL + 1)
        .await
        .unwrap();
    store
        .record_delivery(
            &system,
            InboxDecision::Accepted,
            ARRIVAL,
            InboxOfferExpiry::SystemWithoutExpiry,
        )
        .await
        .unwrap();
    store
        .record_delivery(
            &refused,
            InboxDecision::Refused {
                reason: InboxRefusalReason::UnknownSession,
            },
            ARRIVAL,
            fixture_expiry(ARRIVAL),
        )
        .await
        .unwrap();
    drop(store);

    let mut reopened = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let rows = reopened
        .scan_direct_handoffs()
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(
        rows.iter()
            .map(|row| (&row.message_id, row.handoff))
            .collect::<Vec<_>>(),
        [
            (&waiting.message_id, DirectHandoff::Waiting),
            (&completed.message_id, DirectHandoff::NotKnown),
        ]
    );
    assert_eq!(rows[0].expires_at, at(ARRIVAL + 900_000));
    assert_eq!(
        reopened
            .message_for(&completed.message_id)
            .await
            .unwrap()
            .expect_clean(),
        Some(completed)
    );
}

#[tokio::test]
async fn handover_batch_compares_every_row_before_writing_any_of_them() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let first = message(1);
    let second = message(2);
    keep(&mut store, &first).await;
    keep(&mut store, &second).await;
    let writing = DirectHandoff::Writing { attempt: attempt() };
    let mut changes = [
        DirectHandoffChange {
            message_id: first.message_id.clone(),
            expected: DirectHandoff::Waiting,
            next: writing,
        },
        DirectHandoffChange {
            message_id: second.message_id.clone(),
            expected: DirectHandoff::NotKnown,
            next: writing,
        },
    ];

    assert_eq!(
        store
            .compare_and_set_direct_handoffs(&first.target, &changes, at(ARRIVAL + 1))
            .await
            .unwrap(),
        HandoffCommit::ChangedSinceRead
    );
    assert_eq!(
        store
            .scan_direct_handoffs()
            .await
            .unwrap()
            .expect_clean()
            .into_iter()
            .map(|row| row.handoff)
            .collect::<Vec<_>>(),
        [DirectHandoff::Waiting, DirectHandoff::Waiting]
    );
    changes[1].expected = DirectHandoff::Waiting;
    let mut another = first.target.clone();
    another.session_id = "different".parse().unwrap();
    assert_eq!(
        store
            .compare_and_set_direct_handoffs(&another, &changes, at(ARRIVAL + 1))
            .await
            .unwrap(),
        HandoffCommit::ChangedSinceRead
    );
    assert!(matches!(
        store.compare_and_set_direct_handoffs(
            &first.target,
            &[changes[0].clone(), changes[0].clone()],
            at(ARRIVAL + 1),
        ).await,
        Err(InboxStoreError::DuplicateHandoffChange { message_id }) if message_id == first.message_id
    ));
    assert_eq!(
        store
            .compare_and_set_direct_handoffs(&first.target, &changes, at(ARRIVAL + 1))
            .await
            .unwrap(),
        HandoffCommit::Committed
    );
    drop(store);
    let mut reopened = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    assert_eq!(
        reopened
            .scan_direct_handoffs()
            .await
            .unwrap()
            .expect_clean()
            .into_iter()
            .map(|row| row.handoff)
            .collect::<Vec<_>>(),
        [writing, writing]
    );
}

#[tokio::test]
async fn a_corrupt_attempt_does_not_hide_another_valid_direct_row() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let valid = message(1);
    let corrupt = message(2);
    keep(&mut store, &valid).await;
    keep(&mut store, &corrupt).await;
    // Canonical spelling and variant, but version 4.
    let not_v7 = Uuid::parse_str("019f0000-0000-4000-8000-000000000011").unwrap();
    let update = serde_json::json!({
        "kind": "direct",
        "state": { "kind": "writing", "attempt": not_v7.hyphenated().to_string() }
    });
    sqlx::query(
        "UPDATE inbox_messages SET delivery_state_json = json_set(delivery_state_json, '$.handoff', json(?)) WHERE message_id = ?"
    )
    .bind(update.to_string())
    .bind(corrupt.message_id.as_str())
    .execute(&mut store.store.connection)
    .await
    .unwrap();

    let scan = store.scan_direct_handoffs().await.unwrap();
    assert_eq!(scan.healthy.len(), 1);
    assert_eq!(scan.healthy[0].message_id, valid.message_id);
    assert_eq!(scan.corrupt_rows.len(), 1);
    assert_eq!(scan.corrupt_rows[0].key, corrupt.message_id.as_str());
    assert_eq!(
        scan.corrupt_rows[0].reason,
        RowCorruptionReason::InvalidJson
    );
}

#[tokio::test]
async fn owed_seen_is_independent_of_delivery_confirmation_and_sixty_day_retention() {
    let root = tempfile::tempdir().unwrap();
    let mut store = InboxStore::new(
        MachineLocalStore::open(&root.path().join("machine_local.sqlite"))
            .await
            .unwrap(),
    );
    let notice = message(1);
    keep(&mut store, &notice).await;
    let completed_at = at(ARRIVAL + 10);
    let seen = DirectHandoff::Seen {
        completed_at,
        receipt: SeenReceiptState::Owed,
    };
    assert_eq!(
        store
            .compare_and_set_direct_handoffs(
                &notice.target,
                &[DirectHandoffChange {
                    message_id: notice.message_id.clone(),
                    expected: DirectHandoff::Waiting,
                    next: seen,
                }],
                completed_at
            )
            .await
            .unwrap(),
        HandoffCommit::Committed
    );
    assert_eq!(
        store.pending_receipts().await.unwrap().expect_clean().len(),
        1
    );
    store
        .mark_receipt_confirmed(&notice.message_id, ARRIVAL + 11)
        .await
        .unwrap();
    assert!(
        store
            .pending_receipts()
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    assert_eq!(
        read_delivery(&mut store.store.connection, &notice.message_id)
            .await
            .unwrap()
            .unwrap()
            .direct_handoff(),
        Some(seen)
    );

    let sixty_days = ARRIVAL + 60 * 24 * 60 * 60 * 1_000;
    assert_eq!(
        store
            .prune_past_retention(sixty_days - 1)
            .await
            .unwrap()
            .expect_clean(),
        0
    );
    assert_eq!(
        store
            .prune_past_retention(sixty_days)
            .await
            .unwrap()
            .expect_clean(),
        1
    );
}
