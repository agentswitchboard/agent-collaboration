use super::{InboxDecision, InboxRefusalReason, InboxStore, InboxStoreError, RecordDelivery};
use crate::MachineLocalStore;
use chrono::DateTime;
use hegel::{TestCase, generators as gs};
use router_clock::{ManualClock, RuntimeClock};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use workspace_ids::MessageId;
use workspace_model::central_mail::MessageRecord;

fn clock_millis(clock: &ManualClock) -> i64 {
    clock.utc_now().timestamp_millis()
}

fn message(index: u8) -> MessageRecord {
    serde_json::from_value(serde_json::json!({
        "messageId": format!("01890f2e-7b4c-7cc0-98c4-{index:012x}"),
        "sender": { "kind": "central" },
        "target": {
            "machineId": "01890f2e-7b4c-7cc0-98c4-000000000002",
            "sessionId": "receiver",
        },
        "body": { "kind": "direct", "body": "hello", "delivery": "auto", "replyTo": null },
        "state": { "kind": "queued" },
        "source": null,
        "createdAt": "2030-01-01T00:00:00Z",
        "updatedAt": "2030-01-01T00:00:00Z",
    }))
    .unwrap()
}

#[derive(Clone, Copy, Debug)]
enum Event {
    Deliver,
    Repeat,
    LoseReceipt,
    Flush,
    Reconnect,
}

struct InboxTrial {
    store: InboxStore,
    clock: ManualClock,
    connected: bool,
    delivered: BTreeSet<MessageId>,
    handoffs: BTreeMap<MessageId, usize>,
    settled: BTreeMap<MessageId, InboxDecision>,
}

impl InboxTrial {
    async fn deliver(&mut self, index: u8) {
        assert!(self.connected);
        let message = message(index);
        let now = clock_millis(&self.clock);
        let first_push = self.delivered.insert(message.message_id.clone());
        let recorded = self
            .store
            .record_delivery(&message, decision(index), now)
            .await
            .unwrap();
        assert_eq!(matches!(recorded, RecordDelivery::Inserted), first_push);
        if recorded == RecordDelivery::Inserted && decision(index) == InboxDecision::Accepted {
            let handoff = self.handoffs.entry(message.message_id.clone()).or_default();
            *handoff += 1;
            self.store
                .record_handoff(&message.message_id, now)
                .await
                .unwrap();
        }
    }

    async fn flush(&mut self, disconnect_before_receipt: bool) {
        self.clock.advance(Duration::from_secs(60)).unwrap();
        let now = clock_millis(&self.clock);
        if !self.connected {
            return;
        }
        for receipt in self.store.pending_receipts().await.unwrap() {
            if disconnect_before_receipt {
                self.connected = false;
                break;
            }
            if let Some(previous) = self
                .settled
                .insert(receipt.message_id.clone(), receipt.decision)
            {
                assert_eq!(
                    previous, receipt.decision,
                    "a replay changes the central decision"
                );
            }
            self.store
                .mark_receipt_confirmed(&receipt.message_id, now)
                .await
                .unwrap();
        }
    }

    async fn reconnect(&mut self) {
        self.connected = true;
        for index in [1, 3] {
            let id = message(index).message_id;
            if self.delivered.contains(&id) && !self.settled.contains_key(&id) {
                self.deliver(index).await;
            }
        }
    }
}

#[tokio::test]
async fn committed_inbox_receipt_survives_reopen_without_waiting_for_handoff() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let delivery = message(0);

    store
        .record_delivery(&delivery, InboxDecision::Accepted, 1_000)
        .await
        .unwrap();
    drop(store);
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());

    let receipts = store.pending_receipts().await.unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].message_id, delivery.message_id);
    assert_eq!(receipts[0].decision, InboxDecision::Accepted);
    assert_eq!(receipts[0].received_at_ms, 1_000);
    assert_eq!(
        store.pending_handoffs().await.unwrap(),
        std::slice::from_ref(&delivery)
    );
    store
        .mark_receipt_confirmed(&delivery.message_id, 2_000)
        .await
        .unwrap();
    assert!(store.pending_receipts().await.unwrap().is_empty());
    assert_eq!(store.pending_handoffs().await.unwrap(), [delivery]);
    assert_eq!(store.next_prune_at_ms().await.unwrap(), None);
}

#[tokio::test]
async fn failed_insert_has_no_pending_receipt_and_a_retry_keeps_the_payload() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let mut fault_connection = MachineLocalStore::open(&path).await.unwrap();
    sqlx::query(
        "CREATE TRIGGER reject_first_delivery BEFORE INSERT ON inbox_messages
         BEGIN SELECT RAISE(ABORT, 'injected admission failure'); END",
    )
    .execute(&mut fault_connection.connection)
    .await
    .unwrap();
    let delivery = message(0);

    assert!(matches!(
        inbox
            .record_delivery(&delivery, InboxDecision::Accepted, 0)
            .await,
        Err(InboxStoreError::Database(_))
    ));
    assert!(inbox.pending_receipts().await.unwrap().is_empty());
    assert!(inbox.pending_handoffs().await.unwrap().is_empty());

    sqlx::query("DROP TRIGGER reject_first_delivery")
        .execute(&mut fault_connection.connection)
        .await
        .unwrap();
    assert_eq!(
        inbox
            .record_delivery(&delivery, InboxDecision::Accepted, 1_000)
            .await
            .unwrap(),
        RecordDelivery::Inserted,
    );
    assert_eq!(
        inbox.pending_receipts().await.unwrap()[0].message_id,
        delivery.message_id
    );
    assert_eq!(inbox.pending_handoffs().await.unwrap(), [delivery]);
}

fn decision(index: u8) -> InboxDecision {
    if index.is_multiple_of(2) {
        InboxDecision::Accepted
    } else {
        InboxDecision::Refused {
            reason: if index == 1 {
                InboxRefusalReason::UnknownSession
            } else {
                InboxRefusalReason::AmbiguousSession
            },
        }
    }
}

#[hegel::test(test_cases = 48)]
fn unconfirmed_receipt_replays_reach_one_settlement_without_repeating_handoff(tc: TestCase) {
    let history = tc.draw(
        gs::vecs(gs::tuples!(
            gs::sampled_from(vec![
                Event::Deliver,
                Event::Repeat,
                Event::LoseReceipt,
                Event::Flush,
                Event::Reconnect,
            ]),
            gs::integers::<u8>().max_value(3)
        ))
        .max_size(20),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("machine_local.sqlite");
        let mut trial = InboxTrial {
            store: InboxStore::new(MachineLocalStore::open(&path).await.unwrap()),
            clock: ManualClock::new(DateTime::UNIX_EPOCH),
            connected: true,
            delivered: BTreeSet::new(),
            handoffs: BTreeMap::new(),
            settled: BTreeMap::new(),
        };
        trial.deliver(0).await;
        trial.flush(true).await;
        assert!(trial.settled.is_empty());
        // Read the deduplication state through a freshly opened SQLite handle.
        trial.store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
        trial.reconnect().await;
        trial.deliver(0).await;
        trial.flush(false).await;
        assert_eq!(
            trial.settled.get(&message(0).message_id),
            Some(&decision(0))
        );
        for (event, index) in history {
            match event {
                Event::Deliver if trial.connected => trial.deliver(index).await,
                Event::Repeat
                    if trial.connected && trial.delivered.contains(&message(index).message_id) =>
                {
                    trial.deliver(index).await;
                }
                Event::LoseReceipt => trial.flush(true).await,
                Event::Flush => trial.flush(false).await,
                Event::Reconnect => trial.reconnect().await,
                Event::Deliver | Event::Repeat => {}
            }
        }
        trial.reconnect().await;
        trial.flush(false).await;
        assert!(trial.store.pending_receipts().await.unwrap().is_empty());
        assert_eq!(trial.settled.len(), trial.delivered.len());
        for index in 0..4 {
            let id = message(index).message_id;
            if trial.delivered.contains(&id) {
                let expected_handoffs = if decision(index) == InboxDecision::Accepted {
                    Some(&1)
                } else {
                    None
                };
                assert_eq!(trial.handoffs.get(&id), expected_handoffs);
                assert_eq!(trial.settled.get(&id), Some(&decision(index)));
            }
        }
        let mut database = MachineLocalStore::open(&path).await.unwrap();
        let (rows, retained_payloads): (i64, i64) =
            sqlx::query_as("SELECT count(*), count(message_json) FROM inbox_messages")
                .fetch_one(&mut database.connection)
                .await
                .unwrap();
        assert_eq!(usize::try_from(rows).unwrap(), trial.delivered.len());
        assert_eq!(retained_payloads, 0);
    });
}

#[tokio::test]
async fn inbox_prunes_only_completed_decisions_older_than_retention() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = ManualClock::new(DateTime::UNIX_EPOCH);
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let now = clock_millis(&clock);
    for index in 0..3 {
        assert_eq!(
            store
                .record_delivery(&message(index), InboxDecision::Accepted, now)
                .await
                .unwrap(),
            RecordDelivery::Inserted,
        );
    }
    for index in 0..2 {
        store
            .record_handoff(&message(index).message_id, now)
            .await
            .unwrap();
    }
    clock.advance(Duration::from_secs(60)).unwrap();
    let confirmed_at = clock_millis(&clock);
    let oldest = message(0);
    store
        .mark_receipt_confirmed(&oldest.message_id, confirmed_at)
        .await
        .unwrap();

    clock.advance(Duration::from_secs(24 * 60 * 60)).unwrap();
    let boundary = clock_millis(&clock);
    assert_eq!(store.next_prune_at_ms().await.unwrap(), Some(boundary + 1));
    store.prune_ended(boundary).await.unwrap();
    assert_eq!(
        store
            .record_delivery(&oldest, InboxDecision::Accepted, boundary)
            .await
            .unwrap(),
        RecordDelivery::Duplicate,
        "the exact 24-hour boundary still retains the decision",
    );
    assert_eq!(
        store
            .receipt_for(&oldest.message_id)
            .await
            .unwrap()
            .unwrap()
            .received_at_ms,
        now,
    );
    assert_eq!(store.pending_receipts().await.unwrap().len(), 2);

    clock.advance(Duration::from_millis(1)).unwrap();
    let after_boundary = clock_millis(&clock);
    store.prune_ended(after_boundary).await.unwrap();
    assert_eq!(store.receipt_for(&oldest.message_id).await.unwrap(), None);
    assert_eq!(store.next_prune_at_ms().await.unwrap(), None);

    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT message_id FROM inbox_messages ORDER BY message_id")
            .fetch_all(&mut database.connection)
            .await
            .unwrap();
    assert_eq!(
        ids,
        [
            message(1).message_id.as_str(),
            message(2).message_id.as_str()
        ]
    );
    assert_eq!(
        store
            .record_delivery(&oldest, InboxDecision::Accepted, after_boundary)
            .await
            .unwrap(),
        RecordDelivery::Inserted,
        "after pruning the same message ID is new to the store",
    );
    assert_eq!(
        store.pending_handoffs().await.unwrap(),
        [message(2), oldest]
    );
}

#[tokio::test]
async fn inbox_rejects_unknown_state_and_a_payload_with_the_wrong_message_id() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let now = 0;
    let first = message(0);
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    store
        .record_delivery(&first, InboxDecision::Accepted, now)
        .await
        .unwrap();

    let mut database = MachineLocalStore::open(&path).await.unwrap();
    sqlx::query("UPDATE inbox_messages SET delivery_state_json = ?")
        .bind(r#"{"kind":"unknown"}"#)
        .execute(&mut database.connection)
        .await
        .unwrap();
    let result = store
        .record_delivery(&first, InboxDecision::Accepted, now)
        .await;
    assert!(matches!(
        result,
        Err(InboxStoreError::Corrupt {
            field: "delivery_state_json",
            ..
        })
    ));

    sqlx::query("UPDATE inbox_messages SET delivery_state_json = ?, message_json = ?")
        .bind(r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"ended_at_ms":null}"#)
        .bind(serde_json::to_string(&message(1)).unwrap())
        .execute(&mut database.connection)
        .await
        .unwrap();
    let result = store.pending_handoffs().await;
    assert!(matches!(
        result,
        Err(InboxStoreError::Corrupt {
            field: "message_json.message_id",
            ..
        })
    ));
}

#[tokio::test]
async fn corrupt_completion_marker_cannot_prune_a_message_with_work_owed() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut database = MachineLocalStore::open(&path).await.unwrap();
    let event = message(0);
    let body = serde_json::to_string(&event).unwrap();
    let state = r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"ended_at_ms":0}"#;
    sqlx::query(
        "INSERT INTO inbox_messages (message_id, message_json, delivery_state_json, created_at, updated_at)
         VALUES (?, ?, ?, 0, 0)",
    ).bind(event.message_id.as_str()).bind(&body).bind(state).execute(&mut database.connection).await.unwrap();
    let mut inbox = InboxStore::new(database);

    for result in [
        inbox.next_prune_at_ms().await.map(|_| ()),
        inbox.prune_ended(24 * 60 * 60 * 1000 + 1).await,
    ] {
        assert!(matches!(
            result,
            Err(InboxStoreError::Corrupt {
                field: "delivery_state_json.ended_at_ms",
                ..
            })
        ));
    }
    let mut reopened = MachineLocalStore::open(&path).await.unwrap();
    let retained: (String, String) =
        sqlx::query_as("SELECT message_json, delivery_state_json FROM inbox_messages")
            .fetch_one(&mut reopened.connection)
            .await
            .unwrap();
    assert_eq!(retained, (body, state.to_owned()));
}
