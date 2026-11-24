use super::{
    InboxDecision, InboxOfferExpiry, InboxRefusalReason, InboxStore, InboxStoreError,
    RecordDelivery,
};
use crate::{MachineLocalStore, RowCorruptionReason};

#[path = "inbox_corruption_test.rs"]
mod corruption;
#[path = "inbox_expiry_validation_test.rs"]
mod expiry_validation;
#[path = "inbox_retention_test.rs"]
mod retention;
#[path = "inbox_timestamp_test.rs"]
mod timestamps;
use chrono::DateTime;
use hegel::{TestCase, generators as gs};
use router_clock::{ManualClock, RuntimeClock};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use workspace_ids::MessageId;
use workspace_model::{Timestamp, central_mail::MessageRecord};

fn clock_millis(clock: &ManualClock) -> i64 {
    clock.utc_now().timestamp_millis()
}

pub(super) fn fixture_expiry(arrival_ms: i64) -> InboxOfferExpiry {
    InboxOfferExpiry::DirectWithExpiry(
        Timestamp::from_timestamp_millis(arrival_ms.saturating_add(900_000))
            .unwrap_or(Timestamp::UNIX_EPOCH),
    )
}

pub(super) fn message(index: u8) -> MessageRecord {
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
            .record_delivery(&message, decision(index), now, fixture_expiry(now))
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
        for receipt in self.store.pending_receipts().await.unwrap().expect_clean() {
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
        .record_delivery(
            &delivery,
            InboxDecision::Accepted,
            1_000,
            fixture_expiry(1_000),
        )
        .await
        .unwrap();
    drop(store);
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());

    let receipts = store.pending_receipts().await.unwrap().expect_clean();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].message_id, delivery.message_id);
    assert_eq!(receipts[0].decision, InboxDecision::Accepted);
    assert_eq!(receipts[0].received_at_ms, 1_000);
    assert_eq!(
        store.pending_handoffs().await.unwrap().expect_clean(),
        std::slice::from_ref(&delivery)
    );
    store
        .mark_receipt_confirmed(&delivery.message_id, 2_000)
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
        store.pending_handoffs().await.unwrap().expect_clean(),
        [delivery]
    );
    assert_eq!(
        store.next_prune_at_ms().await.unwrap().expect_clean(),
        Some(1_000 + 60 * 24 * 60 * 60 * 1_000)
    );
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
            .record_delivery(&delivery, InboxDecision::Accepted, 0, fixture_expiry(0))
            .await,
        Err(InboxStoreError::Database(_))
    ));
    assert!(
        inbox
            .pending_receipts()
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
    assert!(
        inbox
            .pending_handoffs()
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );

    sqlx::query("DROP TRIGGER reject_first_delivery")
        .execute(&mut fault_connection.connection)
        .await
        .unwrap();
    assert_eq!(
        inbox
            .record_delivery(
                &delivery,
                InboxDecision::Accepted,
                1_000,
                fixture_expiry(1_000)
            )
            .await
            .unwrap(),
        RecordDelivery::Inserted,
    );
    assert_eq!(
        inbox.pending_receipts().await.unwrap().expect_clean()[0].message_id,
        delivery.message_id
    );
    assert_eq!(
        inbox.pending_handoffs().await.unwrap().expect_clean(),
        [delivery]
    );
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
        assert!(
            trial
                .store
                .pending_receipts()
                .await
                .unwrap()
                .expect_clean()
                .is_empty()
        );
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
        let accepted = trial.handoffs.len();
        assert_eq!(
            usize::try_from(retained_payloads).unwrap(),
            accepted,
            "hand-off retains each accepted message"
        );
        assert!(
            trial
                .store
                .pending_handoffs()
                .await
                .unwrap()
                .expect_clean()
                .is_empty()
        );
    });
}

#[tokio::test]
async fn inbox_rejects_unknown_state_and_a_payload_with_the_wrong_message_id() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let now = 0;
    let first = message(0);
    let mut store = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    store
        .record_delivery(&first, InboxDecision::Accepted, now, fixture_expiry(now))
        .await
        .unwrap();

    let mut database = MachineLocalStore::open(&path).await.unwrap();
    sqlx::query("UPDATE inbox_messages SET delivery_state_json = ?")
        .bind(r#"{"kind":"unknown"}"#)
        .execute(&mut database.connection)
        .await
        .unwrap();
    let result = store
        .record_delivery(&first, InboxDecision::Accepted, now, fixture_expiry(now))
        .await;
    assert!(matches!(
        result,
        Err(InboxStoreError::Corrupt {
            field: "delivery_state_json",
            ..
        })
    ));

    sqlx::query("UPDATE inbox_messages SET delivery_state_json = ?, message_json = ?")
        .bind(r#"{"decision":{"kind":"accepted"},"receipt":{"kind":"pending"},"handoff":{"kind":"pending"},"family":"direct"}"#)
        .bind(serde_json::to_string(&message(1)).unwrap())
        .execute(&mut database.connection)
        .await
        .unwrap();
    let scan = store.pending_handoffs().await.unwrap();
    assert!(scan.healthy.is_empty());
    assert_eq!(
        scan.corrupt_rows,
        [crate::RowCorruption {
            key: first.message_id.to_string(),
            field: "message_json.message_id",
            reason: RowCorruptionReason::KeyMismatch,
        }]
    );
}
