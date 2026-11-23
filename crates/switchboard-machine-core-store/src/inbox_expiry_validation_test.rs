use super::{
    InboxDecision, InboxOfferExpiry, InboxRefusalReason, InboxStore, InboxStoreError,
    MachineLocalStore, message,
};
use crate::{MAX_INBOX_DELIVERY_WINDOW_MS, RowCorruption, RowCorruptionReason, StoreScan};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

const ARRIVAL_MS: i64 = 50_000;
const DECISIONS: [InboxDecision; 2] = [
    InboxDecision::Accepted,
    InboxDecision::Refused {
        reason: InboxRefusalReason::UnknownSession,
    },
];

fn at(millis: i64) -> Timestamp {
    Timestamp::from_timestamp_millis(millis).unwrap()
}

fn system_message(index: u8) -> MessageRecord {
    let mut system = message(index);
    system.body = MessageBody::Notice { items: Vec::new() };
    system
}

#[tokio::test]
async fn invalid_offer_families_and_expiry_windows_fail_on_first_insert_for_both_decisions() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let direct = message(1);
    let system = system_message(2);
    for decision in DECISIONS {
        let non_direct = InboxOfferExpiry::SystemWithoutExpiry;
        let valid_direct = InboxOfferExpiry::DirectWithExpiry(at(ARRIVAL_MS + 1));
        for (offer, expiry) in [(&direct, non_direct), (&system, valid_direct)] {
            let result = inbox
                .record_delivery(offer, decision, ARRIVAL_MS, expiry)
                .await;
            assert!(
                matches!(result, Err(InboxStoreError::OfferFamilyMismatch)),
                "write-time family must reject either kind mismatch, including a missing direct expiry: {decision:?}, {result:?}"
            );
        }
        for invalid_delta in [0, -1, i64::from(MAX_INBOX_DELIVERY_WINDOW_MS) + 1] {
            let expiry = InboxOfferExpiry::DirectWithExpiry(at(ARRIVAL_MS + invalid_delta));
            let result = inbox
                .record_delivery(&direct, decision, ARRIVAL_MS, expiry)
                .await;
            assert!(
                matches!(result, Err(InboxStoreError::InvalidExpiry)),
                "write-time expiry must reject delta {invalid_delta} for {decision:?}: {result:?}"
            );
        }
    }
    assert!(
        inbox
            .pending_receipts()
            .await
            .unwrap()
            .expect_clean()
            .is_empty()
    );
}

async fn replace_expiry(
    database: &mut MachineLocalStore,
    event: &MessageRecord,
    value: Option<i64>,
) {
    sqlx::query("UPDATE inbox_messages SET expires_at = ? WHERE message_id = ?")
        .bind(value)
        .bind(event.message_id.as_str())
        .execute(&mut database.connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn impossible_expiry_is_corrupt_for_accepted_and_refused_direct_and_system_rows() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut raw = MachineLocalStore::open(&path).await.unwrap();
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    for (offset, decision) in DECISIONS.into_iter().enumerate() {
        let index = u8::try_from(offset).unwrap();
        let direct = message(index);
        let system = system_message(index + 2);
        let max_valid = ARRIVAL_MS + i64::from(MAX_INBOX_DELIVERY_WINDOW_MS);
        inbox
            .record_delivery(
                &direct,
                decision,
                ARRIVAL_MS,
                InboxOfferExpiry::DirectWithExpiry(at(max_valid)),
            )
            .await
            .unwrap();
        inbox
            .record_delivery(
                &system,
                decision,
                ARRIVAL_MS,
                InboxOfferExpiry::SystemWithoutExpiry,
            )
            .await
            .unwrap();
        assert_eq!(
            inbox
                .expiry_for(&direct.message_id)
                .await
                .unwrap()
                .expect_clean(),
            Some(at(max_valid))
        );
        for invalid in [
            None,
            Some(ARRIVAL_MS),
            Some(ARRIVAL_MS - 1),
            Some(max_valid + 1),
        ] {
            replace_expiry(&mut raw, &direct, invalid).await;
            assert_eq!(
                inbox.expiry_for(&direct.message_id).await.unwrap(),
                corrupt_expiry(&direct),
                "stored direct expiry {invalid:?} must fail for {decision:?}"
            );
        }
        replace_expiry(&mut raw, &system, Some(ARRIVAL_MS + 1)).await;
        assert_eq!(
            inbox.expiry_for(&system.message_id).await.unwrap(),
            corrupt_expiry(&system),
            "stored system expiry must be absent for {decision:?}"
        );
    }
    let receipts = inbox.pending_receipts().await.unwrap();
    assert_eq!(receipts.corrupt_rows.len(), 4);
    assert!(receipts.healthy.is_empty());
}

fn corrupt_expiry(event: &MessageRecord) -> StoreScan<Option<Timestamp>> {
    StoreScan {
        healthy: None,
        corrupt_rows: vec![RowCorruption {
            key: event.message_id.to_string(),
            field: "expires_at",
            reason: RowCorruptionReason::InconsistentState,
        }],
    }
}

#[tokio::test]
async fn a_retained_payload_with_the_wrong_family_is_reported_by_field() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut raw = MachineLocalStore::open(&path).await.unwrap();
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let direct = message(1);
    inbox
        .record_delivery(
            &direct,
            InboxDecision::Accepted,
            ARRIVAL_MS,
            InboxOfferExpiry::DirectWithExpiry(at(ARRIVAL_MS + 1)),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE inbox_messages SET message_json = ? WHERE message_id = ?")
        .bind(serde_json::to_string(&system_message(1)).unwrap())
        .bind(direct.message_id.as_str())
        .execute(&mut raw.connection)
        .await
        .unwrap();
    assert_eq!(
        inbox.expiry_for(&direct.message_id).await.unwrap(),
        StoreScan {
            healthy: None,
            corrupt_rows: vec![RowCorruption {
                key: direct.message_id.to_string(),
                field: "message_json.body.kind",
                reason: RowCorruptionReason::InconsistentState,
            }],
        },
        "the kept payload kind must agree with the original offer family"
    );
}

#[tokio::test]
async fn historical_payloads_are_skipped_until_a_confirmed_row_is_due() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let mut raw = MachineLocalStore::open(&path).await.unwrap();
    let mut inbox = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let direct = message(1);
    inbox
        .record_delivery(
            &direct,
            InboxDecision::Accepted,
            ARRIVAL_MS,
            InboxOfferExpiry::DirectWithExpiry(at(ARRIVAL_MS + 1)),
        )
        .await
        .unwrap();
    inbox
        .record_handoff(&direct.message_id, ARRIVAL_MS)
        .await
        .unwrap();
    inbox
        .mark_receipt_confirmed(&direct.message_id, ARRIVAL_MS)
        .await
        .unwrap();
    sqlx::query("UPDATE inbox_messages SET message_json = '{' WHERE message_id = ?")
        .bind(direct.message_id.as_str())
        .execute(&mut raw.connection)
        .await
        .unwrap();

    assert_eq!(
        inbox.pending_handoffs().await.unwrap(),
        StoreScan {
            healthy: Vec::new(),
            corrupt_rows: Vec::new()
        },
        "handoff polling must not decode a completed body"
    );
    let cutoff = ARRIVAL_MS + 60 * 24 * 60 * 60 * 1_000;
    let wake = inbox.next_prune_at_ms().await.unwrap();
    assert!(
        wake.corrupt_rows.is_empty(),
        "retention scheduling must not decode historical bodies"
    );
    assert_eq!(wake.healthy, Some(cutoff));
    assert_eq!(
        inbox
            .prune_past_retention(cutoff - 1)
            .await
            .unwrap()
            .expect_clean(),
        0
    );
    let prune = inbox.prune_past_retention(cutoff).await.unwrap();
    assert_eq!(prune.healthy, 0);
    assert_eq!(prune.corrupt_rows[0].field, "message_json");
    assert_eq!(
        inbox.next_prune_at_ms().await.unwrap().expect_clean(),
        None,
        "a damaged due-row payload must not rearm an already elapsed retention wake"
    );
}
