#![expect(
    clippy::unwrap_used,
    reason = "An invalid fixture cannot exercise the inbox's stored expiry."
)]

use central_link_client::ValidatedDeliveryOffer;
use central_link_protocol::{DeliveryMessage, TimeLeft};
use router_clock::{ManualClock, RuntimeClock};
use std::{
    future::{Future, ready},
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use switchboard_machine_core_store::{
    InboxDecision, InboxOfferExpiry, InboxStore, MachineLocalStore, RecordDelivery,
};
use switchboard_message_custody::{
    recipient_inbox::{InboxOfferResult, RecipientInbox},
    recipient_sessions::{RecipientSessionAnswer, RecipientSessionDirectory},
};
use workspace_ids::{MessageId, SessionId};
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageOutcome, MessageRecord, MessageRefusal},
};

use super::{
    batch_replay::KnownRecipient,
    inbox_custody::{direct_offer, recipient_session},
    outbox_timing::manual_clock,
};

const ARRIVAL_MS: i64 = 1_763_380_800_123;
const DIRECT_MESSAGE_ID: &str = "019f0000-0000-7000-8000-000000000c41";
const TIME_LEFT_MS: u32 = 123_456;

#[test]
fn the_store_and_wire_agree_on_the_longest_delivery_window() {
    assert_eq!(
        switchboard_machine_core_store::MAX_INBOX_DELIVERY_WINDOW_MS,
        central_link_protocol::MAX_TIME_LEFT_MS,
        "the store and wire must agree on the longest possible delivery window"
    );
}

#[derive(Default)]
struct DeferredOnce(AtomicBool);

impl RecipientSessionDirectory for DeferredOnce {
    fn query_recipient_session<'lookup>(
        &'lookup self,
        _: &'lookup SessionId,
    ) -> Pin<Box<dyn Future<Output = RecipientSessionAnswer> + Send + 'lookup>> {
        let answer = if self.0.swap(true, Ordering::SeqCst) {
            RecipientSessionAnswer::KnownOnce
        } else {
            RecipientSessionAnswer::CouldNotTell
        };
        Box::pin(ready(answer))
    }
}

async fn open_store(path: &Path) -> InboxStore {
    InboxStore::new(MachineLocalStore::open(path).await.unwrap())
}

async fn open_recipient(
    path: &Path,
    clock: Arc<ManualClock>,
    directory: Arc<dyn RecipientSessionDirectory>,
) -> RecipientInbox {
    RecipientInbox::new(
        open_store(path).await,
        recipient_session().machine_id,
        directory,
        clock,
    )
}

fn offer_with_time_left(body: &str, remaining_ms: u32) -> ValidatedDeliveryOffer {
    let (message, _) = direct_offer(DIRECT_MESSAGE_ID, body).into_parts();
    ValidatedDeliveryOffer::new(message, Some(TimeLeft::try_from(remaining_ms).unwrap())).unwrap()
}

fn first_expiry() -> Timestamp {
    Timestamp::from_timestamp_millis(ARRIVAL_MS + i64::from(TIME_LEFT_MS)).unwrap()
}

#[tokio::test]
async fn first_direct_save_uses_the_original_arrival_plus_centrals_time_left() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(ARRIVAL_MS);
    let mut inbox = open_recipient(&path, clock.clone(), Arc::new(DeferredOnce::default())).await;
    let message_id: MessageId = DIRECT_MESSAGE_ID.parse().unwrap();

    assert_eq!(
        inbox
            .receive_offer(
                offer_with_time_left("delay the first save", TIME_LEFT_MS),
                clock.utc_now()
            )
            .await
            .unwrap(),
        InboxOfferResult::LookupDeferred
    );
    let mut reader = open_store(&path).await;
    assert!(
        reader
            .receipt_for(&message_id)
            .await
            .unwrap()
            .healthy
            .is_none()
    );
    clock.advance(Duration::from_secs(15)).unwrap();
    assert!(inbox.retry_uncommitted().await.is_empty());

    let receipt = reader
        .receipt_for(&message_id)
        .await
        .unwrap()
        .healthy
        .unwrap();
    assert_eq!(receipt.received_at_ms, ARRIVAL_MS);
    assert_eq!(
        reader.expiry_for(&message_id).await.unwrap().healthy,
        Some(first_expiry()),
        "the first save must use original arrival plus Central's time left"
    );
}

#[tokio::test]
async fn a_repeated_direct_offer_does_not_move_its_first_expiry() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(ARRIVAL_MS);
    let mut inbox = open_recipient(&path, clock.clone(), Arc::new(KnownRecipient)).await;
    let message_id: MessageId = DIRECT_MESSAGE_ID.parse().unwrap();

    inbox
        .receive_offer(
            offer_with_time_left("first offer", TIME_LEFT_MS),
            clock.utc_now(),
        )
        .await
        .unwrap();
    clock.advance(Duration::from_secs(29)).unwrap();
    assert_eq!(
        inbox
            .receive_offer(
                offer_with_time_left("repeat offer", 54_321),
                clock.utc_now()
            )
            .await
            .unwrap(),
        InboxOfferResult::AlreadyDecided {
            decision: InboxDecision::Accepted
        }
    );
    let mut reader = open_store(&path).await;
    let first_message = reader
        .message_for(&message_id)
        .await
        .unwrap()
        .healthy
        .unwrap();
    assert_eq!(
        reader
            .record_delivery(
                &first_message,
                InboxDecision::Accepted,
                ARRIVAL_MS + 29_000,
                InboxOfferExpiry::DirectWithExpiry(
                    Timestamp::from_timestamp_millis(ARRIVAL_MS + 29_000 + 54_321).unwrap()
                ),
            )
            .await
            .unwrap(),
        RecordDelivery::Duplicate,
    );
    let found = reader.expiry_for(&message_id).await.unwrap();
    assert!(found.corrupt_rows.is_empty());
    assert_eq!(
        found.healthy,
        Some(first_expiry()),
        "a repeated offer must retain the first expiry"
    );
}

#[tokio::test]
async fn system_notices_and_outcomes_are_saved_with_null_expiry() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(ARRIVAL_MS);
    let mut inbox = open_recipient(&path, clock.clone(), Arc::new(KnownRecipient)).await;
    let mut reader = open_store(&path).await;
    for (key, body) in [
        (
            "019f0000-0000-7000-8000-000000000c42",
            MessageBody::Notice { items: Vec::new() },
        ),
        (
            "019f0000-0000-7000-8000-000000000c43",
            MessageBody::DeliveryOutcome {
                message_id: DIRECT_MESSAGE_ID.parse().unwrap(),
                request_id: "019f0000-0000-7000-8000-000000000c44".parse().unwrap(),
                outcome: MessageOutcome::Refused {
                    reason: MessageRefusal::UnknownSession,
                },
            },
        ),
    ] {
        let id: MessageId = key.parse().unwrap();
        let notice = MessageRecord::queued_central(
            id.clone(),
            recipient_session(),
            body,
            Timestamp::from_timestamp_millis(ARRIVAL_MS).unwrap(),
        );
        let offer =
            ValidatedDeliveryOffer::new(Box::new(DeliveryMessage::System(notice)), None).unwrap();
        assert_eq!(
            inbox.receive_offer(offer, clock.utc_now()).await.unwrap(),
            InboxOfferResult::Recorded {
                decision: InboxDecision::Accepted
            }
        );
        assert!(reader.message_for(&id).await.unwrap().healthy.is_some());
        let found = reader.expiry_for(&id).await.unwrap();
        assert!(found.corrupt_rows.is_empty());
        assert_eq!(found.healthy, None, "system offers must store NULL expiry");
    }
}

#[tokio::test]
async fn the_first_direct_expiry_survives_reopening_the_store() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("machine_local.sqlite");
    let clock = manual_clock(ARRIVAL_MS);
    let mut inbox = open_recipient(&path, clock.clone(), Arc::new(KnownRecipient)).await;
    inbox
        .receive_offer(
            offer_with_time_left("persist the cutoff", TIME_LEFT_MS),
            clock.utc_now(),
        )
        .await
        .unwrap();
    drop(inbox);

    let mut reopened = open_store(&path).await;
    let id = DIRECT_MESSAGE_ID.parse().unwrap();
    let found = reopened.expiry_for(&id).await.unwrap();
    assert!(found.corrupt_rows.is_empty());
    assert_eq!(
        found.healthy,
        Some(first_expiry()),
        "the typed expiry must survive closing and reopening SQLite"
    );
}
