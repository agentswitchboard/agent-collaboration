use std::{sync::Arc, time::Duration};

use central_link_protocol::ReceiptResult;
use router_clock::{ClockInstant, RuntimeClock};
use switchboard_machine_core_store::{
    InboxDecision, InboxOfferExpiry, InboxStore, MachineLocalStore,
};
use switchboard_message_custody::{
    custody_task::{CustodyTaskEvent, custody_task},
    receipt_delivery::ReceiptFrameOutcome,
};
use tokio::sync::mpsc;
use workspace_model::{
    Timestamp,
    central_mail::{MessageBody, MessageRecord},
};

use super::{batch_limits::limits_fixture, batch_replay::new_admission, custody_task::ObserveTask};

#[tokio::test]
async fn collecting_an_old_receipt_does_not_consume_the_replacement_mark() {
    let (root, custody, lane, clock) = limits_fixture().await;
    let original = lane.admitted.borrow().clone().unwrap();
    let session = super::inbox_custody::recipient_session();
    let path = root.path().join("machine_local.sqlite");
    let mut observer = InboxStore::new(MachineLocalStore::open(&path).await.unwrap());
    let arrival = clock.utc_now();
    for (id, confirmed) in [
        ("019f0000-0000-7000-8000-000000000181", false),
        ("019f0000-0000-7000-8000-000000000182", true),
    ] {
        let note = MessageRecord::queued_central(
            id.parse().unwrap(),
            session.clone(),
            MessageBody::Notice { items: Vec::new() },
            arrival,
        );
        observer
            .record_delivery(
                &note,
                InboxDecision::Accepted,
                arrival.timestamp_millis(),
                InboxOfferExpiry::SystemWithoutExpiry,
            )
            .await
            .unwrap();
        if confirmed {
            observer
                .mark_receipt_confirmed(&note.message_id, arrival.timestamp_millis())
                .await
                .unwrap();
        }
    }
    // This unrelated, far-future wait witnesses a complete poll of the task's
    // select while the old send slot is still occupied.
    let retention = clock.wall_deadline(runtime_primitives::SyncTime::from_unix_millis(
        observer.next_prune_at_ms().await.unwrap().healthy.unwrap(),
    ));
    let release = lane.hold_next_receipt_completion();
    let mut sent = lane.watch_receipt_commands();
    let (observed, mut events) = mpsc::unbounded_channel();
    let (_calls, group) = custody_task(custody, Arc::new(ObserveTask(observed)));
    let original_mark = ClockInstant::ZERO + Duration::from_secs(20);
    clock.not_due_check_now(original_mark).await;
    clock.advance(Duration::from_secs(20)).unwrap();
    let old = sent
        .wait_for(|sent| sent.len() == 1)
        .await
        .unwrap()
        .first()
        .unwrap()
        .1
        .clone();
    assert_eq!(old.connection, original.connection);

    let mut replacement = new_admission(session.machine_id.clone(), clock.clone());
    replacement.clock_anchor.central_time =
        Timestamp::from_timestamp_millis(clock.utc_now().timestamp_millis() - 1_000).unwrap();
    lane.set_connected(Some(replacement.clone()));
    clock.advance(Duration::from_millis(500)).unwrap();
    clock.not_due_check_now(retention).await;
    clock.advance(Duration::from_millis(500)).unwrap();
    let replacement_mark = original_mark + Duration::from_secs(1);
    assert_eq!(clock.monotonic_now(), replacement_mark);
    clock.not_due_check_now(retention).await;
    release.send(()).unwrap();
    assert!(matches!(
        events.recv().await,
        Some(CustodyTaskEvent::Receipt(
            ReceiptFrameOutcome::StaleCompletion
        ))
    ));

    let following = replacement_mark + Duration::from_secs(60);
    let replacement_receipt = tokio::select! {
        biased;
        submitted = sent.wait_for(|sent| sent.len() == 2) => {
            submitted.unwrap().last().unwrap().clone()
        }
        () = clock.not_due_check_now(following) => {
            panic!("the still-open replacement mark was consumed by the outstanding old receipt");
        }
    };
    assert_eq!(replacement_receipt.0, replacement_mark);
    assert_eq!(replacement_receipt.1.connection, replacement.connection);
    assert_eq!(replacement_receipt.1.receipts, old.receipts);
    assert!(matches!(
        events.recv().await,
        Some(CustodyTaskEvent::Receipt(ReceiptFrameOutcome::Confirmed { results }))
            if results == [ReceiptResult::Recorded]
    ));
    clock.not_due_check_now(following).await;
    assert_eq!(sent.borrow().len(), 2);
    assert!(
        observer
            .pending_receipts()
            .await
            .unwrap()
            .healthy
            .is_empty()
    );
    group.request_stop();
    group.join().await.unwrap();
}
