use super::{MARK, caller, open, uuid_v7};
use crate::{
    MacOutbox, NewOutboxItem, OutboxQueuedWrite, OutboxRequestStatus, OutboxStoreError,
    StoredSenderOutcome,
};
use central_link_protocol::ReplyPayload;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailResult, MessageOutcome, MessageRefusal, WaitingReason,
};

#[tokio::test]
async fn latest_sender_outcome_recovers_acceptance_without_displacing_a_final_answer() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = NewOutboxItem {
        request: uuid_v7(100).parse().unwrap(),
        caller: caller(1),
        payload: OutboxQueuedWrite::SendMessage {
            request_id: uuid_v7(100).parse().unwrap(),
            message: serde_json::from_value(serde_json::json!({
                "to": {"machineId": uuid_v7(101), "sessionId": "recipient"},
                "body": "hello", "delivery": "auto", "replyTo": null
            }))
            .unwrap(),
        },
        flush_at_ms: MARK,
    };
    let pending = StoredSenderOutcome {
        inbox_message_id: uuid_v7(102).parse().unwrap(),
        message_id: uuid_v7(103).parse().unwrap(),
        request_id: write.request.clone(),
        outcome: MessageOutcome::DeliveryNotConfirmed,
        reported_at: chrono::DateTime::from_timestamp_millis(MARK + 1_000).unwrap(),
    };
    let final_outcome = StoredSenderOutcome {
        inbox_message_id: uuid_v7(104).parse().unwrap(),
        outcome: MessageOutcome::NotSeen {
            waiting: WaitingReason::SessionNotOpen,
        },
        reported_at: chrono::DateTime::from_timestamp_millis(MARK + 2_000).unwrap(),
        ..pending.clone()
    };
    let older_final = StoredSenderOutcome {
        inbox_message_id: uuid_v7(105).parse().unwrap(),
        outcome: MessageOutcome::Refused {
            reason: MessageRefusal::UnknownSession,
        },
        reported_at: pending.reported_at,
        ..pending.clone()
    };
    let later_unconfirmed = StoredSenderOutcome {
        inbox_message_id: uuid_v7(106).parse().unwrap(),
        outcome: MessageOutcome::DeliveredSeenNotConfirmed {
            arrived_at: pending.reported_at,
            waiting: None,
        },
        reported_at: chrono::DateTime::from_timestamp_millis(MARK + 3_000).unwrap(),
        ..pending.clone()
    };
    let mut outbox = MacOutbox::new(&mut database);
    outbox.insert(&write, MARK - 1_000).await.unwrap();
    assert!(matches!(
        outbox
            .record_sender_delivery(&write.caller, &pending, MARK)
            .await,
        Err(OutboxStoreError::DeliveryOutcomeMismatch { .. }),
    ));
    outbox
        .mark_sent(std::slice::from_ref(&write.request), MARK)
        .await
        .unwrap();
    assert!(
        !outbox
            .record_sender_delivery(&caller(2), &pending, MARK)
            .await
            .unwrap()
    );
    assert!(
        outbox
            .record_sender_delivery(&write.caller, &pending, MARK)
            .await
            .unwrap()
    );
    assert_eq!(
        outbox
            .request_status(&write.caller, &write.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(ReplyPayload::Mail(CentralMailResult::Done(
            CentralMailAnswer::Queued {
                message_id: pending.message_id.clone()
            }
        ))),
    );
    assert!(
        outbox
            .record_sender_delivery(&write.caller, &final_outcome, MARK)
            .await
            .unwrap()
    );
    assert!(
        outbox
            .record_sender_delivery(&write.caller, &final_outcome, MARK)
            .await
            .unwrap()
    );
    assert!(
        !outbox
            .record_sender_delivery(&write.caller, &older_final, MARK)
            .await
            .unwrap()
    );
    assert!(
        !outbox
            .record_sender_delivery(&write.caller, &later_unconfirmed, MARK)
            .await
            .unwrap()
    );

    database.close().await.unwrap();
    let mut reopened = open(&root).await;
    let mut outbox = MacOutbox::new(&mut reopened);
    assert_eq!(
        outbox
            .sender_outcome(&caller(2), &write.request)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        outbox
            .sender_outcome(&write.caller, &write.request)
            .await
            .unwrap(),
        Some(final_outcome)
    );
}
