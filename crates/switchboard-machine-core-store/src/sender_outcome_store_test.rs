use super::{MARK, caller, open, uuid_v7};
use crate::{
    MacOutbox, NewOutboxItem, OutboxAccepted, OutboxQueuedWrite, OutboxRequestStatus,
    OutboxStoreError, StoredSenderOutcome,
};
use workspace_model::central_mail::{MessageOutcome, MessageRefusal, WaitingReason};

pub(super) fn direct_write(seed: u16) -> NewOutboxItem {
    NewOutboxItem {
        request: uuid_v7(seed).parse().unwrap(),
        caller: caller(1),
        payload: OutboxQueuedWrite::SendMessage {
            request_id: uuid_v7(seed).parse().unwrap(),
            message: serde_json::from_value(serde_json::json!({
                "to": {"machineId": uuid_v7(101), "sessionId": "recipient"},
                "body": "hello", "delivery": "auto", "replyTo": null
            }))
            .unwrap(),
        },
        flush_at_ms: MARK,
    }
}

#[tokio::test]
async fn first_final_outcome_survives_contradictory_and_replayed_notices() {
    let root = tempfile::tempdir().unwrap();
    let mut database = open(&root).await;
    let write = direct_write(100);
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
        reported_at: chrono::DateTime::from_timestamp_millis(MARK + 500).unwrap(),
        ..pending.clone()
    };
    let contradictory_final = StoredSenderOutcome {
        inbox_message_id: uuid_v7(105).parse().unwrap(),
        outcome: MessageOutcome::Refused {
            reason: MessageRefusal::UnknownSession,
        },
        ..final_outcome.clone()
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
        OutboxRequestStatus::Answered(OutboxAccepted::SendMessage {
            message_id: pending.message_id.clone()
        }),
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
    for reported_at in [final_outcome.reported_at, later_unconfirmed.reported_at] {
        let conflicting = StoredSenderOutcome {
            reported_at,
            ..contradictory_final.clone()
        };
        assert!(matches!(
            outbox.record_sender_delivery(&write.caller, &conflicting, MARK + 10_000).await,
            Err(OutboxStoreError::ContradictoryFinalOutcome { request }) if request == write.request
        ));
    }
    let changed_same_id = StoredSenderOutcome {
        inbox_message_id: final_outcome.inbox_message_id.clone(),
        ..contradictory_final
    };
    assert!(matches!(
        outbox
            .record_sender_delivery(&write.caller, &changed_same_id, MARK)
            .await,
        Err(OutboxStoreError::DeliveryOutcomeMismatch { .. })
    ));
    assert!(
        !outbox
            .record_sender_delivery(&write.caller, &later_unconfirmed, MARK)
            .await
            .unwrap()
    );

    let stamp: i64 = sqlx::query_scalar("SELECT updated_at FROM outbox_items")
        .fetch_one(&mut database.connection)
        .await
        .unwrap();
    assert_eq!(stamp, MARK);
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

#[tokio::test]
async fn late_outcome_recovers_only_the_expired_request_that_may_have_left() {
    use crate::OutboxCancellation;
    const START: i64 = MARK - 1_000;
    const DEADLINE: i64 = START + 15 * 60 * 1_000;
    let root = tempfile::tempdir().unwrap();
    let claimed = direct_write(125);
    let never_sent = direct_write(126);
    let mut database = open(&root).await;
    let mut outbox = MacOutbox::new(&mut database);
    for write in [&claimed, &never_sent] {
        outbox.insert(write, START).await.unwrap();
    }
    let batch = outbox
        .claim_next_batch(MARK, 1, MARK, MARK)
        .await
        .unwrap()
        .expect_clean();
    assert_eq!(batch.writes[0].request, claimed.request);
    outbox
        .return_unanswered(std::slice::from_ref(&claimed.request), DEADLINE)
        .await
        .unwrap()
        .expect_clean();
    outbox.end_overdue(DEADLINE).await.unwrap().expect_clean();
    database.close().await.unwrap();

    let mut reopened = open(&root).await;
    let mut store = MacOutbox::new(&mut reopened);
    assert_eq!(
        store
            .request_status(&claimed.caller, &claimed.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotConfirmedWithin15Minutes
        }
    );
    assert_eq!(
        store
            .request_status(&never_sent.caller, &never_sent.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotSentWithin15Minutes
        }
    );

    let notice = StoredSenderOutcome {
        inbox_message_id: uuid_v7(127).parse().unwrap(),
        message_id: uuid_v7(128).parse().unwrap(),
        request_id: claimed.request.clone(),
        outcome: MessageOutcome::NotSeen {
            waiting: WaitingReason::Busy,
        },
        reported_at: chrono::DateTime::from_timestamp_millis(DEADLINE + 1).unwrap(),
    };
    for now in [DEADLINE + 1, DEADLINE + 2] {
        assert!(
            store
                .record_sender_delivery(&claimed.caller, &notice, now)
                .await
                .unwrap()
        );
    }
    let provisional = StoredSenderOutcome {
        inbox_message_id: uuid_v7(129).parse().unwrap(),
        reported_at: chrono::DateTime::from_timestamp_millis(DEADLINE + 3).unwrap(),
        outcome: MessageOutcome::DeliveryNotConfirmed,
        ..notice.clone()
    };
    assert!(
        !store
            .record_sender_delivery(&claimed.caller, &provisional, DEADLINE + 3)
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .sender_outcome(&claimed.caller, &claimed.request)
            .await
            .unwrap(),
        Some(notice.clone())
    );
    assert_eq!(
        store
            .request_status(&claimed.caller, &claimed.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Answered(OutboxAccepted::SendMessage {
            message_id: notice.message_id.clone()
        })
    );

    let false_notice = StoredSenderOutcome {
        request_id: never_sent.request.clone(),
        ..notice
    };
    assert!(matches!(
        store
            .record_sender_delivery(&never_sent.caller, &false_notice, DEADLINE + 1)
            .await,
        Err(OutboxStoreError::DeliveryOutcomeMismatch { .. })
    ));
    assert_eq!(
        store
            .request_status(&never_sent.caller, &never_sent.request)
            .await
            .unwrap(),
        OutboxRequestStatus::Ended {
            reason: OutboxCancellation::NotSentWithin15Minutes
        }
    );
}
