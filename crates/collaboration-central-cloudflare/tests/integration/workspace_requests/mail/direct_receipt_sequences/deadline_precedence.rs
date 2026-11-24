use super::*;
#[tokio::test]
async fn overdue_receipt_or_status_settles_before_later_evidence() {
    for (initial_receipt, touching_status) in [(false, false), (true, false), (false, true)] {
        let fixture = Fixture::new().await;
        let central = runtime(&fixture).await;
        let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
        let mark = fleet_mail::message_deadline::direct_receipt_mark(now, now).unwrap();
        let message_id = queue_direct(&fixture, now, 981020).await;
        let store = central.statement_host();
        if initial_receipt {
            assert_eq!(
                apply_fixture_receipt(
                    &fixture,
                    &central,
                    &message_id,
                    ReceiptKind::Delivered { arrived_at: now }
                )
                .await,
                MailReceiptSettlement::Applied
            );
        }
        let due_count = || Statement {
            sql: "SELECT count(*) FROM engine_due_entries WHERE work_kind='direct_message' AND entry_role='item' AND subject_key=?",
            args: vec![text(&message_id)],
        };
        let later = mark + chrono::Duration::seconds(1);
        set_receipt_clock(&fixture, &central, later.timestamp_millis()).await;
        let initial_state = if initial_receipt {
            "delivered"
        } else {
            "queued"
        };
        assert_store(&central, &message_id, initial_state, None).await;
        assert_eq!(store.read(due_count()).await.unwrap().rows, [[integer(1)]]);
        let caller = fresh_auth(&fixture.host, &fixture.caller);
        if touching_status {
            let status = fixture
                .host
                .request(&caller, status_query(message_id.clone()))
                .await
                .unwrap()
                .result;
            assert_eq!(
                status,
                status_answer(MessageStatus::Sent {
                    deadline_passed: true
                })
            );
            assert_store(
                &central,
                &message_id,
                "queued",
                Some("deliveryNotConfirmed"),
            )
            .await;
        }
        let is_refusal = initial_receipt || touching_status;
        let late_receipt = if is_refusal {
            ReceiptKind::Refused {
                reason: MessageRefusal::AmbiguousSession,
            }
        } else {
            ReceiptKind::Seen {
                completed_at: later,
            }
        };
        let settlement = apply_fixture_receipt(&fixture, &central, &message_id, late_receipt).await;
        assert_eq!(settlement, MailReceiptSettlement::Applied);
        let state = if is_refusal { "refused" } else { "seen" };
        let outcome = if initial_receipt {
            "deliveredSeenNotConfirmed"
        } else {
            "deliveryNotConfirmed"
        };
        assert_store(&central, &message_id, state, Some(outcome)).await;
        assert_eq!(store.read(due_count()).await.unwrap().rows, [[integer(0)]]);
        let next_mark = mark + chrono::Duration::minutes(5);
        set_receipt_clock(&fixture, &central, next_mark.timestamp_millis()).await;
        central.after_event().await.unwrap();
        assert_store(&central, &message_id, state, Some(outcome)).await;
        fixture.host.close().await;
    }
}

#[tokio::test]
async fn corrupt_deadline_candidates_reach_typed_owners_and_never_settle() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let sent_at = now - chrono::Duration::seconds(30);
    let mark = fleet_mail::message_deadline::direct_receipt_mark(sent_at, now).unwrap();
    let store = central.statement_host();
    let formatted = |time: workspace_model::Timestamp| {
        text(time.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
    };
    let cases = [
        (
            "UPDATE mail_messages SET deadline_settled_at=? WHERE message_id=?",
            text("now"),
            "deadline_settled_at",
        ),
        (
            "UPDATE mail_messages SET cancel_due_at=? WHERE message_id=?",
            formatted(mark + chrono::Duration::minutes(5)),
            "cancel_due_at",
        ),
        (
            "UPDATE mail_messages SET deadline_settled_at=? WHERE message_id=?",
            formatted(mark - chrono::Duration::milliseconds(1)),
            "deadline_settled_at",
        ),
    ];
    let mut messages = Vec::new();
    for ((change, value, column), request_number) in cases.into_iter().zip(981022..981025) {
        let message_id = queue_direct(&fixture, sent_at, request_number).await;
        raw(&central, change, vec![value, text(&message_id)]).await;
        let candidate = store
            .read(Statement {
                sql: "SELECT subject_key FROM direct_message_due_rows WHERE subject_key=?",
                args: vec![text(&message_id)],
            })
            .await
            .unwrap();
        assert_eq!(candidate.rows, [[text(&message_id)]]);

        let rejected = submit_receipt(
            &fixture,
            &central,
            &message_id,
            ReceiptKind::Seen { completed_at: now },
        )
        .await;
        assert!(
            matches!(rejected, Err(StoreFailure::Corrupt { reason: central_storage::CorruptReason::BadValue { column: reported } }) if reported.ends_with(column))
        );
        messages.push((message_id, column));
    }
    set_receipt_clock(&fixture, &central, mark.timestamp_millis()).await;
    central.after_event().await.unwrap();
    for (message_id, column) in messages {
        assert_store(&central, &message_id, "queued", None).await;
        let attempts = store.read(Statement {
            sql: "SELECT failure_count,last_failure,last_failure_detail FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&message_id)],
        }).await.unwrap();
        assert!(
            matches!(attempts.rows.as_slice(), [attempt] if matches!(attempt.as_slice(),
            [count, kind, SqlValue::Text(detail)] if count == &integer(1) && kind == &text("corrupt_row") && detail.contains(column))),
            "{attempts:?}"
        );
    }
    fixture.host.close().await;
}

pub(super) async fn set_receipt_clock(
    fixture: &Fixture,
    central: &RemoteRuntime,
    milliseconds: i64,
) {
    fixture.clock.0.store(milliseconds, Ordering::SeqCst);
    assert!(
        central
            .control::<bool>("clock", &milliseconds)
            .await
            .unwrap()
    );
}
