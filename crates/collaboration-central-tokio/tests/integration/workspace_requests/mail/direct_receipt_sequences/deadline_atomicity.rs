use super::*;
use central_storage::SqlValue;

#[tokio::test]
async fn a_rejected_direct_mark_rolls_back_and_a_superseding_receipt_prevents_the_charge() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let now = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::direct_receipt_mark(now, now).unwrap();
    let message_id = queue_direct(&fixture, now, 981002).await;
    raw(&central, REJECT_OUTCOME, vec![]).await;
    let _alarm_driver = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");

    let mut held = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeEngineFailureWrite, message_id.as_str());
    fixture.set_utc(mark).await;
    let batch = tokio::spawn({
        let central = central.clone();
        async move { central.after_event().await }
    });
    held.until_held().await;

    assert_store(&central, &message_id, "queued", None).await;
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 1).await;
    let attempts = || async {
        central.runtime().statement_host().read(Statement {
            sql: "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&message_id)],
        }).await.unwrap().rows
    };
    assert!(attempts().await.is_empty());
    // Put the interleaving receipt before the mark; once the mark is reached,
    // deadline precedence requires the same outcome write the fixture rejects.
    fixture
        .set_utc(mark - chrono::Duration::milliseconds(1))
        .await;
    let recipient = fixture.fresh_caller(&fixture.caller).await;
    assert_eq!(
        central
            .apply_delivery_receipt(
                &recipient.connection().attachment(),
                &message_id,
                ReceiptKind::Seen { completed_at: now },
            )
            .await
            .unwrap(),
        MailReceiptSettlement::Applied
    );
    fixture.set_utc(mark).await;
    held.release();
    batch.await.unwrap().unwrap();

    assert_store(&central, &message_id, "seen", None).await;
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 0).await;
    assert!(
        attempts().await.is_empty(),
        "the rolled-back version was superseded"
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn rejecting_the_final_due_retirement_rolls_back_all_mail_writes() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let now = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::direct_receipt_mark(now, now).unwrap();
    let message_id = queue_direct(&fixture, now, 981019).await;
    // The existing raw statement fixture attaches a foreign-key dependent
    // to the due row. Only the final DELETE touches that referenced row.
    raw(&central, "CREATE TABLE fixture_direct_due_reference (due_entry_id TEXT PRIMARY KEY REFERENCES engine_due_entries(due_entry_id) ON DELETE RESTRICT)", vec![]).await;
    raw(&central, "INSERT INTO fixture_direct_due_reference SELECT due_entry_id FROM engine_due_entries WHERE work_kind='direct_message' AND entry_role='item' AND subject_key=?", vec![text(&message_id)]).await;
    let _alarm_driver = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");
    let store = central.runtime().statement_host();
    let projected = || Statement {
        sql: "SELECT due_entry_id, occurrence_ms, base_due_ms, effective_due_ms FROM engine_due_entries WHERE work_kind='direct_message' AND entry_role='item' AND subject_key=?",
        args: vec![text(&message_id)],
    };
    let before_due = store.read(projected()).await.unwrap();
    assert_eq!(before_due.rows.len(), 1);
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 1).await;
    fixture.set_utc(mark).await;
    central.after_event().await.unwrap();
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 1).await;
    assert_eq!(store.read(projected()).await.unwrap(), before_due);
    raw(&central, "DROP TABLE fixture_direct_due_reference", vec![]).await;
    let retry = mark + chrono::Duration::minutes(5);
    fixture.set_utc(retry).await;
    central.after_event().await.unwrap();
    let settled = text(retry.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
    assert_mark_facts(&central, &message_id, settled.clone(), 1, 0).await;
    central.after_event().await.unwrap();
    assert_mark_facts(&central, &message_id, settled, 1, 0).await;
    fixture.host.close().await;
}

async fn assert_mark_facts(
    central: &super::super::engine_probe_support::Runtime,
    message_id: &workspace_ids::MessageId,
    settled_at: SqlValue,
    outcomes: i64,
    due_entries: i64,
) {
    let query = Statement {
        sql: "SELECT deadline_settled_at,(SELECT COUNT(*) FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=m.message_id),(SELECT COUNT(*) FROM engine_due_entries WHERE work_kind='direct_message' AND entry_role='item' AND subject_key=m.message_id) FROM mail_messages AS m WHERE m.message_id=?",
        args: vec![text(message_id)],
    };
    let result = central
        .runtime()
        .statement_host()
        .read(query)
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        [[settled_at, integer(outcomes), integer(due_entries)]]
    );
}
