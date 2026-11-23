use super::deadline_precedence::set_receipt_clock;
use super::*;

#[tokio::test]
async fn a_rejected_direct_mark_rolls_back_and_a_superseding_receipt_prevents_the_charge() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let mark = fleet_mail::message_deadline::direct_receipt_mark(now, now).unwrap();
    let message_id = queue_direct(&fixture, now, 981002).await;
    raw(&central, REJECT_OUTCOME, vec![]).await;

    let mut held = central
        .pause_points()
        .hold_engine(PausePoint::BeforeEngineFailureWrite, message_id.as_str())
        .await;
    set_receipt_clock(&fixture, &central, mark.timestamp_millis()).await;
    let batch = tokio::spawn({
        let central = central.clone();
        async move { central.after_event().await }
    });
    held.until_held().await;

    assert_store(&central, &message_id, "queued", None).await;
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 1).await;
    let attempts = || async {
        central.read(Statement {
            sql: "SELECT attempt_id FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&message_id)],
        }).await.unwrap().rows
    };
    assert!(attempts().await.is_empty());
    // Put the interleaving receipt before the mark; once the mark is reached,
    // deadline precedence requires the same outcome write the fixture rejects.
    set_receipt_clock(&fixture, &central, mark.timestamp_millis() - 1).await;
    assert_eq!(
        apply_fixture_receipt(
            &fixture,
            &central,
            &message_id,
            ReceiptKind::Seen { completed_at: now }
        )
        .await,
        MailReceiptSettlement::Applied
    );
    set_receipt_clock(&fixture, &central, mark.timestamp_millis()).await;
    held.release().await;
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
    let now = chrono::DateTime::from_timestamp(FIXTURE_NOW, 0).unwrap();
    let mark = fleet_mail::message_deadline::direct_receipt_mark(now, now).unwrap();
    let message_id = queue_direct(&fixture, now, 981019).await;
    // The existing raw statement fixture attaches a foreign-key dependent
    // to the due row. Only the final DELETE touches that referenced row.
    raw(&central, "CREATE TABLE fixture_direct_due_reference (due_entry_id TEXT PRIMARY KEY REFERENCES engine_due_entries(due_entry_id) ON DELETE RESTRICT)", vec![]).await;
    raw(&central, "INSERT INTO fixture_direct_due_reference SELECT due_entry_id FROM engine_due_entries WHERE work_kind='direct_message' AND entry_role='item' AND subject_key=?", vec![text(&message_id)]).await;
    let store = central.statement_host();
    let projected = || Statement {
        sql: "SELECT due_entry_id, occurrence_ms, base_due_ms, effective_due_ms FROM engine_due_entries WHERE work_kind='direct_message' AND entry_role='item' AND subject_key=?",
        args: vec![text(&message_id)],
    };
    let before_due = store.read(projected()).await.unwrap();
    assert_eq!(before_due.rows.len(), 1);
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 1).await;
    set_receipt_clock(&fixture, &central, mark.timestamp_millis()).await;
    central.after_event().await.unwrap();
    assert_mark_facts(&central, &message_id, SqlValue::Null, 0, 1).await;
    assert_eq!(store.read(projected()).await.unwrap(), before_due);
    raw(&central, "DROP TABLE fixture_direct_due_reference", vec![]).await;
    let retry = mark + chrono::Duration::minutes(5);
    set_receipt_clock(&fixture, &central, retry.timestamp_millis()).await;
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
    let result = central.read(query).await.unwrap();
    assert_eq!(
        result.rows,
        [[settled_at, integer(outcomes), integer(due_entries)]]
    );
}
