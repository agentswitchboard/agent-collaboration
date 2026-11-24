use super::super::direct_receipt_sequences::queue_direct;
use super::*;
use fleet_runtime::PausePoint;

#[tokio::test]
async fn direct_mark_and_charge_survive_a_rejected_final_alarm_and_retry_once() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let accepted = now(&fixture);
    let mark = fleet_mail::message_deadline::direct_receipt_mark(accepted, accepted).unwrap();
    let retry_mark = mark + chrono::Duration::minutes(5);
    let healthy = queue_direct(&fixture, accepted, 981007).await;
    let rejected = queue_direct(&fixture, accepted, 981008).await;
    raw(&central,
        "CREATE TRIGGER reject_one_due_outcome BEFORE INSERT ON mail_messages WHEN NEW.message_kind='deliveryOutcome' AND json_extract(NEW.body,'$.requestId')='01890f2e-7b4c-7cc0-98c4-0000000ef810' BEGIN SELECT RAISE(ABORT, 'rejected direct outcome'); END",
        vec![],
    ).await;

    let mut marked = central
        .pause_points()
        .hold_engine(PausePoint::AfterEngineCommit, healthy.as_str())
        .await;
    let mut charged = central
        .pause_points()
        .hold_engine(PausePoint::BeforeEngineFailureWrite, rejected.as_str())
        .await;
    assert!(
        central
            .control::<bool>("fail-alarm-batch", &StoreFailure::Unavailable)
            .await
            .unwrap()
    );
    set_now(&fixture, mark + chrono::Duration::milliseconds(500));
    let finishing = tokio::spawn({
        let central = central.clone();
        async move { central.after_event().await }
    });
    marked.until_held().await;
    charged.until_held().await;
    marked.release().await;
    charged.release().await;
    assert_eq!(finishing.await.unwrap(), Err(StoreFailure::Unavailable));

    let attempt = || async {
        central.statement_host().read(Statement {
            sql: "SELECT state,failure_count,next_try_at FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&rejected)],
        }).await.unwrap().rows
    };
    let outcome = |message: &workspace_ids::MessageId| Statement {
        sql: "SELECT message_id,body,created_at,updated_at FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=?",
        args: vec![text(message)],
    };
    let state = |message: &workspace_ids::MessageId| Statement {
        sql: "SELECT state, deadline_settled_at IS NOT NULL FROM mail_messages WHERE message_id=?",
        args: vec![text(message)],
    };
    let original_outcome = central
        .statement_host()
        .read(outcome(&healthy))
        .await
        .unwrap()
        .rows;
    assert_eq!(original_outcome.len(), 1);
    assert_eq!(
        central
            .statement_host()
            .read(state(&healthy))
            .await
            .unwrap()
            .rows,
        [vec![text("queued"), integer(1)]]
    );
    assert_eq!(
        central
            .statement_host()
            .read(state(&rejected))
            .await
            .unwrap()
            .rows,
        [vec![text("queued"), integer(0)]]
    );
    assert!(
        central
            .statement_host()
            .read(outcome(&rejected))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    let owed = vec![vec![
        text("failing"),
        integer(1),
        text(fleet_runtime_ports::stored_values::format_utc_millis(
            retry_mark,
        )),
    ]];
    assert_eq!(attempt().await, owed);

    // The next engine alarm completion republishes the due retry without spending it early.
    set_now(&fixture, mark + chrono::Duration::seconds(31));
    central.after_event().await.unwrap();
    assert_eq!(
        alarm_ms(&central).await,
        Some(retry_mark.timestamp_millis())
    );
    assert_eq!(attempt().await, owed);
    assert_eq!(
        central
            .statement_host()
            .read(outcome(&healthy))
            .await
            .unwrap()
            .rows,
        original_outcome
    );
    raw(&central, "DROP TRIGGER reject_one_due_outcome", vec![]).await;
    retained_alarm_fires_at(&fixture, &central, retry_mark).await;
    assert!(attempt().await.is_empty());
    assert_eq!(
        central
            .statement_host()
            .read(outcome(&healthy))
            .await
            .unwrap()
            .rows,
        original_outcome
    );
    assert_eq!(
        central
            .statement_host()
            .read(outcome(&rejected))
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(
        central
            .statement_host()
            .read(state(&rejected))
            .await
            .unwrap()
            .rows,
        [vec![text("queued"), integer(1)]]
    );
    fixture.host.close().await;
}
