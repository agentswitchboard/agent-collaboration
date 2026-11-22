use super::super::direct_receipt_sequences::queue_direct;
use super::*;
use fleet_runtime::PausePoint;

#[tokio::test]
async fn direct_mark_and_charge_survive_a_rejected_final_alarm_and_retry_once() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let accepted = fixture.clock.utc_now();
    let mark = fleet_mail::message_deadline::direct_receipt_mark(accepted, accepted);
    let retry_mark = mark + chrono::Duration::minutes(5);
    let healthy = queue_direct(&fixture, accepted, 981007).await;
    let rejected = queue_direct(&fixture, accepted, 981008).await;
    raw(&central,
        "CREATE TRIGGER reject_one_due_outcome BEFORE INSERT ON mail_messages WHEN NEW.message_kind='deliveryOutcome' AND json_extract(NEW.body,'$.requestId')='01890f2e-7b4c-7cc0-98c4-0000000ef810' BEGIN SELECT RAISE(ABORT, 'rejected direct outcome'); END",
        vec![],
    ).await;

    driver_settled(&fixture, &central).await;
    let alarm_driver = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");
    let mut marked = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::AfterEngineCommit, healthy.as_str());
    let mut charged = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeEngineFailureWrite, rejected.as_str());
    central
        .runtime()
        .statement_host()
        .fail_next_alarm_batch(StoreFailure::Unavailable);
    fixture
        .set_utc(mark + chrono::Duration::milliseconds(500))
        .await;
    let finishing = tokio::spawn({
        let central = central.clone();
        async move { central.after_event().await }
    });
    marked.until_held().await;
    charged.until_held().await;
    marked.release();
    charged.release();
    assert_eq!(
        finishing.await.unwrap(),
        Err(AlarmNotPublished {
            failure: StoreFailure::Unavailable
        })
    );

    let attempt = || async {
        central.runtime().statement_host().read(Statement {
            sql: "SELECT state,failure_count,next_try_at FROM engine_work_attempts WHERE machine_kind='direct_message' AND subject_key=?",
            args: vec![text(&rejected)],
        }).await.unwrap().rows
    };
    let outcome = |message: &workspace_ids::MessageId| Statement {
        sql: "SELECT message_id,body,created_at,updated_at FROM mail_messages WHERE message_kind='deliveryOutcome' AND json_extract(body,'$.messageId')=?",
        args: vec![text(message)],
    };
    let state = |message: &workspace_ids::MessageId| Statement {
        sql: "SELECT state FROM mail_messages WHERE message_id=?",
        args: vec![text(message)],
    };
    let original_outcome = central
        .runtime()
        .statement_host()
        .read(outcome(&healthy))
        .await
        .unwrap()
        .rows;
    assert_eq!(original_outcome.len(), 1);
    assert_eq!(
        central
            .runtime()
            .statement_host()
            .read(state(&healthy))
            .await
            .unwrap()
            .rows,
        [vec![text("pastTheMark")]]
    );
    assert_eq!(
        central
            .runtime()
            .statement_host()
            .read(state(&rejected))
            .await
            .unwrap()
            .rows,
        [vec![text("queued")]]
    );
    assert!(
        central
            .runtime()
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
    fixture.set_utc(mark + chrono::Duration::seconds(31)).await;
    central.after_event().await.unwrap();
    assert_eq!(alarm_ms(&central), Some(retry_mark.timestamp_millis()));
    assert_eq!(attempt().await, owed);
    assert_eq!(
        central
            .runtime()
            .statement_host()
            .read(outcome(&healthy))
            .await
            .unwrap()
            .rows,
        original_outcome
    );
    raw(&central, "DROP TRIGGER reject_one_due_outcome", vec![]).await;
    drop(alarm_driver);
    driver_settled(&fixture, &central).await;
    let completions = central.runtime().alarm_completion_count();
    fixture
        .advance_to(SyncTime::from_unix_millis(retry_mark.timestamp_millis()))
        .await;
    central
        .runtime()
        .until_alarm_completed(completions + 1)
        .await;
    assert!(attempt().await.is_empty());
    assert_eq!(
        central
            .runtime()
            .statement_host()
            .read(outcome(&healthy))
            .await
            .unwrap()
            .rows,
        original_outcome
    );
    assert_eq!(
        central
            .runtime()
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
            .runtime()
            .statement_host()
            .read(state(&rejected))
            .await
            .unwrap()
            .rows,
        [vec![text("pastTheMark")]]
    );
    fixture.host.close().await;
}
