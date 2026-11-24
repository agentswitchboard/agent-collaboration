//! Exercises alarm retention and clearing through the owning host.
use super::super::support::*;
use super::engine_event_support::*;
use super::engine_probe_support::*;
use central_storage::{
    AlarmPublication, AlarmUpdate, BatchOutcome, EngineAlarmPlan, StatementBatch,
};

use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult,
};

async fn request(
    fixture: &Fixture,
    number: u32,
    operation: CentralMailOperation,
) -> CentralMailResult {
    let caller = fresh_auth(&fixture.host, &fixture.caller);
    fixture
        .host
        .request_central(
            &caller,
            CentralMailRequest {
                request_id: id(number),
                operation,
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn cancelling_the_last_capped_wake_clears_the_alarm() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let start = now(&fixture);
    let mark = at(start, 5, 0, 0);
    let continuation = at(start, 10, 0, 0);
    let mut wakes = Vec::new();
    for number in 47800..48801 {
        wakes.push(set_wake(&fixture, number, mark).await);
    }
    let tail = wakes
        .into_iter()
        .max_by(|left, right| left.as_str().cmp(right.as_str()))
        .unwrap();
    let wake_due_rows = "SELECT COUNT(*) FROM engine_due_entries WHERE work_kind='wake_timer' AND entry_role='item'";
    let wake_messages = "SELECT COUNT(*) FROM mail_messages WHERE message_kind='wake'";

    assert_eq!(alarm_ms(&runtime).await, ms(mark));
    set_now(&fixture, mark);
    let completion = runtime
        .dispatch_due_alarm()
        .await
        .unwrap()
        .expect("the first mark dispatches the real callback");
    assert_eq!(completion.wakes_fired, 1_000);
    assert_eq!(count(&runtime, wake_messages, vec![]).await, 1_000);
    assert_eq!(count(&runtime, wake_due_rows, vec![]).await, 1);
    assert_eq!(wake_state(&runtime, &tail).await, text("active"));
    assert_eq!(alarm_ms(&runtime).await, ms(continuation));

    // Explicit Clear takes effect even while the capped wake and its projected row remain.
    let clear = runtime
        .statement_host()
        .run_batch(StatementBatch {
            statements: vec![],
            alarm: AlarmUpdate::Engine(EngineAlarmPlan::Clear),
        })
        .await
        .unwrap();
    let alarm_after_clear = alarm_ms(&runtime).await;
    let rearm = runtime
        .statement_host()
        .run_batch(StatementBatch {
            statements: vec![],
            alarm: AlarmUpdate::Engine(EngineAlarmPlan::Recompute),
        })
        .await
        .unwrap();
    assert_eq!(
        rearm,
        BatchOutcome::Applied {
            rows_affected: vec![],
            publication: AlarmPublication::Set {
                unix_millis: continuation.timestamp_millis(),
            },
        }
    );
    assert_eq!(alarm_ms(&runtime).await, ms(continuation));

    assert_eq!(
        request(
            &fixture,
            48802,
            CentralMailOperation::CancelWake {
                wake_id: tail.clone(),
            },
        )
        .await,
        CentralMailResult::Done(CentralMailAnswer::WakeCancelled {
            wake_id: tail.clone(),
        }),
    );

    assert_eq!(wake_state(&runtime, &tail).await, text("disabled"));
    assert_eq!(count(&runtime, wake_due_rows, vec![]).await, 0);
    assert_eq!(count(&runtime, wake_messages, vec![]).await, 1_000);
    assert_eq!(
        (alarm_ms(&runtime).await, alarm_after_clear, clear),
        (
            None,
            None,
            BatchOutcome::Applied {
                rows_affected: vec![],
                publication: AlarmPublication::Cleared,
            },
        ),
        "the empty scan and explicit Clear each remove the retained alarm"
    );
    set_now(&fixture, continuation);
    assert!(runtime.dispatch_due_alarm().await.unwrap().is_none());
    fixture.host.close().await;
}
