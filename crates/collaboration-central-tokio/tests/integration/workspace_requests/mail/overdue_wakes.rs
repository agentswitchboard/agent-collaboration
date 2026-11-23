use super::super::{
    Statement,
    support::{Fixture, text},
};
use super::engine_event_support::{at, driver_settled, ms, set_wake, stamp, wake_state};
use super::engine_probe_support::{Runtime, alarm_ms, runtime};
use central_storage::SqlValue;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;

async fn wake_effects(runtime: &Runtime) -> Vec<Vec<SqlValue>> {
    runtime
        .runtime()
        .statement_host()
        .read(Statement {
            sql: "SELECT json_extract(body, '$.wakeId'), created_at
                  FROM mail_messages WHERE message_kind='wake'",
            args: vec![],
        })
        .await
        .unwrap()
        .rows
}

#[tokio::test]
async fn admitted_wake_committed_after_due_fires_inside_request_spacing() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let preceding_batch = at(start, 4, 45, 0);
    let due = at(start, 5, 0, 0);
    let committed = at(start, 5, 1, 0);
    let request_alarm_floor = at(start, 5, 15, 0);
    let next_mark = at(start, 10, 0, 0);
    fixture.set_utc(preceding_batch).await;
    let future = set_wake(&fixture, 45351, next_mark).await;
    assert_eq!(
        central
            .runtime()
            .due_batch_snapshot()
            .last_start_unix_millis,
        ms(preceding_batch)
    );
    assert_eq!(alarm_ms(&central), ms(next_mark));
    driver_settled(&fixture, &central).await;
    let callbacks = central.runtime().alarm_completion_count();

    fixture.set_utc(at(start, 4, 59, 0)).await;
    let mut held = central
        .runtime()
        .pause_points()
        .hold_central(PausePoint::BeforeWakeRequestCommit);
    let mut held_transition = central
        .runtime()
        .pause_points()
        .hold_central(PausePoint::BeforeTimerCommit);
    let mut request = std::pin::pin!(set_wake(&fixture, 45352, due));
    tokio::select! {
        wake = &mut request => panic!("wake {wake} answered before its held commit"),
        () = held.until_held() => {}
    }
    assert!(wake_effects(&central).await.is_empty());
    assert_eq!(alarm_ms(&central), ms(next_mark));
    fixture.set_utc(committed).await;
    held.release();
    tokio::select! {
        wake = &mut request => panic!("wake {wake} answered before its guarded transition"),
        () = held_transition.until_held() => {}
    }
    assert!(wake_effects(&central).await.is_empty());
    assert_eq!(alarm_ms(&central), ms(request_alarm_floor));
    held_transition.release();
    let wake = request.await;

    assert_eq!(wake_state(&central, &wake).await, text("done"));
    assert_eq!(
        wake_effects(&central).await,
        vec![vec![text(&wake), stamp(committed)]]
    );
    assert_eq!(wake_state(&central, &future).await, text("active"));
    assert_eq!(alarm_ms(&central), ms(next_mark));
    assert_eq!(central.runtime().alarm_completion_count(), callbacks);
    assert_eq!(
        central
            .runtime()
            .due_batch_snapshot()
            .last_start_unix_millis,
        ms(preceding_batch),
        "the ordinary due batch was still inside its request spacing"
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn cold_open_fires_an_overdue_accepted_wake_before_the_next_mark() {
    let mut fixture = Fixture::new().await;
    let opened = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let due = at(start, 5, 0, 0);
    let reopened_at = at(start, 9, 31, 0);
    let next_mark = at(start, 10, 0, 0);
    let wake = set_wake(&fixture, 45353, due).await;
    let future = set_wake(&fixture, 45354, next_mark).await;
    assert_eq!(alarm_ms(&opened), ms(due));
    driver_settled(&fixture, &opened).await;

    // Move UTC without advancing the scheduled driver's monotonic deadline.
    fixture.set_utc(reopened_at).await;
    assert_eq!(wake_state(&opened, &wake).await, text("active"));
    assert!(wake_effects(&opened).await.is_empty());
    drop(opened);
    fixture.reopen().await;
    let reopened = runtime(&fixture).await;

    assert_eq!(wake_state(&reopened, &wake).await, text("done"));
    assert_eq!(
        wake_effects(&reopened).await,
        vec![vec![text(&wake), stamp(reopened_at)]]
    );
    assert_eq!(wake_state(&reopened, &future).await, text("active"));
    // The next wake is inside a regular batch's look-ahead. A cold claim
    // would also floor its alarm at 10:10:01, one second after its due time.
    assert_eq!(alarm_ms(&reopened), ms(next_mark));
    assert!(reopened.runtime().due_batch_starts().is_empty());
    assert_eq!(reopened.runtime().alarm_completion_count(), 0);
    fixture.host.close().await;
}
