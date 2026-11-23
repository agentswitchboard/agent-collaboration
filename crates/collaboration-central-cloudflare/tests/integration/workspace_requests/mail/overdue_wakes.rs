use super::super::{
    Statement,
    support::{Fixture, text},
};
use super::engine_event_support::{at, callbacks, ms, set_wake, stamp, wake_state};
use super::engine_probe_support::{Runtime, alarm_ms, now, runtime, set_now};
use central_storage::SqlValue;

async fn wake_effects(runtime: &Runtime) -> Vec<Vec<SqlValue>> {
    runtime
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
async fn admitted_wake_committed_after_due_fires_on_its_own_commit() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let start = now(&fixture);
    let preceding_batch = at(start, 4, 45, 0);
    let due = at(start, 5, 0, 0);
    let committed = at(start, 5, 1, 0);
    let next_mark = at(start, 10, 0, 0);
    let before_batch = central.legacy_unit_invocations().await;
    set_now(&fixture, preceding_batch);
    let future = set_wake(&fixture, 45351, next_mark).await;
    let after_batch = central.legacy_unit_invocations().await;
    assert_eq!(after_batch.windows, before_batch.windows);
    assert_eq!(after_batch.drains, before_batch.drains);
    assert_eq!(alarm_ms(&central).await, ms(next_mark));
    let callbacks_before = callbacks(&central).await;

    let point = "beforeWakeRequestCommit";
    let transition = "beforeTimerCommit";
    set_now(&fixture, at(start, 4, 59, 0));
    assert!(
        central
            .control::<bool>("hold-central", &point)
            .await
            .unwrap()
    );
    assert!(
        central
            .control::<bool>("hold-central", &transition)
            .await
            .unwrap()
    );
    let mut request = std::pin::pin!(set_wake(&fixture, 45352, due));
    tokio::select! {
        wake = &mut request => panic!("wake {wake} answered before its held commit"),
        held = central.control::<bool>("until-held-central", &point) => assert!(held.unwrap()),
    }
    assert!(wake_effects(&central).await.is_empty());
    assert_eq!(alarm_ms(&central).await, ms(next_mark));
    set_now(&fixture, committed);
    assert!(
        central
            .control::<bool>("clock", &committed.timestamp_millis())
            .await
            .unwrap()
    );
    assert!(
        central
            .control::<bool>("release-central", &point)
            .await
            .unwrap()
    );
    tokio::select! {
        wake = &mut request => panic!("wake {wake} answered before its guarded transition"),
        held = central.control::<bool>("until-held-central", &transition) => assert!(held.unwrap()),
    }
    assert!(wake_effects(&central).await.is_empty());
    assert_eq!(alarm_ms(&central).await, ms(next_mark));
    assert!(
        central
            .control::<bool>("release-central", &transition)
            .await
            .unwrap()
    );
    let wake = request.await;

    assert_eq!(wake_state(&central, &wake).await, text("done"));
    assert_eq!(
        wake_effects(&central).await,
        vec![vec![text(&wake), stamp(committed)]]
    );
    assert_eq!(wake_state(&central, &future).await, text("active"));
    assert_eq!(alarm_ms(&central).await, ms(next_mark));
    assert_eq!(callbacks(&central).await, callbacks_before);
    let after_commit = central.legacy_unit_invocations().await;
    assert_eq!(after_commit.windows, after_batch.windows);
    assert_eq!(after_commit.drains, after_batch.drains);
    fixture.host.close().await;
}

#[tokio::test]
async fn cold_open_fires_an_overdue_accepted_wake_before_the_next_mark() {
    let mut fixture = Fixture::new().await;
    let opened = runtime(&fixture).await;
    let start = now(&fixture);
    let due = at(start, 5, 0, 0);
    let reopened_at = at(start, 9, 31, 0);
    let next_mark = at(start, 10, 0, 0);
    let wake = set_wake(&fixture, 45353, due).await;
    let future = set_wake(&fixture, 45354, next_mark).await;
    assert_eq!(wake_state(&opened, &wake).await, text("active"));
    assert!(wake_effects(&opened).await.is_empty());
    assert_eq!(alarm_ms(&opened).await, ms(due));
    let callbacks_before = callbacks(&opened).await;
    assert!(opened.control::<bool>("raw-drop-alarm", &()).await.unwrap());
    assert_eq!(alarm_ms(&opened).await, None);
    drop(opened);

    fixture.reopen().await;
    set_now(&fixture, reopened_at);
    let reopened = runtime(&fixture).await;

    // The read opens the owning runtime without a business request or an alarm callback.
    assert_eq!(wake_state(&reopened, &wake).await, text("done"));
    assert_eq!(
        wake_effects(&reopened).await,
        vec![vec![text(&wake), stamp(reopened_at)]]
    );
    assert_eq!(wake_state(&reopened, &future).await, text("active"));
    // Cold recovery targets only the already overdue wake.
    assert_eq!(alarm_ms(&reopened).await, ms(next_mark));
    assert_eq!(callbacks(&reopened).await, callbacks_before);
    fixture.host.close().await;
}
