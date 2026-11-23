//! V3 check 2 on the native host with real automations: an already-fired
//! occurrence handled again is Stale and ends Superseded, and an automation
//! selected 45 s early by a real due batch fires once for its occurrence.
//! Times are named by their wall time from the 10:00:00 start.
use super::super::engine_evidence::{
    assert_fixture_attempt_ids_are_uuid_v7, witness_fixture_attempt_ids,
};
use super::super::engine_v2_support::*;
use super::super::support::*;
use super::engine_alarm::driver_settled;
use fleet_contracts::EventCompletion;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;
use workspace_model::central_mail::{
    CentralMailAnswer as Answer, CentralMailOperation as Operation, CentralMailRequest,
    CentralMailResult as MailResult,
};

fn at(start: Time, minutes: i64, seconds: i64) -> Time {
    start + chrono::Duration::minutes(minutes) + chrono::Duration::seconds(seconds)
}
async fn due_batch_at(fixture: &Fixture, runtime: &Runtime, time: Time) -> EventCompletion {
    driver_settled(fixture, runtime).await;
    fixture.set_utc(time).await;
    runtime.after_event().await.unwrap()
}
async fn set_interval(fixture: &Fixture, request: u32) -> String {
    let result = fixture
        .host
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(request),
                operation: Operation::SetAutomation {
                    target: session(&fixture.caller),
                    timing: serde_json::from_str(r#"{"kind":"interval","seconds":300}"#).unwrap(),
                    text: "interval".to_owned().try_into().unwrap(),
                },
            },
        )
        .await
        .unwrap();
    let MailResult::Done(Answer::AutomationSet { automation_id, .. }) = result else {
        panic!("interval refused: {result:?}")
    };
    automation_id.to_string()
}
/// (state, next_due_at, evaluated_through_at)
async fn automation_row(
    runtime: &Runtime,
    automation: &str,
) -> Vec<Vec<central_storage::SqlValue>> {
    rows(
        runtime,
        "SELECT state,next_due_at,evaluated_through_at FROM automation_schedules WHERE automation_id=?",
        vec![text(automation)],
    )
    .await
}
fn no_rows() -> Vec<Vec<central_storage::SqlValue>> {
    Vec::new()
}

#[tokio::test]
async fn a_competing_event_does_not_fire_the_held_occurrence_twice() {
    let mut fixture = Fixture::new().await;
    witness_fixture_attempt_ids(&fixture).await;
    let runtime = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let automation = set_interval(&fixture, 15000).await;
    assert_eq!(
        automation_messages(&runtime, &automation).await,
        1,
        "the 10:00 anchor"
    );
    assert_eq!(
        automation_row(&runtime, &automation).await[0][1],
        stamp(at(start, 5, 0))
    );
    // The native alarm driver is also due at 10:05 (the automation's next
    // occurrence); park it before its drive guard so only the two direct
    // batches take the load pauses.
    let _native_driver = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");
    let points = runtime.runtime().pause_points();
    let mut first = points.hold_engine(PausePoint::AfterEngineLoad, &automation);
    driver_settled(&fixture, &runtime).await;
    fixture.set_utc(at(start, 5, 0)).await;
    let first_batch = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    first.until_held().await;
    // A later event can commit its own work while the occurrence is held.
    fixture.set_utc(at(start, 5, 30)).await;
    let second_batch = tokio::spawn({
        let runtime = runtime.clone();
        let caller = fixture.caller.clone();
        let target = session(&fixture.caller);
        async move {
            runtime
                .request_central(
                    &caller,
                    CentralMailRequest {
                        request_id: id(15001),
                        operation: Operation::SetWake {
                            target,
                            due_at: at(start, 60, 0),
                            text: "second event".to_owned().try_into().unwrap(),
                        },
                    },
                )
                .await
        }
    });
    second_batch.await.unwrap().unwrap();
    assert_eq!(automation_messages(&runtime, &automation).await, 1);
    first.release();
    let first_result = first_batch.await.unwrap().unwrap();
    assert_eq!(first_result.work.automations_fired, 1);
    assert_eq!(
        automation_messages(&runtime, &automation).await,
        2,
        "the anchor and the one 10:05 message"
    );
    // The first batch transitions when released, at 10:05:30, so its evaluated
    // point is max(occurrence 10:05, now 10:05:30); the held first transition uses the later applying sample.
    assert_eq!(
        automation_row(&runtime, &automation).await,
        vec![vec![
            text("active"),
            stamp(at(start, 10, 0)),
            stamp(at(start, 5, 30))
        ]]
    );
    assert_eq!(
        record(&runtime, "automation_timer", &automation).await,
        no_rows()
    );

    // The design's replay after a restart: a fresh object's batch rereads the
    // row, so the fired occurrence is never selected again.
    drop(runtime);
    fixture.reopen().await;
    let runtime = super::super::engine_v2_support::runtime(&fixture).await;
    fixture.set_utc(at(start, 5, 35)).await;
    let replay = runtime.after_event().await.unwrap();
    assert_eq!(replay.work.automations_fired, 0);
    assert_eq!(automation_messages(&runtime, &automation).await, 2);
    assert_eq!(
        record(&runtime, "automation_timer", &automation).await,
        no_rows()
    );
    assert_fixture_attempt_ids_are_uuid_v7(
        &fixture,
        "a_competing_event_does_not_fire_the_held_occurrence_twice",
        &[],
    )
    .await;
    fixture.host.close().await;
}

#[tokio::test]
async fn an_at_automation_selected_early_fires_once_and_finishes() {
    let fixture = Fixture::new().await;
    witness_fixture_attempt_ids(&fixture).await;
    let runtime = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    fixture.set_utc(at(start, 1, 0)).await;
    let automation = set_automation_at(&fixture, 15100, at(start, 5, 0), "at").await;

    let early = due_batch_at(&fixture, &runtime, at(start, 4, 15)).await;

    assert_eq!(
        early.work.automations_fired, 1,
        "10:05 is within 10:04:15 + 50 s"
    );
    assert_eq!(
        automation_row(&runtime, &automation).await,
        vec![vec![
            text("done"),
            central_storage::SqlValue::Null,
            stamp(at(start, 5, 0))
        ]]
    );
    assert_eq!(automation_messages(&runtime, &automation).await, 1);

    let at_mark = due_batch_at(&fixture, &runtime, at(start, 5, 0)).await;

    assert_eq!(at_mark.work.automations_fired, 0, "nothing at 10:05");
    assert_eq!(automation_messages(&runtime, &automation).await, 1);
    assert_eq!(
        record(&runtime, "automation_timer", &automation).await,
        no_rows()
    );
    assert_fixture_attempt_ids_are_uuid_v7(
        &fixture,
        "an_at_automation_selected_early_fires_once_and_finishes",
        &[],
    )
    .await;
    fixture.host.close().await;
}

#[tokio::test]
async fn an_interval_automation_selected_early_fires_once_and_moves_to_the_next_mark() {
    let fixture = Fixture::new().await;
    witness_fixture_attempt_ids(&fixture).await;
    let runtime = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let automation = set_interval(&fixture, 15200).await;
    due_batch_at(&fixture, &runtime, at(start, 0, 30)).await;
    assert_eq!(
        automation_messages(&runtime, &automation).await,
        1,
        "the anchor"
    );

    let early = due_batch_at(&fixture, &runtime, at(start, 4, 15)).await;

    assert_eq!(early.work.automations_fired, 1);
    assert_eq!(
        automation_row(&runtime, &automation).await,
        vec![vec![
            text("active"),
            stamp(at(start, 10, 0)),
            stamp(at(start, 5, 0))
        ]]
    );
    assert_eq!(automation_messages(&runtime, &automation).await, 2);

    let at_mark = due_batch_at(&fixture, &runtime, at(start, 5, 0)).await;

    assert_eq!(at_mark.work.automations_fired, 0, "10:05 already consumed");
    assert_eq!(automation_messages(&runtime, &automation).await, 2);
    assert_eq!(
        automation_row(&runtime, &automation).await[0][1],
        stamp(at(start, 10, 0))
    );
    assert_fixture_attempt_ids_are_uuid_v7(
        &fixture,
        "an_interval_automation_selected_early_fires_once_and_moves_to_the_next_mark",
        &[],
    )
    .await;
    fixture.host.close().await;
}
