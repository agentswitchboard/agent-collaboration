use super::*;

#[tokio::test]
async fn one_held_unit_never_delays_the_other_nineteen_or_a_probe_request() {
    let fixture = Fixture::new().await;
    witness_fixture_attempt_ids(&fixture).await;
    let runtime = runtime(&fixture).await;
    let due = fixture.clock.utc_now() + minutes(5);
    let group = probe_id(9200);
    let items: Vec<_> = (0..20).map(|offset| probe_id(9201 + offset)).collect();
    for (offset, item) in items.iter().enumerate() {
        seed_probe(&runtime, *item, group, Some(due), &format!("unit{offset}")).await;
    }
    let other = probe_id(9230);
    seed_probe(&runtime, other, group, None, "request").await;
    // The native alarm driver would also find this work due; park it before its
    // drive guard so only this case's direct engine calls take the pauses.
    let _native_driver = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::AfterEngineLoad, &items[0].to_string());
    fixture.set_utc(due).await;
    let batch = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    held.until_held().await;
    let mut committed = 0;
    while committed < 19 {
        tokio::task::yield_now().await;
        committed = 0;
        for item in &items[1..] {
            if probe_facts(&runtime, *item, group).await[0] == text("fired") {
                committed += 1;
            }
        }
    }
    let answer = runtime
        .request_probe(&fixture.caller, probe_request(9231, other, ProbeEvent::Run))
        .await
        .unwrap();
    assert!(matches!(answer, EngineRequestOutcome::Answer { .. }));
    assert_eq!(
        probe_facts(&runtime, items[0], group).await[0],
        text("waiting")
    );
    held.release();
    batch.await.unwrap().unwrap();
    assert_eq!(
        probe_facts(&runtime, items[0], group).await[0],
        text("fired")
    );
    assert_fixture_attempt_ids_are_uuid_v7(
        &fixture,
        "one_held_unit_never_delays_the_other_nineteen_or_a_probe_request",
        &[],
    )
    .await;
    fixture.host.close().await;
}
