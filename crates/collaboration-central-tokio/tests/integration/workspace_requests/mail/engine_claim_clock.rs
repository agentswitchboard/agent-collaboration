use super::super::support::*;
use super::engine_event_support::*;
use super::engine_probe_support::*;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult, NoticeCadence,
};

async fn fired_probes(runtime: &Runtime, group: uuid::Uuid) -> i64 {
    count(
        runtime,
        "SELECT COUNT(*) FROM engine_probe_items WHERE group_id=? AND state='fired'",
        vec![text(group)],
    )
    .await
}

#[tokio::test]
async fn alarm_waiting_across_two_marks_claims_with_the_current_cutoff_and_fence() {
    let fixture = Fixture::new().await;
    let central = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let old_group = probe_id(62_000);
    let new_group = probe_id(62_001);
    for index in 0..510 {
        let (group, due) = if index < 480 {
            (old_group, at(start, 5, 0, 0))
        } else {
            (new_group, at(start, 15, 0, 0))
        };
        seed_probe(
            &central,
            probe_id(63_000 + index),
            group,
            Some(due),
            &format!("claim-clock-{index}"),
        )
        .await;
    }
    let _physical_driver = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");
    let mut selected = central
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::AfterEngineLoad, &probe_id(63_000).to_string());
    fixture.set_utc(at(start, 5, 0, 0)).await;
    let first = tokio::spawn({
        let central = central.clone();
        async move { central.after_alarm().await.unwrap() }
    });
    selected.until_held().await;
    assert!(central.runtime().due_batch_snapshot().leftovers_pending);

    fixture.set_utc_while_a_batch_is_held(at(start, 10, 0, 0));
    let mut delayed = std::pin::pin!(central.after_alarm());
    tokio::select! {
        biased;
        _ = &mut delayed => panic!("the later alarm took a still-protected claim"),
        () = async {} => {},
    }
    fixture.set_utc_while_a_batch_is_held(at(start, 15, 0, 0));
    selected.release();
    first.await.unwrap();
    delayed.await.unwrap();

    assert_eq!(
        central
            .runtime()
            .due_batch_snapshot()
            .last_start_unix_millis,
        ms(at(start, 15, 0, 0))
    );
    assert_eq!(fired_probes(&central, old_group).await, 480);
    assert_eq!(fired_probes(&central, new_group).await, 20);
    assert_eq!(alarm_ms(&central), ms(at(start, 20, 0, 0)));

    fixture.set_utc(at(start, 15, 31, 0)).await;
    let fresh_caller = fixture.fresh_caller(&fixture.caller).await;
    assert!(matches!(
        fixture
            .host
            .request_central(
                &fresh_caller,
                CentralMailRequest {
                    request_id: id(62_002),
                    operation: CentralMailOperation::SetNoticeCadence {
                        cadence: NoticeCadence::M15
                    },
                },
            )
            .await
            .unwrap(),
        CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet { .. })
    ));
    assert_eq!(fired_probes(&central, new_group).await, 20);
    assert_eq!(alarm_ms(&central), ms(at(start, 20, 0, 0)));
    fixture.host.close().await;
}
