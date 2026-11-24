//! A competing event cannot apply a second due batch or count an early failure.
use super::super::support::*;
use super::engine_probe_support::*;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;

#[tokio::test]
async fn the_active_claim_keeps_a_competing_event_from_charging_the_same_item() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let due = fixture.clock.utc_now() + chrono::Duration::minutes(5);
    let (item, group) = (probe_id(13090), probe_id(13091));
    seed_probe(&runtime, item, group, Some(due), "corrupt").await;
    raw(
        &runtime,
        "UPDATE engine_probe_items SET state='damaged' WHERE item_id=?",
        vec![text(item)],
    )
    .await;
    // Batch A stops after its attempt check saw no record.
    // The native alarm driver would also find this work due; park it before its
    // drive guard so only this case's direct engine calls take the pauses.
    let _native_driver = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::BeforeDriveGuard, "driver");
    let mut held = runtime
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::AfterEngineAttemptGate, &item.to_string());
    fixture.set_utc(due).await;
    let batch_a = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    held.until_held().await;

    // A later competing event completes its own work while the first batch is held.
    fixture.set_utc_while_a_batch_is_held(due + chrono::Duration::seconds(31));
    runtime.after_event().await.unwrap();
    assert!(attempt(&runtime, item).await.is_empty());

    // The claimant alone records the corrupt load.
    held.release();
    batch_a.await.unwrap().unwrap();

    assert_eq!(
        attempt(&runtime, item).await,
        vec![vec![text("failing"), integer(1), text("corrupt_row")]],
        "a failure seen before the next try must not count as the second"
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn stale_selected_work_loses_the_guard_after_the_winning_apply_or_attempt() {
    use super::engine_event_support::{at, ms};
    use collaboration_central_core::engine_probe::{SelectedProbe, SelectedProbeVerdict};

    for corrupt in [false, true] {
        let fixture = Fixture::new().await;
        let runtime = runtime(&fixture).await;
        let start = fixture.clock.utc_now();
        let due = at(start, 5, 0, 0);
        let sample = at(start, 4, 45, 0);
        let (item, group) = (probe_id(43880), probe_id(43881));
        seed_probe(&runtime, item, group, Some(due), "overlapping guarded due").await;
        if corrupt {
            raw(
                &runtime,
                "UPDATE engine_probe_items SET state='broken' WHERE item_id=?",
                vec![text(item)],
            )
            .await;
        }
        fixture.set_utc(sample).await;
        let hold_point = if corrupt {
            PausePoint::BeforeEngineFailureWrite
        } else {
            PausePoint::AfterEngineLoad
        };
        let mut held = runtime
            .runtime()
            .pause_points()
            .hold_engine(hold_point, &item.to_string());
        let stale = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                runtime
                    .apply_selected_probe(SelectedProbe {
                        key: key(item),
                        occurrence: due,
                    })
                    .await
            }
        });
        held.until_held().await;
        let winning: SelectedProbeVerdict = runtime
            .apply_selected_probe(SelectedProbe {
                key: key(item),
                occurrence: due,
            })
            .await;
        assert_eq!(
            winning,
            if corrupt {
                SelectedProbeVerdict::AttemptRecorded
            } else {
                SelectedProbeVerdict::OccurrenceCommitted
            }
        );
        held.release();
        assert_eq!(stale.await.unwrap(), SelectedProbeVerdict::Unchanged);

        runtime.after_event().await.unwrap();
        assert_eq!(
            probe_facts(&runtime, item, group).await,
            if corrupt {
                vec![text("broken"), integer(0), integer(1)]
            } else {
                vec![text("fired"), integer(1), integer(0)]
            },
        );
        if corrupt {
            assert_eq!(
                attempt(&runtime, item).await,
                vec![vec![text("failing"), integer(1), text("corrupt_row")]]
            );
            assert_eq!(alarm_ms(&runtime), ms(due));
        } else {
            assert_eq!(alarm_ms(&runtime), None);
        }
        fixture.host.close().await;
    }
}
