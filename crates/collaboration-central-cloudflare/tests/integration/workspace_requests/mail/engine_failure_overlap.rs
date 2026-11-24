//! A competing event cannot apply a second due batch or count an early failure.
use super::super::support::*;
use super::engine_probe_support::*;
use collaboration_central_core::engine_probe::ProbeEvent;
use fleet_runtime::PausePoint;

#[tokio::test]
async fn the_active_claim_keeps_a_competing_event_from_charging_the_same_item() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let due = now(&fixture) + chrono::Duration::minutes(5);
    let (item, group) = (probe_id(13090), probe_id(13091));
    let unrelated = probe_id(13092);
    seed_probe(&runtime, item, group, Some(due), "corrupt").await;
    seed_probe(
        &runtime,
        unrelated,
        probe_id(13093),
        Some(due + chrono::Duration::minutes(30)),
        "unrelated",
    )
    .await;
    raw(
        &runtime,
        "UPDATE engine_probe_items SET state='damaged' WHERE item_id=?",
        vec![text(item)],
    )
    .await;
    // Batch A stops after its attempt check saw no record.
    let mut held = runtime
        .pause_points()
        .hold_engine(PausePoint::AfterEngineAttemptGate, &item.to_string())
        .await;
    set_now(&fixture, due);
    let batch_a = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    held.until_held().await;

    // A later competing event completes its own work while the first batch is held.
    set_now(&fixture, due + chrono::Duration::seconds(31));
    runtime
        .request_probe(
            &fixture.caller,
            probe_request(13094, unrelated, ProbeEvent::Stop),
        )
        .await
        .unwrap();
    assert!(attempt(&runtime, item).await.is_empty());

    // The claimant alone records the corrupt load.
    held.release().await;
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
        let start = now(&fixture);
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
        set_now(&fixture, sample);
        let hold_point = if corrupt {
            PausePoint::BeforeEngineFailureWrite
        } else {
            PausePoint::AfterEngineLoad
        };
        let mut held = runtime
            .pause_points()
            .hold_engine(hold_point, &item.to_string())
            .await;
        let stale: tokio::task::JoinHandle<SelectedProbeVerdict> = tokio::spawn({
            let runtime = runtime.clone();
            async move {
                runtime
                    .control(
                        "engine-probe-selected-apply",
                        &SelectedProbe {
                            key: key(item),
                            occurrence: due,
                        },
                    )
                    .await
                    .unwrap()
            }
        });
        held.until_held().await;
        let winning: SelectedProbeVerdict = runtime
            .control(
                "engine-probe-selected-apply",
                &SelectedProbe {
                    key: key(item),
                    occurrence: due,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            winning,
            if corrupt {
                SelectedProbeVerdict::AttemptRecorded
            } else {
                SelectedProbeVerdict::OccurrenceCommitted
            }
        );
        held.release().await;
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
            assert_eq!(alarm_ms(&runtime).await, ms(due));
        } else {
            assert_eq!(alarm_ms(&runtime).await, None);
        }
        fixture.host.close().await;
    }
}
