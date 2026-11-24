//! V12 races on the native host: a publication built before a newer claim uses
//! the newer floor and backlog, a claim waits out a decision's publication, two
//! simultaneous events run one due batch, notice units get no look-ahead, and a
//! changed organization stamp refuses every event.
use super::super::support::*;
use super::engine_event_support::*;
use super::engine_probe_support::*;
use central_storage::{AlarmPublication, CorruptReason, StoreFailure};
use collaboration_central_core::engine_probe::ProbeEvent;
use fleet_contracts::AlarmNotPublished;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;
use std::sync::Arc;

fn set(time: Time) -> AlarmPublication {
    AlarmPublication::Set {
        unix_millis: time.timestamp_millis(),
    }
}

/// Moves the observer's open window to `due`, keeping its items.
async fn window_due_at(central_service: &Runtime, due: Time) {
    raw(
        central_service,
        "UPDATE mail_notification_windows SET due_at=?",
        vec![stamp(due)],
    )
    .await;
}

#[tokio::test]
async fn overlapping_publications_keep_the_exact_window_deadline() {
    let fixture = Fixture::new().await;
    let central_service = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    open_notice_window(&fixture, 32000).await;
    due_batch_at(&fixture, &central_service, at(start, 4, 0, 0)).await;
    window_due_at(&central_service, at(start, 5, 0, 0)).await;
    driver_settled(&fixture, &central_service).await;
    fixture.set_utc(at(start, 4, 20, 0)).await;
    // Event A, inside the spacing, holds its only batch after commit and
    // before its alarm decision, with the 10:04:00 start still current.
    let mut held = central_service
        .runtime()
        .statement_host()
        .hold_alarm_publication();
    let event_a = tokio::spawn({
        let central_service = central_service.clone();
        async move { central_service.after_event().await }
    });
    held.until_held().await;
    fixture.set_utc(at(start, 4, 58, 0)).await;
    let event_b = tokio::spawn({
        let central_service = central_service.clone();
        async move { central_service.after_event().await }
    });
    // B claimed before its first await and now queues behind A's batch.
    central_service
        .runtime()
        .statement_host()
        .until_batch_waiting()
        .await;
    fixture.set_utc(at(start, 4, 59, 0)).await;

    held.release();
    let published_a = event_a.await.unwrap().unwrap().publication;
    let completion_b = event_b.await.unwrap().unwrap();

    assert_eq!(
        published_a,
        set(at(start, 5, 0, 0)),
        "A used B's floor, never its own 10:05:00"
    );
    assert_eq!(completion_b.publication, set(at(start, 5, 0, 0)));
    assert_eq!(
        completion_b.work.windows_fired, 0,
        "the window is not run early"
    );
    assert_eq!(alarm_ms(&central_service), ms(at(start, 5, 0, 0)));
    assert_eq!(
        window_due(&central_service).await,
        vec![vec![stamp(at(start, 5, 0, 0))]]
    );
    assert_eq!(notices(&central_service).await, 0);
    fixture.host.close().await;
}

#[tokio::test]
async fn a_request_held_across_a_capped_drain_does_not_rearm_finished_work() {
    let fixture = Fixture::new().await;
    let central_service = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    due_batch_at(&fixture, &central_service, at(start, 4, 0, 0)).await;
    let group = probe_id(32100);
    for index in 0..251 {
        seed_probe(
            &central_service,
            probe_id(32200 + index),
            group,
            Some(at(start, 5, 0, 0)),
            &format!("capped{index}"),
        )
        .await;
    }
    let (traffic, traffic_group) = (probe_id(32101), probe_id(32102));
    seed_probe(&central_service, traffic, traffic_group, None, "traffic").await;
    let mut held = central_service
        .runtime()
        .pause_points()
        .hold_engine(PausePoint::AfterEngineCommit, &traffic.to_string());
    driver_settled(&fixture, &central_service).await;
    fixture.set_utc(at(start, 4, 20, 0)).await;
    let request_a = tokio::spawn({
        let central_service = central_service.clone();
        let caller = fixture.caller.clone();
        async move {
            central_service
                .request_probe(&caller, probe_request(32103, traffic, ProbeEvent::Run))
                .await
        }
    });
    held.until_held().await;
    fixture.set_utc(at(start, 4, 58, 0)).await;

    let completion_b = central_service.after_event().await.unwrap();

    assert_eq!(completion_b.publication, AlarmPublication::Cleared);
    assert_eq!(
        count(
            &central_service,
            "SELECT COUNT(*) FROM engine_probe_items WHERE group_id=? AND state='fired'",
            vec![text(group)]
        )
        .await,
        251
    );
    driver_settled(&fixture, &central_service).await;
    fixture.set_utc(at(start, 4, 59, 0)).await;
    held.release();
    assert!(matches!(
        request_a.await.unwrap().unwrap(),
        collaboration_central_core::engine_probe::EngineRequestOutcome::Answer { .. }
    ));
    assert_eq!(
        alarm_ms(&central_service),
        None,
        "A did not resurrect completed work"
    );
    fixture.host.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_events_released_together_run_exactly_one_due_batch() {
    let fixture = Fixture::new().await;
    let central_service = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    due_batch_at(&fixture, &central_service, at(start, 4, 0, 0)).await;
    seed_probe(
        &central_service,
        probe_id(35402),
        probe_id(35403),
        Some(at(start, 5, 0, 0)),
        "single due claim",
    )
    .await;
    let before = central_service.runtime().legacy_unit_invocations();
    fixture.set_utc(at(start, 4, 31, 0)).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let events: Vec<_> = (0..2)
        .map(|_| {
            let central_service = central_service.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                central_service.after_event().await
            })
        })
        .collect();

    for event in events {
        event.await.unwrap().unwrap();
    }

    let after = central_service.runtime().legacy_unit_invocations();
    assert_eq!(
        (after.windows - before.windows, after.drains - before.drains),
        (1, 1),
        "exactly one due batch ran its notice units"
    );
    assert_eq!(
        central_service.runtime().due_batch_starts().last().copied(),
        ms(at(start, 4, 31, 0))
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn a_wake_due_with_a_window_fires_early_and_the_window_waits_for_its_slot() {
    let fixture = Fixture::new().await;
    let central_service = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    open_notice_window(&fixture, 32400).await;
    assert_eq!(
        window_due(&central_service).await,
        vec![vec![stamp(at(start, 15, 0, 0))]]
    );
    let wake = set_wake(&fixture, 32410, at(start, 15, 0, 0)).await;
    let windows = count(
        &central_service,
        "SELECT COUNT(*) FROM mail_notification_windows",
        vec![],
    )
    .await;
    assert!(windows > 0);

    due_batch_at(&fixture, &central_service, at(start, 14, 40, 0)).await;

    assert_eq!(
        wake_state(&central_service, &wake).await,
        text("done"),
        "20 s early"
    );
    assert_eq!(
        notices(&central_service).await,
        0,
        "the window is not run early"
    );
    assert_eq!(alarm_ms(&central_service), ms(at(start, 15, 0, 0)));
    driver_fires_at(&fixture, &central_service, at(start, 15, 0, 0)).await;
    assert_eq!(
        notices(&central_service).await,
        windows,
        "each window sent its notice"
    );
    due_batch_at(&fixture, &central_service, at(start, 15, 41, 0)).await;
    assert_eq!(
        notices(&central_service).await,
        windows,
        "each notice went out once"
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn another_valid_stamp_refuses_every_event_and_touches_nothing() {
    let fixture = Fixture::new().await;
    let central_service = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let (due, due_group) = (probe_id(32500), probe_id(32501));
    let (traffic, traffic_group) = (probe_id(32502), probe_id(32503));
    seed_probe(
        &central_service,
        due,
        due_group,
        Some(at(start, 5, 0, 0)),
        "due",
    )
    .await;
    seed_probe(&central_service, traffic, traffic_group, None, "traffic").await;
    due_batch_at(&fixture, &central_service, at(start, 1, 0, 0)).await;
    let previous = alarm_ms(&central_service);
    assert_eq!(previous, ms(at(start, 5, 0, 0)));
    let other: uuid::Uuid = id::<workspace_model::WorkspaceId>(32504)
        .to_string()
        .parse()
        .unwrap();
    assert_eq!(other.get_version_num(), 7);
    raw(
        &central_service,
        "UPDATE signin_organization_identity SET id=?",
        vec![text(other)],
    )
    .await;
    let refused = StoreFailure::Corrupt {
        reason: CorruptReason::BadValue {
            column: "signin_organization_identity.id".to_owned(),
        },
    };
    driver_settled(&fixture, &central_service).await;
    fixture.set_utc(at(start, 5, 0, 0)).await;

    let fan_out = central_service.after_event().await;
    let request = central_service
        .request_probe(
            &fixture.caller,
            probe_request(32505, traffic, ProbeEvent::Run),
        )
        .await;

    assert_eq!(
        fan_out,
        Err(AlarmNotPublished {
            failure: refused.clone()
        })
    );
    assert_eq!(request, Err(refused));
    for (item, group) in [(due, due_group), (traffic, traffic_group)] {
        assert_eq!(
            probe_facts(&central_service, item, group).await,
            vec![text("waiting"), integer(0), integer(0)],
            "no instance is touched"
        );
    }
    assert_eq!(receipts(&central_service, 32505).await, 0);
    assert_eq!(
        count(
            &central_service,
            "SELECT COUNT(*) FROM engine_work_attempts",
            vec![]
        )
        .await,
        0,
        "no record is written"
    );
    assert_eq!(alarm_ms(&central_service), previous);

    raw(
        &central_service,
        "DELETE FROM signin_organization_identity",
        vec![],
    )
    .await;
    central_service.after_event().await.unwrap();
    assert_eq!(
        alarm_ms(&central_service),
        None,
        "unprovisioned: the alarm is cleared"
    );
    assert_eq!(
        probe_facts(&central_service, due, due_group).await,
        vec![text("waiting"), integer(0), integer(0)]
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn a_future_text_key_cannot_mask_a_corrupt_legacy_deadline() {
    let malformed = ["now", "2050-02-30T15:00:00.000Z", "2050-02-25T15:00:00.000"];
    for (notices, corrupt_deadline) in [true, false].into_iter().flat_map(|notices| {
        malformed
            .into_iter()
            .map(move |deadline| (notices, deadline))
    }) {
        let fixture = Fixture::new().await;
        let runtime = runtime(&fixture).await;
        let start = fixture.clock.utc_now();
        let future = at(start, 15, 0, 0);
        let now = at(start, 4, 20, 0);
        let (healthy, invalid) = (probe_id(43852), probe_id(43853));
        let (work_kind, read_deadlines) = if notices {
            raw(
                &runtime,
                "INSERT INTO mail_notification_windows VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)",
                vec![
                    text(healthy), text(probe_id(43854)), stamp(start), stamp(future), stamp(start), stamp(start),
                    text(invalid), text(probe_id(43855)), stamp(start), text(corrupt_deadline), stamp(start), stamp(start),
                ],
            ).await;
            (
                "legacy_notice_windows",
                "SELECT due_at FROM mail_notification_windows ORDER BY due_at",
            )
        } else {
            raw(
                &runtime,
                "INSERT INTO directory_workspaces(workspace_id,name,state,created_at,updated_at) VALUES (?,'fixture one','active',?,?), (?,'fixture two','active',?,?)",
                vec![text(healthy), stamp(start), stamp(start), text(invalid), stamp(start), stamp(start)],
            ).await;
            raw(
                &runtime,
                "INSERT INTO mail_workspace_drains VALUES (?,0,?,?,?), (?,0,?,?,?)",
                vec![
                    text(healthy),
                    stamp(future),
                    stamp(start),
                    stamp(start),
                    text(invalid),
                    text(corrupt_deadline),
                    stamp(start),
                    stamp(start),
                ],
            )
            .await;
            (
                "legacy_workspace_drains",
                "SELECT drain_due_at FROM mail_workspace_drains WHERE drain_due_at IS NOT NULL ORDER BY drain_due_at",
            )
        };

        due_batch_at(&fixture, &runtime, now).await;

        let next_try = at(start, 5, 0, 0);
        let store = runtime.runtime().statement_host();
        let attempts = store.read(super::super::Statement {
            sql: "SELECT state,failure_count,next_try_at FROM engine_work_attempts WHERE machine_kind=? AND subject_key='all'",
            args: vec![text(work_kind)],
        }).await.unwrap().rows;
        assert_eq!(
            attempts,
            vec![vec![text("failing"), integer(1), stamp(next_try)]],
            "{work_kind}"
        );
        assert_eq!(alarm_ms(&runtime), ms(next_try), "{work_kind}");
        let stored = store
            .read(super::super::Statement {
                sql: read_deadlines,
                args: vec![],
            })
            .await
            .unwrap()
            .rows;
        assert_eq!(
            stored,
            vec![vec![stamp(future)], vec![text(corrupt_deadline)]],
            "{work_kind}"
        );
        due_batch_at(&fixture, &runtime, at(start, 4, 59, 0)).await;
        assert_eq!(
            count(&runtime, "SELECT COUNT(*) FROM engine_work_attempts WHERE machine_kind=? AND failure_count=1", vec![text(work_kind)]).await,
            1,
            "the source control overrides the corrupt item until the exact retry"
        );
        assert_eq!(alarm_ms(&runtime), ms(next_try));
        fixture.host.close().await;
    }
}
