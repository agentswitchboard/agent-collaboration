//! V9 on workerd: 1,000 due probe instances of one kind complete in four capped
//! due batches on consecutive marks through real alarm callbacks, earliest first and each
//! once, with at most eight units in flight. Timings print as measurement only.
use super::super::{Statement, support::*};
use super::engine_event_support::*;
use super::engine_probe_support::*;
use central_storage::SqlValue;
use fleet_runtime::PausePoint;
use workspace_model::central_mail::{
    CentralMailAnswer, CentralMailOperation, CentralMailRequest, CentralMailResult, NoticeCadence,
};

const INSTANCES: u32 = 1_000;
const CAP: usize = 250;

async fn submit_ordinary_request(fixture: &Fixture, request: u32) {
    let result = fixture
        .host
        .request_central(
            &fixture.caller,
            CentralMailRequest {
                request_id: id(request),
                operation: CentralMailOperation::SetNoticeCadence {
                    cadence: NoticeCadence::M15,
                },
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        result,
        CentralMailResult::Done(CentralMailAnswer::NoticeCadenceSet { .. })
    ));
}

/// Instance `index` is due 40 ms after the one before it, from 10:04:10, so all
/// are due by the first batch at 10:05:00 and the earliest-first order is known.
fn due_of(start: Time, index: u32) -> Time {
    at(start, 4, 10, i64::from(index) * 40)
}
async fn fired_items(runtime: &Runtime, group: uuid::Uuid) -> Vec<SqlValue> {
    runtime
        .statement_host()
        .read(Statement {
            sql: "SELECT item_id FROM engine_probe_items WHERE group_id=? AND state='fired' ORDER BY due_at,item_id",
            args: vec![text(group)],
        })
        .await
        .unwrap()
        .rows
        .into_iter()
        .map(|mut row| row.remove(0))
        .collect()
}

#[tokio::test]
async fn a_thousand_due_instances_complete_in_four_capped_batches_on_consecutive_marks() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let start = now(&fixture);
    let group = probe_id(40000);
    let items: Vec<_> = (0..INSTANCES)
        .map(|index| probe_id(41000 + index))
        .collect();
    for (index, item) in (0..INSTANCES).zip(&items) {
        seed_probe(
            &runtime,
            *item,
            group,
            Some(due_of(start, index)),
            &format!("bulk{index}"),
        )
        .await;
    }
    let expected: Vec<_> = items.iter().map(text).collect();
    let before = callbacks(&runtime).await;
    let mut measurements = Vec::new();

    for (batch, (minutes, seconds)) in [(5, 0), (10, 0), (15, 0), (20, 0)].into_iter().enumerate() {
        let time = at(start, minutes, seconds, 0);
        let started = std::time::Instant::now();
        if batch == 0 {
            // Seed rows first, then publish and fire the first physical alarm.
            set_now(&fixture, at(start, 3, 0, 0));
            submit_ordinary_request(&fixture, 40002).await;
            assert_eq!(alarm_ms(&runtime).await, ms(time));
            set_now(&fixture, time);
            assert!(runtime.dispatch_due_alarm().await.unwrap().is_some());
        } else {
            assert_eq!(
                alarm_ms(&runtime).await,
                ms(time),
                "batch {batch} is the continuation at the next mark"
            );
            set_now(&fixture, time - chrono::Duration::milliseconds(1));
            assert!(runtime.dispatch_due_alarm().await.unwrap().is_none());
            set_now(&fixture, time);
            assert!(runtime.dispatch_due_alarm().await.unwrap().is_some());
        }
        measurements.push(started.elapsed());
        assert_eq!(
            fired_items(&runtime, group).await,
            expected[..CAP * (batch + 1)],
            "batch {batch} handled exactly the next 250, earliest first"
        );
        if batch == 0 {
            set_now(&fixture, at(start, 5, 31, 0));
            submit_ordinary_request(&fixture, 40003).await;
            assert_eq!(fired_items(&runtime, group).await, expected[..CAP]);
            assert_eq!(alarm_ms(&runtime).await, ms(at(start, 10, 0, 0)));
        }
    }

    assert_eq!(callbacks(&runtime).await, before + 4, "four callbacks");
    assert_eq!(
        alarm_ms(&runtime).await,
        None,
        "nothing is pending afterwards"
    );
    assert_eq!(
        count(
            &runtime,
            "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
            vec![text(group)]
        )
        .await,
        i64::from(INSTANCES),
        "each instance's effect ran once"
    );
    assert_eq!(
        count(
            &runtime,
            "SELECT COUNT(*) FROM engine_work_attempts WHERE machine_kind='engine_probe'",
            vec![]
        )
        .await,
        0
    );
    for (batch, elapsed) in measurements.iter().enumerate() {
        println!(
            "V9 measurement workerd batch {batch}: {CAP} units in {:.3} ms, {:.3} ms per unit (includes the test transport)",
            elapsed.as_secs_f64() * 1e3,
            elapsed.as_secs_f64() * 1e3 / CAP as f64
        );
    }
    fixture.host.close().await;
}

#[tokio::test]
async fn cold_reopen_can_take_the_capped_tail_on_a_request_and_keeps_alarms_on_grid() {
    let mut fixture = Fixture::new().await;
    let opened = runtime(&fixture).await;
    let start = now(&fixture);
    let group = probe_id(40004);
    let items: Vec<_> = (0..=u32::try_from(CAP).unwrap())
        .map(|index| probe_id(43000 + index))
        .collect();
    for (index, item) in items.iter().enumerate() {
        seed_probe(
            &opened,
            *item,
            group,
            Some(due_of(start, u32::try_from(index).unwrap())),
            &format!("cold{index}"),
        )
        .await;
    }
    seed_probe(
        &opened,
        probe_id(44000),
        group,
        Some(at(start, 16, 1, 0)),
        "future off-grid due",
    )
    .await;

    set_now(&fixture, at(start, 3, 0, 0));
    submit_ordinary_request(&fixture, 40005).await;
    let first_mark = at(start, 5, 0, 0);
    assert_eq!(alarm_ms(&opened).await, ms(first_mark));
    set_now(&fixture, first_mark);
    assert!(opened.dispatch_due_alarm().await.unwrap().is_some());
    let expected: Vec<_> = items.iter().map(text).collect();
    assert_eq!(fired_items(&opened, group).await, expected[..CAP]);
    let next_mark = at(start, 10, 0, 0);
    assert_eq!(alarm_ms(&opened).await, ms(next_mark));

    let between_marks = at(start, 5, 31, 0);
    set_now(&fixture, between_marks);
    assert!(opened.control::<bool>("raw-drop-alarm", &()).await.unwrap());
    assert_eq!(alarm_ms(&opened).await, None);
    drop(opened);
    fixture.reopen().await;
    let reopened = runtime(&fixture).await;
    assert_eq!(fired_items(&reopened, group).await, expected[..CAP]);
    assert_eq!(alarm_ms(&reopened).await, ms(next_mark), "cold publication");

    submit_ordinary_request(&fixture, 40006).await;
    assert_eq!(fired_items(&reopened, group).await, expected);
    assert_eq!(alarm_ms(&reopened).await, ms(at(start, 20, 0, 0)));
    fixture.host.close().await;
}

#[tokio::test]
async fn eight_held_units_fill_the_fan_out_and_no_ninth_starts() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let start = now(&fixture);
    let group = probe_id(40001);
    let items: Vec<_> = (0..20).map(|index| probe_id(42000 + index)).collect();
    for (index, item) in (0..20).zip(&items) {
        seed_probe(
            &runtime,
            *item,
            group,
            Some(due_of(start, index)),
            &format!("wide{index}"),
        )
        .await;
    }
    let mut holds = Vec::new();
    for item in &items[..8] {
        holds.push(
            runtime
                .pause_points()
                .hold_engine(PausePoint::AfterEngineLoad, &item.to_string())
                .await,
        );
    }
    let before = runtime.legacy_unit_invocations().await;
    set_now(&fixture, at(start, 5, 0, 0));
    let batch = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    for held in &mut holds {
        held.until_held().await;
    }
    // Each read is another event that lets a ninth unit run if one had started.
    for _ in 0..16 {
        assert!(
            fired_items(&runtime, group).await.is_empty(),
            "a ninth unit ran while eight were in flight"
        );
    }
    assert_eq!(
        runtime.legacy_unit_invocations().await,
        before,
        "the notice units queue behind the eight as well"
    );

    for held in holds {
        held.release().await;
    }
    batch.await.unwrap().unwrap();

    assert_eq!(fired_items(&runtime, group).await.len(), 20);
    fixture.host.close().await;
}
