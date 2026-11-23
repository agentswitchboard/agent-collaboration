//! Bounded due-drain and eight-wide dispatch on the real host.
use super::super::{Statement, support::*};
use super::engine_event_support::*;
use super::engine_probe_support::*;
use central_storage::SqlValue;
use fleet_runtime::PausePoint;
use router_clock::RuntimeClock;

/// Instance `index` is due 40 ms after the one before it, from 10:04:10, so all
/// are due by the first batch at 10:05:00 and the earliest-first order is known.
fn due_of(start: Time, index: u32) -> Time {
    at(start, 4, 10, i64::from(index) * 40)
}
async fn fired_items(runtime: &Runtime, group: uuid::Uuid) -> Vec<SqlValue> {
    runtime
        .runtime().statement_host()
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

struct SharedDueDrainWitness<'a> {
    fixture: &'a Fixture,
    runtime: &'a Runtime,
    start: Time,
    group: uuid::Uuid,
}

impl collaboration_central_conformance::due_drain_scenarios::DueDrainWitness
    for SharedDueDrainWitness<'_>
{
    type Key = SqlValue;

    async fn drive_first_mark(&self) {
        due_batch_at(self.fixture, self.runtime, at(self.start, 5, 0, 0)).await;
    }

    async fn drive_next_mark(&self) {
        let at = at(self.start, 10, 0, 0);
        driver_fires_at(self.fixture, self.runtime, at).await;
    }

    async fn read_pending_alarm(&self) -> Option<i64> {
        alarm_ms(self.runtime)
    }

    async fn read_effect_count(&self) -> i64 {
        count(
            self.runtime,
            "SELECT effect_count FROM engine_probe_groups WHERE group_id=?",
            vec![text(self.group)],
        )
        .await
    }

    async fn read_finished_keys(&self) -> Vec<SqlValue> {
        fired_items(self.runtime, self.group).await
    }
}

#[tokio::test]
async fn a_due_drain_processes_four_batches_and_arms_the_next_mark_for_its_tail() {
    use collaboration_central_conformance::due_drain_scenarios::{
        OVERFLOW_POPULATION, capped_drain_hands_its_tail_to_the_next_mark,
    };

    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
    let group = probe_id(40000);
    let items: Vec<_> = (0..OVERFLOW_POPULATION)
        .map(|index| probe_id(41000 + index))
        .collect();
    for (index, item) in (0..OVERFLOW_POPULATION).zip(&items) {
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
    let witness = SharedDueDrainWitness {
        fixture: &fixture,
        runtime: &runtime,
        start,
        group,
    };
    capped_drain_hands_its_tail_to_the_next_mark(
        &witness,
        &expected,
        at(start, 10, 0, 0).timestamp_millis(),
    )
    .await;
    assert_eq!(
        count(
            &runtime,
            "SELECT COUNT(*) FROM engine_work_attempts WHERE machine_kind='engine_probe'",
            vec![]
        )
        .await,
        0
    );
    fixture.host.close().await;
}

#[tokio::test]
async fn eight_held_units_fill_the_fan_out_and_no_ninth_starts() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let start = fixture.clock.utc_now();
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
    let mut holds: Vec<_> = items[..8]
        .iter()
        .map(|item| {
            runtime
                .runtime()
                .pause_points()
                .hold_engine(PausePoint::AfterEngineLoad, &item.to_string())
        })
        .collect();
    driver_settled(&fixture, &runtime).await;
    let before = runtime.runtime().legacy_unit_invocations();
    fixture.set_utc(at(start, 5, 0, 0)).await;
    let batch = tokio::spawn({
        let runtime = runtime.clone();
        async move { runtime.after_event().await }
    });
    for held in &mut holds {
        held.until_held().await;
    }
    // Give a ninth unit every chance to start and commit while all eight are held.
    for _ in 0..32 {
        tokio::task::yield_now().await;
        assert!(
            fired_items(&runtime, group).await.is_empty(),
            "a ninth unit ran while eight were in flight"
        );
    }
    let invocations = runtime.runtime().legacy_unit_invocations();
    assert_eq!(
        (invocations.windows, invocations.drains),
        (before.windows, before.drains),
        "the notice units queue behind the eight as well"
    );

    drop(holds);
    batch.await.unwrap().unwrap();

    assert_eq!(fired_items(&runtime, group).await.len(), 20);
    fixture.host.close().await;
}
