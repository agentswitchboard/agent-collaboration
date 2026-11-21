use super::super::support::Fixture;
use super::engine_probe_support::{integer, runtime};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_conformance::alarm_input_scenarios::{
    ALARM_INPUT_SCENARIOS, CORRUPT_ALARM_INPUT_SCENARIOS,
};

#[tokio::test]
async fn checked_alarm_inputs_match_main_for_every_job() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let store = runtime.statement_host();
    for scenario in CORRUPT_ALARM_INPUT_SCENARIOS
        .iter()
        .chain(ALARM_INPUT_SCENARIOS)
    {
        if !scenario.writes.is_empty() {
            let statements = scenario
                .writes
                .iter()
                .map(|&(sql, count)| GuardedStatement {
                    statement: Statement { sql, args: vec![] },
                    expect: RowExpectation::Exactly(count),
                })
                .collect();
            let batch = store
                .run_batch(StatementBatch {
                    statements,
                    alarm: AlarmUpdate::Unchanged,
                })
                .await
                .unwrap();
            assert!(
                matches!(batch, BatchOutcome::Applied { .. }),
                "{}: {batch:?}",
                scenario.name
            );
        }
        if scenario
            .name
            .starts_with("active schedule with null due date")
        {
            let effective_due_ms = scenario.expected[0].unwrap();
            let key = SqlValue::Text("01890f2e-7b4c-7cc0-98c4-000000000014".into());
            let oracle = include_str!(
                "../../../../../collaboration-central-core/tests/fixtures/main-schedules.sql"
            );
            for (horizon_ms, expected) in [
                (effective_due_ms - 1, vec![]),
                (
                    effective_due_ms,
                    vec![vec![key, integer(0), integer(effective_due_ms)]],
                ),
            ] {
                let previous = store
                    .read(Statement {
                        sql: oracle,
                        args: vec![integer(horizon_ms), integer(250)],
                    })
                    .await
                    .unwrap();
                assert_eq!(previous.rows, expected, "main list: {}", scenario.name);
                let typed: Vec<(String, i64, i64)> = runtime
                    .control("engine-due-schedules", &horizon_ms)
                    .await
                    .unwrap();
                let typed: Vec<_> = typed
                    .into_iter()
                    .map(|(key, occurrence, effective)| {
                        vec![SqlValue::Text(key), integer(occurrence), integer(effective)]
                    })
                    .collect();
                assert_eq!(typed, expected, "typed list: {}", scenario.name);
            }
        }
        let main = store
            .read(Statement {
                sql: collaboration_central_core::ENGINE_ALARM_INPUTS,
                args: vec![],
            })
            .await
            .unwrap();
        let expected_cells = scenario
            .expected
            .map(|value| value.map_or(SqlValue::Null, integer));
        assert_eq!(
            main.rows,
            vec![expected_cells.to_vec()],
            "main: {}",
            scenario.name
        );
        let actual = runtime
            .control::<Result<[Option<i64>; 2], central_storage::StoreFailure>>(
                "engine-alarm-inputs",
                &(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actual, scenario.expected,
            "checked projection: {}",
            scenario.name
        );
    }
    fixture.host.close().await;
}
