use super::super::support::Fixture;
use super::engine_probe_support::{integer, runtime};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, RowExpectation, SqlValue, Statement,
    StatementBatch,
};
use collaboration_central_conformance::alarm_input_scenarios::{
    ALARM_INPUT_SCENARIOS, CORRUPT_ALARM_INPUT_SCENARIOS,
};
use fleet_runtime_ports::CentralStore as _;

#[tokio::test]
async fn checked_alarm_inputs_match_main_for_every_job() {
    let fixture = Fixture::new().await;
    let runtime = runtime(&fixture).await;
    let store = runtime.runtime().statement_host();
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
            for sql in [
                include_str!(
                    "../../../../../collaboration-central-core/tests/fixtures/main-schedules.sql"
                ),
                include_str!("../../../../../fleet-schedules/src/queries/read_due_schedules.sql"),
            ] {
                let at_deadline = store
                    .read(Statement {
                        sql,
                        args: vec![integer(effective_due_ms), integer(250)],
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    at_deadline.rows,
                    vec![vec![key.clone(), integer(0), integer(effective_due_ms)]]
                );
                let before = store
                    .read(Statement {
                        sql,
                        args: vec![integer(effective_due_ms - 1), integer(250)],
                    })
                    .await
                    .unwrap();
                assert!(before.rows.is_empty());
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
        let actual = {
            let times = store.engine_alarm_inputs().await.unwrap();
            [
                times.earliest_on_grid_due_unix_millis,
                times.earliest_legacy_due_unix_millis,
            ]
        };
        assert_eq!(
            actual, scenario.expected,
            "checked projection: {}",
            scenario.name
        );
    }
    fixture.host.close().await;
}
