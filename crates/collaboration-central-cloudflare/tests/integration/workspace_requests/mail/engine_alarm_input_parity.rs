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
