//! Observe the real SQLite-backed Worker alarm publication for an approval.
use super::witness_error;
use serde_json::json;
use sqlx_durable_object::DoConnection;
use std::sync::Arc;
use worker::{Response, State};

pub(super) async fn publish_alarm_mark(
    state: &State,
    connection: &DoConnection,
    now_ms: i64,
) -> worker::Result<Response> {
    use crate::statement_store::{AlarmPolicy, run_batch};
    use central_storage::{
        AlarmPublication, AlarmUpdate, BatchOutcome, EngineAlarmPlan, GuardedStatement,
        RowExpectation, Statement, StatementBatch,
    };

    let clock = crate::worker_clock::WorkspaceClock::default();
    clock.hold(now_ms);
    let decision = fleet_runtime::engine_alarm_decider(Arc::default());
    let batch = StatementBatch {
        statements: vec![GuardedStatement {
            statement: Statement {
                sql: include_str!(
                    "../../../../fleet-approvals/tests/fixtures/approval_alarm_provisioned.sql"
                ),
                args: vec![],
            },
            expect: RowExpectation::Any,
        }],
        alarm: AlarmUpdate::Engine(EngineAlarmPlan::Recompute),
    };
    let outcome = run_batch(
        connection,
        batch.into(),
        AlarmPolicy::Engine(state.storage(), clock, decision),
    )
    .await
    .map_err(witness_error)?;
    if !matches!(
        outcome,
        BatchOutcome::Applied {
            publication: AlarmPublication::Set { .. },
            ..
        }
    ) {
        return Err(witness_error("approval fixture alarm did not publish"));
    }
    Response::from_json(&json!({ "alarmMs": state.storage().get_alarm().await? }))
}
