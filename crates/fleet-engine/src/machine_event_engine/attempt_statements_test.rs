use super::{plan_control_attempt_upsert, plan_guarded_attempt_upsert};
use central_engine_model::{
    EngineTime, FailureRecord, InstanceKey, ItemFailureClass, MachineKind, RetirementThreshold,
    after_failure,
};
use central_storage::{GuardedStatement, SqlValue};
use fleet_contracts::{AttemptGuard, EngineSubject};
use sqlx::{Connection, Row, SqliteConnection};

const UPSERT_ACTIVE_WAKE_ATTEMPT_SQL: &str = guarded_attempt_upsert_sql!(
    "EXISTS(SELECT 1 FROM automation_wakes WHERE wake_id=?20 AND state='active')"
);
const FIXTURE_WAKE_ID: &str = "01890f2e-7b4c-7cc0-98c4-000000000002";

fn sql_text(value: &str) -> SqlValue {
    SqlValue::Text(value.into())
}
fn make_attempt_subject(kind: &'static str, key: &str) -> EngineSubject {
    EngineSubject {
        kind: MachineKind(kind),
        key: InstanceKey(key.into()),
    }
}
fn next_attempt_failure_record(prior: Option<&FailureRecord>, now: i64) -> FailureRecord {
    match after_failure(
        prior,
        ItemFailureClass::StatementRejected,
        EngineTime::from_timestamp_millis(now).unwrap(),
        RetirementThreshold::DEFAULT,
        "01890f2e-7b4c-7cc0-98c4-000000000001".parse().unwrap(),
    )
    .unwrap()
    {
        central_engine_model::FailureTransition::Record(record) => record,
        central_engine_model::FailureTransition::Unchanged => panic!("the failure must count"),
    }
}
async fn open_migrated_central_database() -> SqliteConnection {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    fleet_storage_schema::workspace_migrations::central_migrator()
        .run(&mut connection)
        .await
        .unwrap();
    connection
}
async fn execute_attempt_statement(
    connection: &mut SqliteConnection,
    statement: GuardedStatement,
) -> u64 {
    let mut query = sqlx::query(statement.statement.sql);
    for value in statement.statement.args {
        query = match value {
            SqlValue::Null => query.bind(None::<i64>),
            SqlValue::Integer(value) => query.bind(value.get()),
            SqlValue::Text(value) => query.bind(value),
            SqlValue::Blob(value) => query.bind(value),
        };
    }
    query.execute(connection).await.unwrap().rows_affected()
}
async fn read_attempt_guard(connection: &mut SqliteConnection) -> AttemptGuard {
    let row = sqlx::query(
        "SELECT attempt_id,state,failure_count,last_failure,updated_at FROM engine_work_attempts",
    )
    .fetch_one(connection)
    .await
    .unwrap();
    AttemptGuard {
        attempt_id: SqlValue::Text(row.try_get("attempt_id").unwrap()),
        state: SqlValue::Text(row.try_get("state").unwrap()),
        failure_count: SqlValue::Integer(
            row.try_get::<i64, _>("failure_count")
                .unwrap()
                .try_into()
                .unwrap(),
        ),
        last_failure: SqlValue::Text(row.try_get("last_failure").unwrap()),
        updated_at: SqlValue::Text(row.try_get("updated_at").unwrap()),
    }
}

#[tokio::test]
async fn an_old_attempt_count_cannot_replace_the_newer_record() {
    let mut connection = open_migrated_central_database().await;
    let subject = make_attempt_subject("engine_source", "wake_timer");
    let first = next_attempt_failure_record(None, 300_000);
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_control_attempt_upsert(&subject, &first, None).unwrap()
        )
        .await,
        1
    );
    let prior = read_attempt_guard(&mut connection).await;
    let second = next_attempt_failure_record(Some(&first), 600_000);
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_control_attempt_upsert(&subject, &second, Some(&prior),).unwrap()
        )
        .await,
        1
    );
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_control_attempt_upsert(&subject, &second, Some(&prior),).unwrap()
        )
        .await,
        0
    );
    let row = sqlx::query("SELECT state,failure_count,next_try_at FROM engine_work_attempts")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>("state").unwrap(), "retired");
    assert_eq!(row.try_get::<i64, _>("failure_count").unwrap(), 2);
    assert!(
        row.try_get::<Option<String>, _>("next_try_at")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_deleted_control_record_is_not_recreated_by_an_old_failure() {
    let mut connection = open_migrated_central_database().await;
    let subject = make_attempt_subject("engine_source", "wake_timer");
    let first = next_attempt_failure_record(None, 300_000);
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_control_attempt_upsert(&subject, &first, None).unwrap()
        )
        .await,
        1
    );
    let prior = read_attempt_guard(&mut connection).await;
    sqlx::query("DELETE FROM engine_work_attempts")
        .execute(&mut connection)
        .await
        .unwrap();
    let second = next_attempt_failure_record(Some(&first), 600_000);
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_control_attempt_upsert(&subject, &second, Some(&prior),).unwrap()
        )
        .await,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM engine_work_attempts")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn a_failure_can_be_recorded_without_retriggering_the_subject_and_cannot_resurrect_it() {
    let mut connection = open_migrated_central_database().await;
    sqlx::query("INSERT INTO automation_wakes(wake_id,target,due_at,text,state,created_at,updated_at) VALUES (?,'not-json','1970-01-01T00:05:00.000Z','broken','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')").bind(FIXTURE_WAKE_ID).execute(&mut connection).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_subject_update BEFORE UPDATE ON automation_wakes BEGIN SELECT RAISE(ABORT,'reject subject'); END").execute(&mut connection).await.unwrap();
    let subject = make_attempt_subject("wake_timer", FIXTURE_WAKE_ID);
    let first = next_attempt_failure_record(None, 300_000);
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_guarded_attempt_upsert(
                UPSERT_ACTIVE_WAKE_ATTEMPT_SQL,
                &subject,
                &first,
                None,
                vec![sql_text(FIXTURE_WAKE_ID)]
            )
            .unwrap()
        )
        .await,
        1
    );
    let prior = read_attempt_guard(&mut connection).await;
    sqlx::query("DROP TRIGGER reject_subject_update")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("UPDATE automation_wakes SET state='disabled' WHERE wake_id=?")
        .bind(FIXTURE_WAKE_ID)
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("DELETE FROM engine_work_attempts")
        .execute(&mut connection)
        .await
        .unwrap();
    let second = next_attempt_failure_record(Some(&first), 600_000);
    assert_eq!(
        execute_attempt_statement(
            &mut connection,
            plan_guarded_attempt_upsert(
                UPSERT_ACTIVE_WAKE_ATTEMPT_SQL,
                &subject,
                &second,
                Some(&prior),
                vec![sql_text(FIXTURE_WAKE_ID)]
            )
            .unwrap()
        )
        .await,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM engine_work_attempts")
            .fetch_one(&mut connection)
            .await
            .unwrap(),
        0
    );
}
