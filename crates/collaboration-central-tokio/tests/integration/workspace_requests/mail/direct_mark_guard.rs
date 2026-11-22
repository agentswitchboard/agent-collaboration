use sqlx::{Connection, SqliteConnection};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

const GUARD: &str = include_str!("../../../../../fleet-mail/src/queries/read_due_mark_guard.sql");
const PLAN: &str = concat!(
    "EXPLAIN QUERY PLAN ",
    include_str!("../../../../../fleet-mail/src/queries/read_due_mark_guard.sql")
);
const BASELINE_GUARD: &str =
    include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_baseline.sql");
const SEED_PENDING_MARKS: &str =
    include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_seed.sql");
const SEED_CASE_MESSAGES: &str =
    include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_case_messages.sql");
const SEED_CASE_ATTEMPTS: &str =
    include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_case_attempts.sql");
const SEED_SOURCE_HOLD: &str =
    include_str!("../../../../../fleet-mail/tests/fixtures/direct_mark_guard_case_source_hold.sql");
const SUBJECT: &str = "019f0000-0000-7000-8000-000000000001";

async fn migrated_store() -> SqliteConnection {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    fleet_storage_schema::workspace_migrations::central_migrator()
        .run(&mut connection)
        .await
        .unwrap();
    sqlx::query("INSERT INTO login_machines(machine_id,label,state,allowed_workspaces,created_at,updated_at) VALUES ('019f0000-0000-7000-8000-000000000002','fixture','enrolled','[]','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')")
        .execute(&mut connection).await.unwrap();
    sqlx::query("INSERT INTO mail_messages(message_id,sender,target_machine_id,target_session,message_kind,body,state,created_at,updated_at,cancel_due_at) VALUES (?,'sender','019f0000-0000-7000-8000-000000000002','session','direct','{}','queued','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z','1970-01-01T00:01:00.000Z')")
        .bind(SUBJECT).execute(&mut connection).await.unwrap();
    connection
}

async fn guard_answer(
    connection: &mut SqliteConnection,
    statement: &'static str,
    subject: &str,
) -> Option<(i64, i64)> {
    sqlx::query_as(statement)
        .bind(subject)
        .fetch_optional(connection)
        .await
        .unwrap()
}

async fn guard_steps(connection: &mut SqliteConnection) -> u64 {
    let steps = Arc::new(AtomicU64::new(0));
    let counted_steps = Arc::clone(&steps);
    connection
        .lock_handle()
        .await
        .unwrap()
        .set_progress_handler(1, move || {
            counted_steps.fetch_add(1, Ordering::Relaxed);
            true
        });
    // Fetch all so a partial cursor cannot hide a scan after the one matching row.
    let rows = sqlx::query_as::<_, (i64, i64)>(GUARD)
        .bind(SUBJECT)
        .fetch_all(&mut *connection)
        .await
        .unwrap();
    connection
        .lock_handle()
        .await
        .unwrap()
        .remove_progress_handler();
    assert_eq!(rows, [(60_000, 60_000)]);
    steps.load(Ordering::Relaxed)
}

#[tokio::test]
async fn the_direct_mark_guard_keeps_sqlite_work_bounded_as_marks_grow() {
    let mut measured = Vec::new();
    for marks in [20_i64, 10_000] {
        let mut connection = migrated_store().await;
        sqlx::query(SEED_PENDING_MARKS)
            .bind(marks)
            .bind(SUBJECT)
            .execute(&mut connection)
            .await
            .unwrap();
        let actual: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM mail_messages WHERE cancel_due_at IS NOT NULL",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert_eq!(actual, marks);
        let plan = sqlx::query_as::<_, (i64, i64, i64, String)>(PLAN)
            .bind(SUBJECT)
            .fetch_all(&mut connection)
            .await
            .unwrap();
        let steps = guard_steps(&mut connection).await;
        println!(
            "direct mark guard: {marks} marks, {steps} SQLite VM progress callbacks, {plan:?}"
        );
        measured.push(steps);
    }
    assert_eq!(measured[0], measured[1], "guard work grew: {measured:?}");
    assert!((1..=150).contains(&measured[0]), "guard work: {measured:?}");
}

#[tokio::test]
async fn the_direct_mark_guard_matches_the_due_view_for_eligibility_and_attempts() {
    let mut connection = migrated_store().await;
    sqlx::query(SEED_CASE_MESSAGES)
        .bind(SUBJECT)
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query(SEED_CASE_ATTEMPTS)
        .execute(&mut connection)
        .await
        .unwrap();
    let cases = [
        Some((60_000, 60_000)),   // queued
        None,                     // no longer eligible
        Some((120_001, 120_001)), // changed occurrence, including fractional time
        Some((0, 0)),             // malformed stored mark
        Some((60_000, 180_000)),  // attempt supplies the effective due time only
        None,                     // retired
        None,                     // null mark
        None,                     // other mail kind
        Some((60_000, 0)),        // malformed retry mark
        None,                     // missing subject
    ];
    for (index, expected) in cases.into_iter().enumerate() {
        let subject = format!("019d0000-0000-7000-8000-{:012}", index + 1);
        assert_eq!(
            guard_answer(&mut connection, BASELINE_GUARD, &subject).await,
            expected
        );
        assert_eq!(
            guard_answer(&mut connection, GUARD, &subject).await,
            expected
        );
    }
    sqlx::query(SEED_SOURCE_HOLD)
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        guard_answer(&mut connection, BASELINE_GUARD, SUBJECT).await,
        None
    );
    assert_eq!(guard_answer(&mut connection, GUARD, SUBJECT).await, None);
}
