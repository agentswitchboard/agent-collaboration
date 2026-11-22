use std::{path::Path, process::Command};

use fleet_actor_identity::{LiveSessionFacts, SessionAvailability};
use fleet_identity_protocol::{
    BindFleetSession, EndFleetSession, EndSessionTarget, RouterEndCause,
};
use sqlx::AssertSqlSafe;
use sqlx::{Connection, SqliteConnection};

use super::fixtures::{FIXED_TIME, Operation, SessionStoreFixture, create, id, reservation};

const FIND_ADDRESS: &str = include_str!("../../src/queries/session_at_address.sql");
const SCOPED_SESSION: &str = include_str!("../../src/queries/session_in_enrollment.sql");
const SCOPED_ENROLLMENT: &str =
    include_str!("../../../fleet-machine-login/src/queries/read_session_enrollment.sql");
const PARENT_SCOPE: &str = include_str!("../../src/queries/parent_scope.sql");

async fn indexed_point_lookups(
    connection: &mut SqliteConnection,
    statement: &'static str,
    bindings: &[&str],
    expected_tables: &[&str],
) {
    let explain = format!("EXPLAIN QUERY PLAN {statement}");
    let mut query = sqlx::query_as::<_, (i64, i64, i64, String)>(AssertSqlSafe(explain));
    for input in bindings {
        query = query.bind(*input);
    }
    let plan = query.fetch_all(connection).await.unwrap();
    assert_eq!(plan.len(), expected_tables.len(), "{plan:?}");
    for ((_, _, _, explanation), table) in plan.iter().zip(expected_tables) {
        assert!(
            explanation.starts_with(&format!("SEARCH {table} ")),
            "{plan:?}"
        );
        assert!(
            explanation.contains(" USING INDEX ") || explanation.contains(" USING COVERING INDEX "),
            "{plan:?}"
        );
        assert!(explanation.contains("=?"), "{plan:?}");
    }
}

// The macOS system SQLite shell reports actual VDBE loop visits. The
// equally permanent SQLx plan checks run on every host, even when its shell
// was built without SQLITE_ENABLE_STMT_SCANSTATUS.
fn sqlite_loop_visits(
    shell: &std::ffi::OsStr,
    database: &Path,
    sql: &str,
    bindings: &[&str],
) -> u64 {
    let mut command = Command::new(shell);
    command.arg("-batch").arg(database).arg(".parameter init");
    for (position, input) in bindings.iter().enumerate() {
        assert!(
            input
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        );
        command.arg(format!(".parameter set ?{} \"'{input}'\"", position + 1));
    }
    let output = command.arg(".scanstats on").arg(sql).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let visits = stdout
        .lines()
        .filter_map(|line| line.rsplit_once("rows="))
        .map(|(_, count)| count.trim_end_matches(')').trim().parse::<u64>().unwrap())
        .collect::<Vec<_>>();
    assert!(!visits.is_empty(), "{stdout}");
    visits.into_iter().sum()
}

#[tokio::test]
async fn address_and_admission_use_unique_point_lookups_at_twenty_and_ten_thousand_rows() {
    for size in [20_i64, 10_000] {
        let mut store = SessionStoreFixture::new().await;
        let mut connection = store.open().await;
        // Fill with rows of the same machine so an unindexed machine scan cannot pass.
        sqlx::query("WITH RECURSIVE fixture_seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fixture_seq WHERE n < ?) INSERT INTO actor_identities (actor_id,actor_kind,created_at,updated_at) SELECT printf('019bc1c0-0000-7000-8000-%012x', n),'session',?,? FROM fixture_seq")
            .bind(size - 1).bind(FIXED_TIME).bind(FIXED_TIME).execute(&mut connection).await.unwrap();
        sqlx::query("WITH RECURSIVE fixture_seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fixture_seq WHERE n < ?) INSERT INTO actor_sessions (actor_id,machine_id,enrollment_id,harness_session_id,create_operation_id,create_contents_digest,create_attempt,agent_program,session_kind,creator_kind,secret_hash,session_state,reserved_until,bound_at,created_at,updated_at) SELECT printf('019bc1c0-0000-7000-8000-%012x', n),?,?,printf('unrelated-%d', n),printf('019cc1c0-0000-7000-8000-%012x', n),'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',1,'codex','ordinary','operator','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','active',?,?,?,? FROM fixture_seq")
            .bind(size - 1).bind(store.caller.machine_id.as_str()).bind(store.caller.enrollment_id.as_str())
            .bind("2026-11-22T10:10:00.000Z").bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME)
            .execute(&mut connection).await.unwrap();
        connection.close().await.unwrap();

        let (session, secret) = reservation(
            store
                .apply(Operation::Create(create(10, 1)), 0)
                .await
                .unwrap(),
        );
        let harness = "session-address";
        store
            .apply(
                Operation::Bind(BindFleetSession {
                    session_id: session.clone(),
                    harness_session_id: harness.parse().unwrap(),
                    secret,
                }),
                1,
            )
            .await
            .unwrap();
        assert_eq!(
            store.count("SELECT COUNT(*) FROM actor_sessions").await,
            size
        );
        assert_eq!(
            store.facts(&session).await,
            SessionAvailability::Live(LiveSessionFacts::Owner)
        );

        let mut connection = store.open().await;
        let machine_id = store.caller.machine_id.clone();
        let enrollment_id = store.caller.enrollment_id.clone();
        let machine = machine_id.as_str();
        let enrollment = enrollment_id.as_str();
        let missing = id(900);
        indexed_point_lookups(
            &mut connection,
            FIND_ADDRESS,
            &[machine, harness],
            &["actor_sessions"],
        )
        .await;
        indexed_point_lookups(
            &mut connection,
            SCOPED_SESSION,
            &[session.as_str(), machine, enrollment],
            &["s", "d"],
        )
        .await;
        indexed_point_lookups(
            &mut connection,
            SCOPED_ENROLLMENT,
            &[enrollment, machine],
            &["a", "m"],
        )
        .await;
        indexed_point_lookups(
            &mut connection,
            PARENT_SCOPE,
            &[session.as_str()],
            &["actor_sessions"],
        )
        .await;
        for (address, expected) in [(harness, Some(session.as_str())), ("missing-address", None)] {
            let found: Option<String> = sqlx::query_scalar(FIND_ADDRESS)
                .bind(machine)
                .bind(address)
                .fetch_optional(&mut connection)
                .await
                .unwrap();
            assert_eq!(found.as_deref(), expected);
        }
        connection.close().await.unwrap();

        let sqlite_shell = std::env::var_os("SESSION_SQLITE_SCAN_CLI");
        let address_and_live_visits = sqlite_shell.as_deref().map(|shell| {
            let address_found = sqlite_loop_visits(
                shell,
                store.database_path(),
                FIND_ADDRESS,
                &[machine, harness],
            );
            let address_missing = sqlite_loop_visits(
                shell,
                store.database_path(),
                FIND_ADDRESS,
                &[machine, "missing-address"],
            );
            let live = sqlite_loop_visits(
                shell,
                store.database_path(),
                SCOPED_SESSION,
                &[session.as_str(), machine, enrollment],
            ) + sqlite_loop_visits(
                shell,
                store.database_path(),
                SCOPED_ENROLLMENT,
                &[enrollment, machine],
            );
            [address_found, address_missing, live]
        });

        store
            .apply(
                Operation::End(EndFleetSession {
                    target: EndSessionTarget::CentralId {
                        session_id: session.clone(),
                    },
                    cause: RouterEndCause::EndedForGood {},
                }),
                2,
            )
            .await
            .unwrap();
        assert_eq!(
            store.facts(&session).await,
            SessionAvailability::Unavailable
        );
        assert_eq!(
            store.facts(&missing.parse().unwrap()).await,
            SessionAvailability::Unavailable
        );
        if let Some((shell, [address_found, address_missing, live])) =
            sqlite_shell.as_deref().zip(address_and_live_visits)
        {
            let ended = sqlite_loop_visits(
                shell,
                store.database_path(),
                SCOPED_SESSION,
                &[session.as_str(), machine, enrollment],
            );
            let absent = sqlite_loop_visits(
                shell,
                store.database_path(),
                SCOPED_SESSION,
                &[&missing, machine, enrollment],
            );
            eprintln!(
                "{size} sessions: address found/missing {address_found}/{address_missing}; admission live/ended/missing {live}/{ended}/{absent}"
            );
            assert!(address_found <= 2 && address_missing <= 1);
            assert!(live <= 5 && ended <= 3 && absent <= 1);
        }
    }
}
