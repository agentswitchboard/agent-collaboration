use std::{ffi::OsString, path::Path, process::Command};

use fleet_actor_identity::{LiveSessionFacts, SessionAvailability};
use fleet_identity_protocol::{
    BindFleetSession, CreateSessionKind, EndFleetSession, EndSessionTarget, PresentedSessionSecret,
    RouterEndCause, SessionCreator,
};
use sqlx::{Arguments, AssertSqlSafe, Connection, Row, SqliteConnection};

use super::{
    enrollment_endings::replace_enrollment,
    fixtures::{FIXED_TIME, Operation, SessionStoreFixture, create, id, reservation},
};

const FIND_ADDRESS: &str = include_str!("../../src/queries/session_at_address.sql");

struct BudgetSample {
    _directory: tempfile::TempDir,
    snapshot: std::path::PathBuf,
}

impl BudgetSample {
    fn begin(store: &SessionStoreFixture) -> Self {
        store.dispatched_reads();
        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("before-operation.sqlite");
        std::fs::copy(store.database_path(), &snapshot).unwrap();
        Self {
            _directory: directory,
            snapshot,
        }
    }

    async fn count(self, store: &SessionStoreFixture, address_only: bool) -> u64 {
        let shell = std::env::var_os("SESSION_SQLITE_SCAN_CLI")
            .unwrap_or_else(|| OsString::from("sqlite3"));
        let reads = store.dispatched_reads();
        let selected: Vec<_> = reads
            .into_iter()
            .filter(|read| !address_only || read.sql.trim() == FIND_ADDRESS.trim())
            .collect();
        assert!(
            !selected.is_empty(),
            "the service dispatched no measured reads"
        );
        if address_only {
            assert_eq!(selected.len(), 1, "address lookup dispatch count");
        }
        let mut counted = 0;
        let mut binding_reader = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        for read in selected {
            let bindings = quoted_bindings(&mut binding_reader, read.arguments).await;
            counted += sqlite_loop_visits(&shell, &self.snapshot, &read.sql, &bindings);
        }
        counted
    }
}

// Decode the original encoded SQLx arguments without reconstructing their identities.
async fn quoted_bindings(
    connection: &mut SqliteConnection,
    arguments: Option<sqlx::sqlite::SqliteArguments>,
) -> Vec<String> {
    let Some(arguments) = arguments else {
        return Vec::new();
    };
    let count = arguments.len();
    if count == 0 {
        return Vec::new();
    }
    let fields = (1..=count)
        .map(|index| format!("quote(?{index})"))
        .collect::<Vec<_>>()
        .join(", ");
    let row = sqlx::query_with(AssertSqlSafe(format!("SELECT {fields}")), arguments)
        .fetch_one(connection)
        .await
        .unwrap();
    (0..count).map(|index| row.get(index)).collect()
}

// The case owns an isolated pre-operation snapshot. Replaying the observed reads
// with their original binds preserves an address miss even when Bind inserts it.
fn sqlite_loop_visits(shell: &OsString, database: &Path, sql: &str, bindings: &[String]) -> u64 {
    let mut command = Command::new(shell);
    command
        .arg("-batch")
        .arg("-readonly")
        .arg(database)
        .arg(".parameter init");
    for (position, quoted) in bindings.iter().enumerate() {
        let escaped = quoted.replace('\\', "\\\\").replace('"', "\\\"");
        command.arg(format!(".parameter set ?{} \"{escaped}\"", position + 1));
    }
    let output = command
        .arg(".scanstats on")
        .arg(sql)
        .output()
        .unwrap_or_else(|error| {
            panic!("native row counter requires a SQLite 3 shell with scan status: {error}")
        });
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
    assert!(
        !visits.is_empty(),
        "SQLite scan status did not report loop visits for {sql:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    visits.into_iter().sum()
}

#[tokio::test]
async fn service_dispatches_bounded_reads_at_twenty_and_ten_thousand_rows() {
    for size in [20_i64, 10_000] {
        let mut store = SessionStoreFixture::new().await;
        let mut connection = store.open().await;
        // Fill with rows of the same machine so an unindexed machine scan cannot pass.
        sqlx::query("WITH RECURSIVE fixture_seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fixture_seq WHERE n < ?) INSERT INTO actor_identities (actor_id,actor_kind,created_at,updated_at) SELECT printf('019bc1c0-0000-7000-8000-%012x', n),'session',?,? FROM fixture_seq")
            .bind(size - 2).bind(FIXED_TIME).bind(FIXED_TIME).execute(&mut connection).await.unwrap();
        sqlx::query("WITH RECURSIVE fixture_seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fixture_seq WHERE n < ?) INSERT INTO actor_sessions (actor_id,machine_id,enrollment_id,harness_session_id,create_operation_id,create_contents_digest,create_attempt,agent_program,session_kind,creator_kind,secret_hash,session_state,reserved_until,bound_at,created_at,updated_at,workspace_id,project_id) SELECT printf('019bc1c0-0000-7000-8000-%012x', n),?,?,printf('unrelated-%d', n),printf('019cc1c0-0000-7000-8000-%012x', n),'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',1,'codex','ordinary','operator','AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA','active',?,?,?,?,'019a3dce-2000-7000-8000-000000000004','019a3dce-2000-7000-8000-000000000005' FROM fixture_seq")
            .bind(size - 2).bind(store.caller.machine_id.as_str()).bind(store.caller.enrollment_id.as_str())
            .bind("2026-11-22T10:10:00.000Z").bind(FIXED_TIME).bind(FIXED_TIME).bind(FIXED_TIME)
            .execute(&mut connection).await.unwrap();
        connection.close().await.unwrap();

        let (parent, parent_secret) = reservation(
            store
                .apply(Operation::Create(create(10, 1)), 0)
                .await
                .unwrap(),
        );
        let sample = BudgetSample::begin(&store);
        store
            .apply(
                Operation::Bind(BindFleetSession {
                    session_id: parent.clone(),
                    harness_session_id: "budget-parent".parse().unwrap(),
                    secret: parent_secret.clone(),
                }),
                0,
            )
            .await
            .unwrap();
        let address_missing = sample.count(&store, true).await;
        assert!(address_missing <= 1);

        let mut delegate_create = create(11, 1);
        delegate_create.creator = SessionCreator::Session {
            session_id: parent.clone(),
            presented: PresentedSessionSecret::Secret {
                secret: parent_secret,
            },
        };
        delegate_create.session_kind = CreateSessionKind::Delegate {
            parent: parent.clone(),
        };
        let (delegate, delegate_secret) = reservation(
            store
                .apply(Operation::Create(delegate_create), 0)
                .await
                .unwrap(),
        );
        let sample = BudgetSample::begin(&store);
        store
            .apply(
                Operation::Bind(BindFleetSession {
                    session_id: delegate.clone(),
                    harness_session_id: "budget-parent".parse().unwrap(),
                    secret: delegate_secret.clone(),
                }),
                0,
            )
            .await
            .unwrap();
        let address_found = sample.count(&store, true).await;
        assert!(address_found <= 2);
        store
            .apply(
                Operation::Bind(BindFleetSession {
                    session_id: delegate.clone(),
                    harness_session_id: "budget-child".parse().unwrap(),
                    secret: delegate_secret,
                }),
                0,
            )
            .await
            .unwrap();

        let sample = BudgetSample::begin(&store);
        assert!(
            matches!(store.facts(&delegate).await, SessionAvailability::Live(LiveSessionFacts::Delegate { parent: recorded })
            if recorded.session_id == parent)
        );
        let live_delegate = sample.count(&store, false).await;
        assert!(live_delegate <= 5);

        let sample = BudgetSample::begin(&store);
        assert_eq!(
            store.facts(&parent).await,
            SessionAvailability::Live(LiveSessionFacts::Owner)
        );
        let live = sample.count(&store, false).await;
        assert!(live <= 5);
        store
            .apply(
                Operation::End(EndFleetSession {
                    target: EndSessionTarget::CentralId {
                        session_id: parent.clone(),
                    },
                    cause: RouterEndCause::EndedForGood {},
                }),
                1,
            )
            .await
            .unwrap();
        let sample = BudgetSample::begin(&store);
        assert_eq!(store.facts(&parent).await, SessionAvailability::Unavailable);
        let ended = sample.count(&store, false).await;
        assert!(ended <= 3);
        let sample = BudgetSample::begin(&store);
        assert_eq!(
            store.facts(&id(999).parse().unwrap()).await,
            SessionAvailability::Unavailable
        );
        let missing = sample.count(&store, false).await;
        assert!(missing <= 1);

        let prior_enrollment = store.caller.enrollment_id.clone();
        replace_enrollment(&mut store, "idle", "2026-11-22T10:00:00.002Z").await;
        store.caller.enrollment_id = prior_enrollment;
        let sample = BudgetSample::begin(&store);
        assert_eq!(
            store.facts(&delegate).await,
            SessionAvailability::Unavailable
        );
        let ended_delegate = sample.count(&store, false).await;
        assert!(ended_delegate <= 3);
        println!(
            "{size} sessions: address found/missing {address_found}/{address_missing}; ordinary admission live/ended/missing {live}/{ended}/{missing}; delegate admission live/ended-enrollment {live_delegate}/{ended_delegate}"
        );
    }
}
