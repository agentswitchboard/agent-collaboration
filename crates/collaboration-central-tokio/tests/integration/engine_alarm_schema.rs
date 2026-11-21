use crate::storage_support::HeldClock;
use central_storage::{
    AlarmPublication, AlarmUpdate, BatchOutcome, EngineAlarmPlan, GuardedStatement, RowExpectation,
    SqlValue, Statement, StatementBatch,
};
use collaboration_central_tokio::ObjectStorage;
use sqlx::migrate::{Migration, MigrationType, Migrator};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt};

const STAMP: &str = "INSERT INTO signin_organization_identity VALUES ('01890f2e-7b4c-7cc0-98c4-000000000021','fixture',1,1)";
const WAKE: &str = "INSERT INTO automation_wakes VALUES ('01890f2e-7b4c-7cc0-98c4-000000000022','target','1970-01-01T00:05:00.000Z','wake','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')";
const OLD_FIXTURE: &str =
    include_str!("../../../fleet-storage-schema/tests/fixtures/main_engine_probe_migration.sql");

fn catalog(production: bool, include_due_views: bool) -> Migrator {
    let migrations: Vec<_> = fleet_storage_schema::workspace_migrations::central_migrator()
        .iter()
        .filter(|migration| {
            (!production || migration.description != "engine probe fixture")
                && (include_due_views || migration.version <= 4)
        })
        .cloned()
        .collect();
    Migrator::with_migrations(migrations)
}

fn central(path: &Path, migrator: Migrator) -> ObjectStorage {
    ObjectStorage::new(path.to_path_buf(), Arc::new(migrator)).with_engine_publication(
        HeldClock::at(0),
        fleet_runtime::engine_alarm_decider(Arc::new(fleet_runtime::DueBatchState::default())),
    )
}

fn batch(statements: &[&'static str], alarm: AlarmUpdate) -> StatementBatch {
    StatementBatch {
        statements: statements
            .iter()
            .map(|&sql| GuardedStatement {
                statement: Statement { sql, args: vec![] },
                expect: RowExpectation::Exactly(1),
            })
            .collect(),
        alarm,
    }
}

#[derive(Clone, Default)]
struct StatementTrace(Arc<Mutex<Vec<String>>>);

impl StatementTrace {
    fn drain_selects(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
            .filter(|sql| sql.trim_start().starts_with("SELECT"))
            .collect()
    }
}

impl Visit for StatementTrace {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "db.statement" {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(value.to_owned());
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for StatementTrace {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() == "sqlx::query" {
            event.record(&mut self.clone());
        }
    }
}

#[tokio::test]
async fn one_checked_select_reads_its_own_write_and_clear_dispatches_none() {
    let trace = StatementTrace::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(trace.clone()))
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let expected = include_str!("../../../fleet-engine/src/queries/read_alarm_inputs.sql").trim();
    for (filename, production) in [("production.sqlite", true), ("fixture.sqlite", false)] {
        let storage = central(&root.path().join(filename), catalog(production, true));
        storage
            .run_batch(batch(&[STAMP], AlarmUpdate::Unchanged))
            .await
            .unwrap();
        trace.drain_selects();
        assert_eq!(
            storage
                .run_batch(batch(
                    &[WAKE],
                    AlarmUpdate::Engine(EngineAlarmPlan::Recompute)
                ))
                .await
                .unwrap(),
            BatchOutcome::Applied {
                rows_affected: vec![1],
                publication: AlarmPublication::Set {
                    unix_millis: 300_000
                }
            },
        );
        let selects = trace.drain_selects();
        assert_eq!(
            selects.iter().map(|sql| sql.trim()).collect::<Vec<_>>(),
            [expected]
        );
        assert_eq!(
            storage
                .run_batch(batch(&[], AlarmUpdate::Engine(EngineAlarmPlan::Clear)))
                .await
                .unwrap(),
            BatchOutcome::Applied {
                rows_affected: vec![],
                publication: AlarmPublication::Cleared
            },
        );
        assert!(trace.drain_selects().is_empty());
        storage.close().await;
    }
}

async fn snapshot(
    storage: &ObjectStorage,
) -> Result<Vec<Vec<SqlValue>>, central_storage::StoreFailure> {
    storage
        .read(Statement {
            sql: "SELECT version, hex(checksum) FROM _sqlx_migrations ORDER BY version",
            args: vec![],
        })
        .await
        .map(|read| read.rows)
}

#[tokio::test]
async fn production_upgrade_keeps_the_old_ledger_and_old_fixture_store_refuses_it() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upgrade.sqlite");
    let before = central(&path, catalog(true, false));
    before
        .run_batch(batch(&[STAMP, WAKE], AlarmUpdate::Unchanged))
        .await
        .unwrap();
    let original = snapshot(&before).await.unwrap();
    assert_eq!(original.len(), 4);
    before.close().await;

    let upgraded = central(&path, catalog(true, true));
    let fresh = central(&root.path().join("fresh.sqlite"), catalog(true, true));
    fresh
        .run_batch(batch(&[STAMP, WAKE], AlarmUpdate::Unchanged))
        .await
        .unwrap();
    let upgraded_ledger = snapshot(&upgraded).await.unwrap();
    assert_eq!(&upgraded_ledger[..original.len()], original);
    assert_eq!(upgraded_ledger, snapshot(&fresh).await.unwrap());
    for store in [&upgraded, &fresh] {
        assert_eq!(
            store
                .run_batch(batch(&[], AlarmUpdate::Engine(EngineAlarmPlan::Recompute)))
                .await
                .unwrap(),
            BatchOutcome::Applied {
                rows_affected: vec![],
                publication: AlarmPublication::Set {
                    unix_millis: 300_000
                }
            },
        );
        store.close().await;
    }

    let old_fixture_path = root.path().join("old-fixture.sqlite");
    let old_fixture = || {
        let mut migrations: Vec<_> = catalog(true, false).iter().cloned().collect();
        migrations.push(Migration::new(
            5,
            "engine probe fixture".into(),
            MigrationType::Simple,
            sqlx::SqlStr::from_static(OLD_FIXTURE),
            false,
        ));
        Migrator::with_migrations(migrations)
    };
    let saved = central(&old_fixture_path, old_fixture());
    saved
        .run_batch(batch(&[STAMP], AlarmUpdate::Unchanged))
        .await
        .unwrap();
    let saved_ledger = snapshot(&saved).await.unwrap();
    saved.close().await;
    let incompatible = central(&old_fixture_path, catalog(true, true));
    assert!(
        incompatible
            .read(Statement {
                sql: "SELECT 1",
                args: vec![]
            })
            .await
            .is_err()
    );
    let reopened = central(&old_fixture_path, old_fixture());
    assert_eq!(snapshot(&reopened).await.unwrap(), saved_ledger);
    reopened.close().await;
}
