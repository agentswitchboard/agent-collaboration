use crate::statement_store::{self, AlarmPolicy};
use central_storage::{
    AlarmUpdate, BatchOutcome, GuardedStatement, ReadOutcome, RowExpectation, Statement,
    StatementBatch,
};
use serde::{Deserialize, Serialize};
use sqlx_core::migrate::{Migration, MigrationType, Migrator};
use sqlx_core::sql_str::SqlStr;
use sqlx_durable_object::DoConnection;
use std::sync::Arc;

const OLD_FIXTURE: &str =
    include_str!("../../fleet-storage-schema/tests/fixtures/main_engine_probe_migration.sql");
const STAMP: &str = "INSERT INTO signin_organization_identity VALUES ('01890f2e-7b4c-7cc0-98c4-000000000021','fixture',1,1)";
const WAKE: &str = "INSERT INTO automation_wakes VALUES ('01890f2e-7b4c-7cc0-98c4-000000000022','target','1970-01-01T00:05:00.000Z','wake','active','1970-01-01T00:00:00.000Z','1970-01-01T00:00:00.000Z')";

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum MigrationCommand {
    SeedMain,
    SeedOldFixture,
    SeedCurrent,
    Upgrade,
    Inspect,
    Reset,
}

#[derive(Serialize)]
pub(super) struct Snapshot {
    ledger: ReadOutcome,
    views: ReadOutcome,
    alarm: Option<[Option<i64>; 2]>,
}

pub(super) async fn run(
    connection: &DoConnection,
    command: MigrationCommand,
) -> Result<Snapshot, String> {
    if !matches!(command, MigrationCommand::Inspect | MigrationCommand::Reset) {
        let old = matches!(
            command,
            MigrationCommand::SeedMain | MigrationCommand::SeedOldFixture
        );
        let mut migrations: Vec<_> = fleet_storage_schema::workspace_migrations::central_migrator()
            .iter()
            .filter(|migration| {
                migration.description != "engine probe fixture"
                    && migration.version <= 7
                    && (!old || migration.version <= 4)
            })
            .cloned()
            .collect();
        if matches!(command, MigrationCommand::SeedOldFixture) {
            migrations.push(Migration::new(
                5,
                "engine probe fixture".into(),
                MigrationType::Simple,
                SqlStr::from_static(OLD_FIXTURE),
                false,
            ));
        }
        statement_store::migrate(connection, Arc::new(Migrator::with_migrations(migrations)))
            .await
            .map_err(|failure| failure.to_string())?;
        if !matches!(command, MigrationCommand::Upgrade) {
            let batch = StatementBatch {
                statements: [STAMP, WAKE]
                    .into_iter()
                    .map(|sql| GuardedStatement {
                        statement: Statement { sql, args: vec![] },
                        expect: RowExpectation::Exactly(1),
                    })
                    .collect(),
                alarm: AlarmUpdate::Unchanged,
            };
            match statement_store::run_batch(connection, batch.into(), AlarmPolicy::Ignore).await {
                Ok(BatchOutcome::Applied { .. }) => {}
                outcome => return Err(format!("unexpected fixture apply result: {outcome:?}")),
            }
        }
    }
    let ledger = read(
        connection,
        "SELECT version, hex(checksum) FROM _sqlx_migrations ORDER BY version",
    )
    .await?;
    let views = read(
        connection,
        "SELECT name, sql FROM sqlite_schema WHERE type='view' ORDER BY name",
    )
    .await?;
    let alarm = if views.rows.is_empty() {
        None
    } else {
        use central_storage::SqlValue;
        let outcome = read(
            connection,
            include_str!("../../fleet-engine/src/queries/read_alarm_inputs.sql"),
        )
        .await?;
        let [row] = outcome.rows.as_slice() else {
            return Err("expected one historical alarm row".into());
        };
        let [grid, legacy] = row.as_slice() else {
            return Err("expected two historical alarm inputs".into());
        };
        let due = |cell: &SqlValue| match cell {
            SqlValue::Integer(value) => Ok(Some(value.get())),
            SqlValue::Null => Ok(None),
            _ => Err("invalid historical alarm input".to_owned()),
        };
        Some([due(grid)?, due(legacy)?])
    };
    Ok(Snapshot {
        ledger,
        views,
        alarm,
    })
}

async fn read(connection: &DoConnection, sql: &'static str) -> Result<ReadOutcome, String> {
    statement_store::read(connection, Statement { sql, args: vec![] }.into())
        .await
        .map_err(|failure| format!("{failure:?}"))
}
