//! Commit and alarm publication share one object turn, forced at the actual commit seam.

use crate::storage_support::{HeldClock, single_integer, workspace};
use central_storage::{AlarmUpdate, BatchOutcome, SqlValue, Statement, StatementBatch};
use collaboration_central_core::storage_conformance::{
    alarm_from_earliest_widget_due, widgets_migrator,
};
use collaboration_central_tokio::ObjectStorage;
use fleet_runtime::{PausePoint, PausePoints};
use fleet_runtime_ports::Clock;
use runtime_primitives::SyncTime;
use std::sync::Arc;

async fn publication_order(older_due: Option<i64>, newer_due: Option<i64>) {
    let root = tempfile::tempdir().unwrap();
    let clock = HeldClock::at(4_102_444_800_000);
    let pauses = PausePoints::default();
    let storage = Arc::new(
        ObjectStorage::new(
            root.path().join("central.sqlite"),
            Arc::new(widgets_migrator()),
        )
        .with_publication_pause(workspace(), pauses.clone()),
    );
    // The same injected clock supplies both batch values; neither waits on wall time.
    let at = clock.now().unix_millis();
    // Use a stored row as the independent oracle for the last database commit.
    let seeded = storage
        .run_batch(StatementBatch {
            statements: vec![
                collaboration_central_core::storage_conformance::insert_owner(
                    "alarm-owner",
                    central_storage::RowExpectation::Exactly(1),
                ),
                collaboration_central_core::storage_conformance::insert_widget(
                    "alarm-widget",
                    "alarm-owner",
                    SqlValue::Text("alarm".to_owned()),
                    SqlValue::Integer(central_storage::SafeInteger::new(1).unwrap()),
                    None,
                ),
            ],
            alarm: AlarmUpdate::Unchanged,
        })
        .await
        .unwrap();
    assert!(matches!(seeded, BatchOutcome::Applied { .. }));
    let stored_batch = |offset: Option<i64>| {
        let due = offset.map_or(SqlValue::Null, |offset| {
            SqlValue::Integer(central_storage::SafeInteger::new(at + offset).unwrap())
        });
        StatementBatch {
            statements: vec![central_storage::GuardedStatement {
                statement: Statement {
                    sql: "UPDATE widgets SET due_at_ms=? WHERE widget_id='alarm-widget'",
                    args: vec![due],
                },
                expect: central_storage::RowExpectation::Exactly(1),
            }],
            alarm: alarm_from_earliest_widget_due(),
        }
    };
    // Older commit is real, but its publication cannot run until explicitly released.
    let mut held = pauses.hold(&workspace(), PausePoint::AfterStoreApplied);
    let older_store = storage.clone();
    let older_batch = stored_batch(older_due);
    let older = tokio::spawn(async move { older_store.run_batch(older_batch).await.unwrap() });
    held.until_held().await;
    let newer_store = storage.clone();
    let newer_batch = stored_batch(newer_due);
    let mut newer = tokio::spawn(async move { newer_store.run_batch(newer_batch).await.unwrap() });
    let queued = tokio::select! {
        () = storage.until_batch_waiting() => true,
        result = &mut newer => { assert!(matches!(result.unwrap(), BatchOutcome::Applied { .. })); false },
    };
    held.release();
    assert!(matches!(older.await.unwrap(), BatchOutcome::Applied { .. }));
    if queued {
        assert!(matches!(newer.await.unwrap(), BatchOutcome::Applied { .. }));
    }
    let row = storage
        .read(Statement {
            sql: "SELECT MIN(due_at_ms) FROM widgets WHERE active=1",
            args: Vec::new(),
        })
        .await
        .unwrap();
    let committed_due = match row.rows.as_slice() {
        [values] if values.as_slice() == [SqlValue::Null] => None,
        _ => Some(SyncTime::from_unix_millis(single_integer(&row))),
    };
    assert_eq!(
        storage.alarm(),
        committed_due,
        "alarm publication overtook the last database commit"
    );
    assert_eq!(
        committed_due,
        newer_due.map(|offset| SyncTime::from_unix_millis(at + offset))
    );
    assert!(queued, "newer commit passed an older unpublished alarm");
    storage.close().await;
}

#[tokio::test]
async fn older_set_cannot_overwrite_a_newer_set() {
    publication_order(Some(5_000), Some(3_000)).await;
}

#[tokio::test]
async fn older_clear_cannot_overwrite_a_newer_set() {
    publication_order(None, Some(3_000)).await;
}

#[tokio::test]
async fn older_set_cannot_overwrite_a_newer_clear() {
    publication_order(Some(5_000), None).await;
}

fn engine_alarm_from_widgets() -> AlarmUpdate {
    AlarmUpdate::Engine(central_storage::EngineAlarmPlan::Recompute)
}

// The checked Engine query operates on real fixture rows, including arbitrary
// dates close to a clock boundary. The single-value conformance tests above
// keep their independent widget migration and statement.
fn engine_widgets_migrator() -> sqlx::migrate::Migrator {
    let mut migrations: Vec<_> = fleet_storage_schema::workspace_migrations::central_migrator()
        .iter()
        .cloned()
        .collect();
    let version = migrations.last().unwrap().version + 1;
    migrations.extend(
        widgets_migrator()
            .iter()
            .enumerate()
            .map(|(offset, migration)| {
                let mut migration = migration.clone();
                migration.version = version + i64::try_from(offset).unwrap();
                migration
            }),
    );
    migrations.push(sqlx::migrate::Migration::new(
        migrations.last().unwrap().version + 1,
        "checked Engine widget fixture".into(),
        sqlx::migrate::MigrationType::Simple,
        sqlx::SqlStr::from_static(
            "INSERT INTO signin_organization_identity VALUES ('01890f2e-7b4c-7cc0-98c4-000000000041','fixture',1,1);"
        ),
        false,
    ));
    sqlx::migrate::Migrator::with_migrations(migrations)
}

fn project_widget(
    mut statements: Vec<central_storage::GuardedStatement>,
    subject: &str,
    due: i64,
) -> Vec<central_storage::GuardedStatement> {
    use fleet_runtime_ports::EngineTime;
    statements.push(crate::storage_support::fixture_due_item(
        fleet_engine::DueKind::WakeTimer,
        subject,
        EngineTime::from_timestamp_millis(due).unwrap(),
        EngineTime::UNIX_EPOCH,
    ));
    statements
}

#[tokio::test]
async fn a_batch_prepared_before_a_new_claim_publishes_the_latest_backlog_mark() {
    let root = tempfile::tempdir().unwrap();
    let clock = HeldClock::at(32_000);
    let state = Arc::new(fleet_runtime::DueBatchState::default());
    let storage = Arc::new(
        ObjectStorage::new(
            root.path().join("latest-floor.sqlite"),
            Arc::new(engine_widgets_migrator()),
        )
        .with_engine_publication(clock, fleet_runtime::engine_alarm_decider(state.clone())),
    );
    assert!(state.try_claim(0, chrono::Duration::seconds(30)).unwrap());
    let mut held = storage.hold_alarm_publication();
    let applying = storage.clone();
    let prepared = StatementBatch {
        statements: project_widget(
            vec![
                collaboration_central_core::storage_conformance::insert_owner(
                    "floor-owner",
                    central_storage::RowExpectation::Exactly(1),
                ),
                collaboration_central_core::storage_conformance::insert_widget(
                    "floor-widget",
                    "floor-owner",
                    SqlValue::Text("floor".to_owned()),
                    SqlValue::Integer(1.try_into().unwrap()),
                    Some(600_000.try_into().unwrap()),
                ),
            ],
            "floor-widget",
            600_000,
        ),
        alarm: engine_alarm_from_widgets(),
    };
    let pending = tokio::spawn(async move { applying.run_batch(prepared).await.unwrap() });
    held.until_held().await;
    assert_eq!(
        single_integer(
            &storage
                .read(Statement {
                    sql: "SELECT COUNT(*) FROM widget_owners WHERE owner_id='floor-owner'",
                    args: vec![]
                })
                .await
                .unwrap()
        ),
        1
    );
    assert_eq!(storage.alarm(), None);
    assert!(
        state
            .try_claim(31_000, chrono::Duration::seconds(30))
            .unwrap()
    );
    assert!(state.finish(31_000, true));
    held.release();
    assert_eq!(
        pending.await.unwrap(),
        BatchOutcome::Applied {
            rows_affected: vec![1, 1, 1],
            publication: central_storage::AlarmPublication::Set {
                unix_millis: 300_000
            }
        }
    );
    assert_eq!(storage.alarm(), Some(SyncTime::from_unix_millis(300_000)));
    storage.close().await;
}

#[tokio::test]
async fn publication_uses_the_shared_clock_after_both_committed_publication_holds() {
    let root = tempfile::tempdir().unwrap();
    let clock = HeldClock::at(0);
    let pauses = PausePoints::default();
    let workspace = workspace();
    let storage = Arc::new(
        ObjectStorage::new(
            root.path().join("publication-clock.sqlite"),
            Arc::new(engine_widgets_migrator()),
        )
        .with_engine_publication(
            clock.clone(),
            fleet_runtime::engine_alarm_decider(Arc::new(fleet_runtime::DueBatchState::default())),
        )
        .with_publication_pause(workspace.clone(), pauses.clone()),
    );
    let mut central_hold = storage.hold_alarm_publication();
    let mut workspace_hold = pauses.hold(&workspace, PausePoint::AfterStoreApplied);
    let applying = storage.clone();
    let batch = tokio::spawn(async move {
        applying
            .run_batch(StatementBatch {
                statements: project_widget(
                    vec![
                        collaboration_central_core::storage_conformance::insert_owner(
                            "clock-owner",
                            central_storage::RowExpectation::Exactly(1),
                        ),
                        collaboration_central_core::storage_conformance::insert_widget(
                            "clock-widget",
                            "clock-owner",
                            SqlValue::Text("clock".to_owned()),
                            SqlValue::Integer(1.try_into().unwrap()),
                            Some(5_000.try_into().unwrap()),
                        ),
                    ],
                    "clock-widget",
                    5_000,
                ),
                alarm: engine_alarm_from_widgets(),
            })
            .await
            .unwrap()
    });
    central_hold.until_held().await;
    assert_eq!(
        single_integer(
            &storage
                .read(Statement {
                    sql: "SELECT due_at_ms FROM widgets WHERE widget_id='clock-widget'",
                    args: vec![],
                })
                .await
                .unwrap()
        ),
        5_000
    );
    assert_eq!(storage.alarm(), None);
    clock.set(1_000);
    central_hold.release();
    workspace_hold.until_held().await;
    clock.set(300_000);
    workspace_hold.release();

    assert_eq!(
        batch.await.unwrap(),
        BatchOutcome::Applied {
            rows_affected: vec![1, 1, 1],
            publication: central_storage::AlarmPublication::Set {
                unix_millis: 600_000
            }
        }
    );
    assert_eq!(storage.alarm(), Some(SyncTime::from_unix_millis(600_000)));
    assert!(storage.take_alarm_publication_failures().is_empty());
    storage.close().await;
}

#[tokio::test]
async fn postcommit_upper_edge_failure_preserves_applied_rows_and_the_previous_alarm() {
    let root = tempfile::tempdir().unwrap();
    let clock = HeldClock::at(0);
    let storage = Arc::new(
        ObjectStorage::new(
            root.path().join("publication-failure.sqlite"),
            Arc::new(engine_widgets_migrator()),
        )
        .with_engine_publication(
            clock.clone(),
            fleet_runtime::engine_alarm_decider(Arc::new(fleet_runtime::DueBatchState::default())),
        ),
    );
    assert_eq!(
        storage
            .run_batch(StatementBatch {
                statements: project_widget(
                    vec![
                        collaboration_central_core::storage_conformance::insert_owner(
                            "failure-owner",
                            central_storage::RowExpectation::Exactly(1),
                        ),
                        collaboration_central_core::storage_conformance::insert_widget(
                            "failure-widget",
                            "failure-owner",
                            SqlValue::Text("failure".to_owned()),
                            SqlValue::Integer(1.try_into().unwrap()),
                            Some(600_000.try_into().unwrap()),
                        ),
                    ],
                    "failure-widget",
                    600_000
                ),
                alarm: engine_alarm_from_widgets(),
            })
            .await
            .unwrap(),
        BatchOutcome::Applied {
            rows_affected: vec![1, 1, 1],
            publication: central_storage::AlarmPublication::Set {
                unix_millis: 600_000
            }
        }
    );
    let previous = storage.alarm();
    assert_eq!(previous, Some(SyncTime::from_unix_millis(600_000)));
    let mut held = storage.hold_alarm_publication();
    let applying = storage.clone();
    let batch =
        tokio::spawn(async move {
            applying
            .run_batch(StatementBatch {
                statements: project_widget(vec![central_storage::GuardedStatement {
                    statement: Statement {
                        sql: "UPDATE widgets SET due_at_ms=0 WHERE widget_id='failure-widget'",
                        args: vec![],
                    },
                    expect: central_storage::RowExpectation::Exactly(1),
                }], "failure-widget", 0),
                alarm: engine_alarm_from_widgets(),
            })
            .await
            .unwrap()
        });
    held.until_held().await;
    clock.set(i64::MAX / 1_000_000);
    held.release();

    assert_eq!(
        batch.await.unwrap(),
        BatchOutcome::Applied {
            rows_affected: vec![1, 1],
            publication: central_storage::AlarmPublication::Failed {
                reason: central_storage::AlarmPublicationFailure::NoLaterGridMark
            }
        }
    );
    assert_eq!(
        single_integer(
            &storage
                .read(Statement {
                    sql: "SELECT due_at_ms FROM widgets WHERE widget_id='failure-widget'",
                    args: vec![],
                })
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(storage.alarm(), previous);
    assert_eq!(
        storage.take_alarm_publication_failures(),
        vec![central_storage::AlarmPublicationFailure::NoLaterGridMark,]
    );
    assert!(storage.take_alarm_publication_failures().is_empty());
    storage.close().await;
}
