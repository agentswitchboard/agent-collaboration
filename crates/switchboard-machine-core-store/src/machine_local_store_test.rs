use super::{MACHINE_LOCAL_DATABASE, MachineLocalStore};

#[tokio::test]
async fn opening_and_reopening_the_core_file_keeps_the_three_store_tables() {
    let root = tempfile::tempdir().unwrap();
    let machine_local = root.path().join(MACHINE_LOCAL_DATABASE);
    MachineLocalStore::open(&machine_local)
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

    let mut reopened = MachineLocalStore::open(&machine_local).await.unwrap();
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(&mut reopened.connection)
            .await
            .unwrap();
    reopened.close().await.unwrap();

    assert_eq!(
        tables,
        [
            "_sqlx_migrations",
            "inbox_messages",
            "machine_identity",
            "outbox_items"
        ]
    );
    assert!(!root.path().join("provider-operations.sqlite").exists());
}

const ENROLLMENT_MIGRATION: i64 = 20261123000001;
const IDENTITY_TIME: i64 = 1_795_000_000_123;

async fn assert_no_enrollment_after_all_five_migrations(store: &mut MachineLocalStore) {
    let (migration_count, last_migration): (i64, Option<i64>) =
        sqlx::query_as("SELECT count(*), max(version) FROM _sqlx_migrations")
            .fetch_one(&mut store.connection)
            .await
            .unwrap();
    assert_eq!(
        (migration_count, last_migration),
        (5, Some(ENROLLMENT_MIGRATION))
    );
    let stored: (i64, Option<String>, Option<Vec<u8>>, i64, i64) = sqlx::query_as(
        "SELECT enrollment_generation, enrollment_json, enrollment_sealed, created_at, updated_at FROM machine_identity",
    )
    .fetch_one(&mut store.connection)
    .await
    .unwrap();
    assert_eq!(stored, (0, None, None, IDENTITY_TIME, IDENTITY_TIME));
    let expiry_column: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pragma_table_info('inbox_messages') WHERE name = 'expires_at'",
    )
    .fetch_one(&mut store.connection)
    .await
    .unwrap();
    assert_eq!(expiry_column, 1);
}

#[tokio::test]
async fn a_fresh_core_database_applies_all_five_migrations() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(MACHINE_LOCAL_DATABASE);
    let mut store = MachineLocalStore::open(&path).await.unwrap();
    store
        .load_or_create_machine_id(IDENTITY_TIME)
        .await
        .unwrap();

    assert_no_enrollment_after_all_five_migrations(&mut store).await;
}

#[tokio::test]
async fn a_core_database_with_four_migrations_retains_its_machine_on_upgrade() {
    use sqlx::{Connection, SqliteConnection, migrate::Migrator, sqlite::SqliteConnectOptions};
    use std::borrow::Cow;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(MACHINE_LOCAL_DATABASE);
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let mut old_connection = SqliteConnection::connect_with(&options).await.unwrap();
    let old_migrations = Migrator {
        migrations: Cow::Owned(
            super::MIGRATOR
                .iter()
                .filter(|migration| migration.version < ENROLLMENT_MIGRATION)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    };
    assert_eq!(old_migrations.iter().count(), 4);
    old_migrations.run(&mut old_connection).await.unwrap();
    let machine = "019a0000-0000-7000-8000-000000000005";
    sqlx::query(
        "INSERT INTO machine_identity (machine_id, created_at, updated_at) VALUES (?, ?, ?)",
    )
    .bind(machine)
    .bind(IDENTITY_TIME)
    .bind(IDENTITY_TIME)
    .execute(&mut old_connection)
    .await
    .unwrap();
    old_connection.close().await.unwrap();

    let mut upgraded = MachineLocalStore::open(&path).await.unwrap();
    let reopened = upgraded
        .load_or_create_machine_id(IDENTITY_TIME + 1)
        .await
        .unwrap();
    assert_eq!(reopened.as_str(), machine);
    assert_no_enrollment_after_all_five_migrations(&mut upgraded).await;
}

async fn rollback_journal_database(path: &std::path::Path) -> sqlx::SqliteConnection {
    use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .pragma("auto_vacuum", "INCREMENTAL");
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::query("PRAGMA user_version = 0")
        .execute(&mut connection)
        .await
        .unwrap();
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(mode, "delete");
    connection
}

#[tokio::test]
async fn first_open_retries_wal_promotion_after_another_connection_releases_its_reservation() {
    use sqlx::Connection;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(MACHINE_LOCAL_DATABASE);
    let mut blocker = rollback_journal_database(&path).await;
    let mut reading = blocker.begin_with("BEGIN IMMEDIATE").await.unwrap();
    let _: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema")
        .fetch_one(&mut *reading)
        .await
        .unwrap();

    let (saw_busy, at_retry) = tokio::sync::oneshot::channel();
    let mut first = std::pin::pin!(MachineLocalStore::open_with_busy_callback(&path, || {
        saw_busy.send(()).unwrap();
    }));
    tokio::select! {
        biased;
        arrived = at_retry => arrived.expect("the first connect reached its BUSY retry"),
        opened = &mut first => panic!("first open returned before retry: {:?}", opened.err()),
    }
    reading.rollback().await.unwrap();

    let (first, second) = tokio::join!(first, MachineLocalStore::open(&path));
    let mut first = first.unwrap();
    let mut second = second.unwrap();
    let (left, right) = tokio::join!(
        first.load_or_create_machine_id(IDENTITY_TIME),
        second.load_or_create_machine_id(IDENTITY_TIME + 1),
    );
    assert_eq!(left.unwrap(), right.unwrap());
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM machine_identity")
        .fetch_one(&mut first.connection)
        .await
        .unwrap();
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn an_exclusive_database_exhausts_the_open_budget_and_returns_sqlite_busy() {
    use crate::MachineLocalStoreError;
    use sqlx::Connection;

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(MACHINE_LOCAL_DATABASE);
    let mut blocker = rollback_journal_database(&path).await;
    let exclusive = blocker.begin_with("BEGIN EXCLUSIVE").await.unwrap();
    let first_busy = std::cell::Cell::new(false);
    let result = MachineLocalStore::open_with_busy_callback(&path, || first_busy.set(true)).await;

    assert!(
        first_busy.get(),
        "the sole open error was classified as retryable BUSY"
    );
    assert!(matches!(
        result,
        Err(MachineLocalStoreError::Database(sqlx::Error::Database(error)))
            if error.code().as_deref() == Some("5")
    ));
    exclusive.rollback().await.unwrap();
}
