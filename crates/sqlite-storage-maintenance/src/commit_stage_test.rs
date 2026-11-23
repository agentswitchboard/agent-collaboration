use std::{future::Future, pin::Pin};

use sqlx::{Connection, SqliteConnection};

use super::{CommitSignal, CommitStageError, SignaledTransaction};

fn count_until_insert(
    connection: &mut SqliteConnection,
) -> Pin<Box<dyn Future<Output = Result<i64, sqlx::Error>> + Send + '_>> {
    Box::pin(async move {
        let changes: i64 = sqlx::query_scalar("SELECT total_changes()")
            .fetch_one(connection)
            .await?;
        if changes == 0 {
            Ok(changes)
        } else {
            Err(sqlx::Error::RowNotFound)
        }
    })
}

#[tokio::test]
async fn a_failed_change_count_is_definite_and_does_not_commit_or_signal() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::query("CREATE TABLE example (id INTEGER)")
        .execute(&mut connection)
        .await
        .unwrap();
    let commits = CommitSignal::new();
    let observed = commits.subscribe();
    let mut transaction = SignaledTransaction::counting(
        connection.begin().await.unwrap(),
        count_until_insert,
        &commits,
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO example (id) VALUES (1)")
        .execute(&mut *transaction)
        .await
        .unwrap();

    assert!(matches!(
        transaction.commit_with_stage().await,
        Err(CommitStageError::BeforeCommit(sqlx::Error::RowNotFound))
    ));
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM example")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    assert!(!observed.has_changed().unwrap());
}

#[tokio::test]
async fn a_failed_sqlite_commit_is_reported_as_already_issued() {
    let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "PRAGMA foreign_keys = ON;
         CREATE TABLE parent (id INTEGER PRIMARY KEY);
         CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    let commits = CommitSignal::new();
    let observed = commits.subscribe();
    let mut transaction = SignaledTransaction::counting(
        connection.begin().await.unwrap(),
        super::sqlite_total_changes,
        &commits,
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO child (parent_id) VALUES (1)")
        .execute(&mut *transaction)
        .await
        .unwrap();

    assert!(matches!(
        transaction.commit_with_stage().await,
        Err(CommitStageError::CommitIssued(sqlx::Error::Database(_)))
    ));
    assert!(!observed.has_changed().unwrap());
}
