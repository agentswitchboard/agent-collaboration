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
