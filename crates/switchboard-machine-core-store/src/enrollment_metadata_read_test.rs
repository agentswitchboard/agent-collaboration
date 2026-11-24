use std::fs;

use crate::{
    EnrollmentRowChange, EnrollmentRowWrite, MACHINE_LOCAL_DATABASE, MachineLocalStore,
    StoredEnrollmentRow, StoredEnrollmentRowMetadata,
};

use super::{
    MachineEnrollmentMetadataRow, existing_database_read_options, read_enrollment_snapshot,
};

#[tokio::test]
async fn enrollment_snapshot_sees_a_commit_after_read_setup_and_before_sqlite_open() {
    let directory = tempfile::tempdir().expect("fixture root");
    let database = directory.path().join(MACHINE_LOCAL_DATABASE);
    let now_ms = 1_790_000_000_000;
    let mut writer = MachineLocalStore::open(&database)
        .await
        .expect("fixture writer");
    let machine_id = writer
        .load_or_create_machine_id(now_ms)
        .await
        .expect("fixture identity");
    writer.close().await.expect("checkpoint fixture database");

    let options = existing_database_read_options(&database)
        .expect("reader setup")
        .expect("database exists");
    let wal = directory
        .path()
        .join(format!("{MACHINE_LOCAL_DATABASE}-wal"));
    assert!(fs::metadata(&wal).map_or_else(
        |error| error.kind() == std::io::ErrorKind::NotFound,
        |metadata| metadata.len() == 0
    ));
    let checkpointed = fs::read(&database).expect("checkpointed database");
    let mut writer = MachineLocalStore::open(&database)
        .await
        .expect("reopen fixture writer");
    let organization_id = "019a0000-0000-7000-8000-000000000001";
    let applied = writer
        .write_enrollment_row(&machine_id, 0, now_ms, |_| {
            let mut sealed = vec![1];
            sealed.resize(30, 0);
            EnrollmentRowChange::<()>::Write(StoredEnrollmentRow::Active {
                generation: 1,
                organization_id: organization_id.into(),
                deployment: "https://signin.example/".into(),
                enrollment_id: "019a0000-0000-7000-8000-000000000002".into(),
                sealed,
            })
        })
        .await
        .expect("commit after reader setup");
    assert_eq!(applied, EnrollmentRowWrite::Applied);
    assert!(fs::metadata(&wal).expect("live WAL").len() > 0);
    assert_eq!(fs::read(&database).expect("machine database"), checkpointed);

    let snapshot = read_enrollment_snapshot(&options)
        .await
        .expect("read one SQLite snapshot");
    assert!(
        matches!(
            snapshot,
            MachineEnrollmentMetadataRow::Found {
                machine_id: observed,
                enrollment: StoredEnrollmentRowMetadata::Active { generation: 1, organization_id: organization, .. },
            } if observed == machine_id && organization == organization_id
        ),
        "status snapshot missed the commit after read setup"
    );
    writer.close().await.expect("close fixture writer");
}
