use std::{
    borrow::Cow, ffi::OsString, fs, io, os::unix::fs::DirBuilderExt, path::Path, process::Output,
};

use sqlx::{
    Connection, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteLockingMode},
};
use switchboard_machine_core_store::{
    EnrollmentMetadataQueryError, EnrollmentMetadataReadError, EnrollmentRowChange,
    EnrollmentRowCorruption, EnrollmentRowWrite, MACHINE_LOCAL_DATABASE, MachineLocalStore,
    StoredEnrollmentEnd, StoredEnrollmentRow, load_or_create_machine_id,
};
use switchboard_machine_enrollment_store::{
    MachineEnrollmentStatus, read_machine_enrollment_status,
};

const ORGANIZATION: &str = "019a0000-0000-7000-8000-000000000001";
const ENROLLMENT: &str = "019a0000-0000-7000-8000-000000000002";
const SIGN_IN: &str = "https://signin.example/";
const NOW_MS: i64 = 1_790_000_000_000;

fn compiled_cli(home: &Path, args: &[&str], root_override: Option<&Path>) -> io::Result<Output> {
    compiled_cli_command(home, args, root_override).output()
}

fn compiled_cli_command(
    home: &Path,
    args: &[&str],
    root_override: Option<&Path>,
) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_codex-router"));
    command
        .env("HOME", home)
        .env("OTEL_SDK_DISABLED", "true")
        .env_remove("CODEX_ROUTER_DEBUG_ROUTER_ROOT")
        .env_remove("CODEX_ROUTER_USE_HOME_DEFAULT")
        .args(args);
    if let Some(root) = root_override {
        command.env("CODEX_ROUTER_DEBUG_ROUTER_ROOT", root);
    }
    command
}

fn stdout(output: &Output) -> Cow<'_, str> {
    assert!(output.status.success(), "CLI output: {output:?}");
    assert!(output.stderr.is_empty(), "CLI output: {output:?}");
    String::from_utf8_lossy(&output.stdout)
}

#[test]
fn enrollment_help_and_invalid_arguments_do_not_create_state() {
    let home = tempfile::tempdir().expect("fixture home");
    let help =
        compiled_cli(home.path(), &["enroll", "--help"], None).expect("run signed Router CLI");
    assert!(stdout(&help).contains("--organization <uuid> --sign-in <https-origin>"));
    assert!(stdout(&help).contains("Stop the debug Host"));
    let help =
        compiled_cli(home.path(), &["enrollment", "--help"], None).expect("run signed Router CLI");
    assert!(stdout(&help).contains("enrollment status"));
    assert!(stdout(&help).contains("Works with the Host running or stopped."));

    let bad_arguments = compiled_cli(
        home.path(),
        &["enroll", "--organization", "bad", "--sign-in", SIGN_IN],
        None,
    )
    .expect("run signed Router CLI");
    assert_eq!(bad_arguments.status.code(), Some(2));
    assert_eq!(
        bad_arguments.stderr,
        b"Copy the organization id from sign-in and pass it to --organization.\n"
    );
    assert!(
        fs::read_dir(home.path())
            .expect("fixture home")
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn fresh_enrollment_status_creates_nothing() {
    let home = tempfile::tempdir().expect("fixture home");
    let router_root = default_fixture_root(home.path());
    let database = router_root.join(MACHINE_LOCAL_DATABASE);
    let result =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(stdout(&result), "This Mac is not enrolled.\n");
    assert!(
        fs::read_dir(home.path())
            .expect("fixture home")
            .next()
            .is_none()
    );
    assert!(matches!(
        read_machine_enrollment_status(&database).await,
        Ok(MachineEnrollmentStatus::DatabaseAbsent)
    ));

    fs::DirBuilder::new()
        .mode(0o700)
        .create(&router_root)
        .expect("fixture Router root");
    let store = MachineLocalStore::open(&database)
        .await
        .expect("fixture database without identity");
    store.close().await.expect("close fixture database");
    let before = fs::read(&database).expect("machine database");
    let result =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(stdout(&result), "This Mac is not enrolled.\n");
    assert_eq!(fs::read(&database).expect("machine database"), before);
    assert!(matches!(
        read_machine_enrollment_status(&database).await,
        Ok(MachineEnrollmentStatus::MachineAbsent)
    ));
    let machine_id = load_or_create_machine_id(&router_root, NOW_MS)
        .await
        .expect("fixture machine ID");
    let before = fs::read(&database).expect("machine database");
    let result =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(
        stdout(&result),
        format!("This Mac is not enrolled.\nMachine: {machine_id}\n")
    );
    assert_eq!(fs::read(&database).expect("machine database"), before);
}

#[cfg(debug_assertions)]
#[test]
fn debug_enrollment_refuses_a_production_root_alias() {
    let home = tempfile::tempdir().expect("fixture home");
    let production = home.path().join(".codex-router");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&production)
        .expect("fixture production root");
    let alias = home.path().join("alias");
    std::os::unix::fs::symlink(&production, &alias).expect("fixture symlink");

    for args in [
        &[
            "enroll",
            "--organization",
            ORGANIZATION,
            "--sign-in",
            SIGN_IN,
        ][..],
        &["enrollment", "status"][..],
    ] {
        let output = compiled_cli(home.path(), args, Some(&alias)).expect("run signed Router CLI");
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("separate from normal state"));
    }
    assert!(
        fs::read_dir(production)
            .expect("fixture production root")
            .next()
            .is_none()
    );
}

#[tokio::test]
async fn compiled_status_reports_active_and_ended_rows_without_writing() {
    let home = tempfile::tempdir().expect("fixture home");
    let router_root = default_fixture_root(home.path());
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&router_root)
        .expect("fixture Router root");
    let machine_id = load_or_create_machine_id(&router_root, NOW_MS)
        .await
        .expect("machine id");
    let database = router_root.join(MACHINE_LOCAL_DATABASE);
    let mut store = MachineLocalStore::open(&database)
        .await
        .expect("fixture store");
    let applied = store
        .write_enrollment_row(&machine_id, 0, NOW_MS, |_| {
            EnrollmentRowChange::<()>::Write(active_row())
        })
        .await
        .expect("seed active enrollment");
    assert_eq!(applied, EnrollmentRowWrite::Applied);
    store.close().await.expect("close seed store");

    let before = fs::read(&database).expect("machine database");
    let active =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(
        stdout(&active),
        format!(
            "This Mac is enrolled.\nMachine: {machine_id}\nOrganization: {ORGANIZATION}\nSign-in: {SIGN_IN}\nThis is what the Mac has stored. It does not show whether the link to central is up.\n"
        )
    );
    assert_eq!(fs::read(&database).expect("machine database"), before);

    let mut store = MachineLocalStore::open(&database)
        .await
        .expect("fixture store");
    let applied = store
        .write_enrollment_ending(&machine_id, 1, NOW_MS, |_| {
            EnrollmentRowChange::<()>::Write(ended_row())
        })
        .await
        .expect("seed ended enrollment");
    assert_eq!(applied, EnrollmentRowWrite::Applied);
    store.close().await.expect("close seed store");

    let before = fs::read(&database).expect("machine database");
    let ended =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(
        stdout(&ended),
        format!(
            "This Mac's enrollment ended: the enrollment was revoked.\nMachine: {machine_id}\nOrganization: {ORGANIZATION}\n"
        )
    );
    assert_eq!(fs::read(&database).expect("machine database"), before);
}

#[tokio::test]
async fn compiled_status_reads_current_enrollment_from_an_open_wal_without_writing() {
    let home = tempfile::tempdir().expect("fixture home");
    let root = default_fixture_root(home.path());
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("fixture Router root");
    let machine_id = load_or_create_machine_id(&root, NOW_MS)
        .await
        .expect("fixture machine ID");
    let database = root.join(MACHINE_LOCAL_DATABASE);
    let wal = root.join(format!("{MACHINE_LOCAL_DATABASE}-wal"));
    let mut store = MachineLocalStore::open(&database)
        .await
        .expect("fixture writer");
    let checkpointed = fs::read(&database).expect("checkpointed machine database");

    let written = store
        .write_enrollment_row(&machine_id, 0, NOW_MS, |_| {
            EnrollmentRowChange::<()>::Write(active_row())
        })
        .await
        .expect("commit active enrollment");
    assert_eq!(written, EnrollmentRowWrite::Applied);
    let active_wal = fs::read(&wal).expect("active WAL");
    assert!(!active_wal.is_empty());
    assert_eq!(fs::read(&database).expect("machine database"), checkpointed);

    let active =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(
        stdout(&active),
        format!(
            "This Mac is enrolled.\nMachine: {machine_id}\nOrganization: {ORGANIZATION}\nSign-in: {SIGN_IN}\nThis is what the Mac has stored. It does not show whether the link to central is up.\n"
        ),
        "status must report the current active enrollment from the open WAL",
    );
    assert_eq!(fs::read(&database).expect("machine database"), checkpointed);
    assert_eq!(fs::read(&wal).expect("active WAL"), active_wal);
    assert_eq!(
        store
            .read_enrollment_row(&machine_id)
            .await
            .expect("active stored row"),
        active_row()
    );

    let written = store
        .write_enrollment_ending(&machine_id, 1, NOW_MS, |_| {
            EnrollmentRowChange::<()>::Write(ended_row())
        })
        .await
        .expect("commit ended enrollment");
    assert_eq!(written, EnrollmentRowWrite::Applied);
    let ended_wal = fs::read(&wal).expect("ended WAL");
    assert!(ended_wal.len() > active_wal.len());
    assert_eq!(fs::read(&database).expect("machine database"), checkpointed);

    let ended =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_eq!(
        stdout(&ended),
        format!(
            "This Mac's enrollment ended: the enrollment was revoked.\nMachine: {machine_id}\nOrganization: {ORGANIZATION}\n"
        ),
        "status must report the current ended enrollment from the open WAL",
    );
    assert_eq!(fs::read(&database).expect("machine database"), checkpointed);
    assert_eq!(fs::read(&wal).expect("ended WAL"), ended_wal);
    assert_eq!(
        store
            .read_enrollment_row(&machine_id)
            .await
            .expect("ended stored row"),
        ended_row()
    );
    store.close().await.expect("close fixture writer");
}

fn active_row() -> StoredEnrollmentRow {
    let mut sealed = vec![1];
    sealed.resize(30, 0);
    StoredEnrollmentRow::Active {
        generation: 1,
        organization_id: ORGANIZATION.into(),
        deployment: SIGN_IN.into(),
        enrollment_id: ENROLLMENT.into(),
        sealed,
    }
}

fn ended_row() -> StoredEnrollmentRow {
    StoredEnrollmentRow::Ended {
        generation: 2,
        organization_id: ORGANIZATION.into(),
        deployment: SIGN_IN.into(),
        enrollment_id: ENROLLMENT.into(),
        why: StoredEnrollmentEnd::Revoked {},
    }
}

fn files_in(root: &Path) -> io::Result<Vec<(OsString, Vec<u8>)>> {
    let mut files = fs::read_dir(root)?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.file_name(), fs::read(entry.path())?))
        })
        .collect::<io::Result<Vec<_>>>()?;
    files.sort_by(|first, second| first.0.cmp(&second.0));
    Ok(files)
}

#[tokio::test]
async fn compiled_status_refuses_unreadable_database_and_corrupt_metadata() {
    let home = tempfile::tempdir().expect("fixture home");
    let root = default_fixture_root(home.path());
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("fixture Router root");
    let database = root.join(MACHINE_LOCAL_DATABASE);
    fs::write(&database, b"not a database").expect("unreadable database fixture");
    let before = fs::read(&database).expect("machine database");
    assert_status_error(
        &compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI"),
        "could not read the machine database",
    );
    assert_eq!(fs::read(&database).expect("machine database"), before);
    assert!(matches!(
        read_machine_enrollment_status(&database).await,
        Err(EnrollmentMetadataReadError::Query(
            EnrollmentMetadataQueryError::Sqlite(_)
        ))
    ));

    fs::remove_file(&database).expect("discard fixture bytes");
    let machine_id = load_or_create_machine_id(&root, NOW_MS)
        .await
        .expect("fixture machine ID");
    let mut store = MachineLocalStore::open(&database)
        .await
        .expect("fixture store");
    let mut sealed = vec![0; 30];
    sealed[0] = 1;
    store
        .write_enrollment_row(&machine_id, 0, NOW_MS, |_| {
            EnrollmentRowChange::<()>::Write(StoredEnrollmentRow::Active {
                generation: 1,
                organization_id: ORGANIZATION.into(),
                deployment: "invalid sign-in deployment".into(),
                enrollment_id: ENROLLMENT.into(),
                sealed,
            })
        })
        .await
        .expect("seed corrupt deployment");
    store.close().await.expect("close seed store");
    let before = fs::read(&database).expect("machine database");
    assert_status_error(
        &compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI"),
        "stored enrollment metadata is corrupt",
    );
    assert_eq!(fs::read(&database).expect("machine database"), before);
    assert!(matches!(
        read_machine_enrollment_status(&database).await,
        Err(EnrollmentMetadataReadError::Query(
            EnrollmentMetadataQueryError::Corrupt(EnrollmentRowCorruption::Deployment)
        ))
    ));
}

fn assert_status_error(output: &Output, expected: &str) {
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected),
        "{output:?}"
    );
}

fn default_fixture_root(home: &Path) -> std::path::PathBuf {
    home.join(if cfg!(debug_assertions) {
        ".codex-router-debug"
    } else {
        ".codex-router"
    })
}

#[cfg(debug_assertions)]
#[tokio::test]
async fn linked_database_and_sidecars_are_refused_before_sign_in_or_status() {
    let home = tempfile::tempdir().expect("fixture home");
    let production = home.path().join(".codex-router");
    let debug = home.path().join(".codex-router-debug");
    for root in [&production, &debug] {
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root)
            .expect("private fixture root");
    }
    let production_database = production.join(MACHINE_LOCAL_DATABASE);
    load_or_create_machine_id(&production, NOW_MS)
        .await
        .expect("protected fixture identity");
    let before = files_in(&production).expect("protected fixture files");
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("listen only for unexpected sign-in TCP connections");
    let sign_in = format!(
        "https://{}/",
        listener.local_addr().expect("fixture address")
    );

    for leaf in [
        MACHINE_LOCAL_DATABASE.to_owned(),
        format!("{MACHINE_LOCAL_DATABASE}-wal"),
        format!("{MACHINE_LOCAL_DATABASE}-shm"),
    ] {
        let link = debug.join(leaf);
        std::os::unix::fs::symlink(&production_database, &link).expect("protected leaf alias");
        for args in [
            &[
                "enroll",
                "--organization",
                ORGANIZATION,
                "--sign-in",
                &sign_in,
            ][..],
            &["enrollment", "status"][..],
        ] {
            let mut process =
                tokio::process::Command::from(compiled_cli_command(home.path(), args, None));
            process.kill_on_drop(true);
            let output = tokio::select! {
                biased;
                connection = listener.accept() => panic!("the linked database path reached sign-in: {:?}", connection.map(|(_, address)| address)),
                result = process.output() => result.expect("run signed Router CLI"),
            };
            assert_status_error(
                &output,
                "machine database and its SQLite sidecars must be regular files",
            );
            assert_eq!(
                files_in(&production).expect("protected fixture files"),
                before
            );
        }
        fs::remove_file(&link).expect("remove just the fixture alias");
    }
    assert!(files_in(&debug).expect("debug fixture files").is_empty());
}

#[tokio::test]
async fn compiled_status_reports_sqlite_busy_without_printing_a_state() {
    let home = tempfile::tempdir().expect("fixture home");
    let root = default_fixture_root(home.path());
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&root)
        .expect("private fixture root");
    load_or_create_machine_id(&root, NOW_MS)
        .await
        .expect("fixture identity");
    let database = root.join(MACHINE_LOCAL_DATABASE);
    let options = SqliteConnectOptions::new()
        .filename(&database)
        .locking_mode(SqliteLockingMode::Exclusive);
    let mut writer = SqliteConnection::connect_with(&options)
        .await
        .expect("open fixture writer");
    let mut held = writer
        .begin_with("BEGIN EXCLUSIVE")
        .await
        .expect("hold SQLite exclusive lock");
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM machine_identity")
        .fetch_one(&mut *held)
        .await
        .expect("hold the WAL database open");

    let output =
        compiled_cli(home.path(), &["enrollment", "status"], None).expect("run signed Router CLI");
    assert_status_error(&output, "the machine database is busy or locked. Try again");
    assert!(matches!(
        read_machine_enrollment_status(&database).await,
        Err(EnrollmentMetadataReadError::Busy(_))
    ));
    held.rollback().await.expect("release exclusive lock");
    writer.close().await.expect("close fixture writer");
}
