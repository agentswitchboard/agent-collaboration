//! The retired journal paths are outside Host startup and shutdown.
#![expect(
    clippy::expect_used,
    clippy::panic,
    reason = "An incomplete fixture or observer must fail the proof."
)]
use codex_router_host::{CollaborationRuntime, CollaborationRuntimeInputs};
use collaboration_client::CollaborationClient;
use collaboration_protocol::EndpointAvailability;
use sqlx::{
    Connection, SqliteConnection,
    sqlite::{SqliteConnectOptions, SqliteJournalMode},
};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

const REGISTRY_NAMES: [&str; 3] = [
    "session-registry.sqlite",
    "session-registry.sqlite-wal",
    "session-registry.sqlite-shm",
];
const CHILD_ROOT: &str = "ROUTER_REGISTRY_PROOF_ROOT";
const CHILD_COMPLETE: &str = "registry Host start and stop completed";

fn private_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().expect("registry proof root");
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
    root
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PathContents {
    Absent,
    Directory(std::time::SystemTime),
    File(std::time::SystemTime, Vec<u8>),
}

fn registry_contents(root: &Path) -> [PathContents; 3] {
    REGISTRY_NAMES.map(|name| {
        let path = root.join(name);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => PathContents::Absent,
            Err(error) => panic!("metadata for {name}: {error}"),
            Ok(metadata) if metadata.is_dir() => {
                PathContents::Directory(metadata.modified().expect("directory timestamp"))
            }
            Ok(metadata) => PathContents::File(
                metadata.modified().expect("file timestamp"),
                fs::read(path).expect("read fixture bytes"),
            ),
        }
    })
}

async fn start_and_stop_host(root: &Path) {
    let clock = crate::provider_test_clock();
    let mut runtime = CollaborationRuntime::start_with_test_clock(
        CollaborationRuntimeInputs {
            directory: root.to_path_buf(),
            codex_home: root.to_path_buf(),
            backend_socket: root.join("backend.sock"),
            mcp_bind: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            native_schema: None,
            peer_registry_directory: None,
            remote_control_server_name: None,
            owner_human_id: None,
        },
        clock,
    )
    .await
    .expect("start Host collaboration runtime");
    runtime
        .backend_ready(
            "2026-11-24T00:00:00Z"
                .to_owned()
                .try_into()
                .expect("fixture time"),
            None,
        )
        .await
        .expect("publish native readiness");
    let client = CollaborationClient::connect(root, "registry-proof", "1")
        .await
        .expect("connect to fixture Host");
    let endpoints = client.list_endpoints().await.expect("published endpoints");
    assert!(matches!(
        endpoints
            .endpoints
            .first()
            .map(|endpoint| &endpoint.availability),
        Some(EndpointAvailability::Available { .. })
    ));
    drop(client);
    runtime
        .shutdown()
        .await
        .expect("stop Host collaboration runtime");
}

#[tokio::test]
async fn absent_registry_stays_absent_when_the_host_starts_and_stops() {
    let root = private_root();
    start_and_stop_host(root.path()).await;
    for name in REGISTRY_NAMES {
        assert!(!root.path().join(name).exists(), "Host created {name}");
    }
}

#[tokio::test]
async fn host_leaves_a_valid_registry_and_its_sidecars_unopened() {
    let root = private_root();
    // The prior journal's legacy schema. A live connection keeps genuine WAL sidecars.
    let mut seed = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(root.path().join(REGISTRY_NAMES[0]))
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true),
    )
    .await
    .expect("open old journal fixture");
    let journal_id = uuid::Uuid::now_v7().to_string();
    sqlx::raw_sql(
        "CREATE TABLE journal_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL, journal_id TEXT NOT NULL, last_sequence INTEGER NOT NULL, retention_clock INTEGER NOT NULL, payload_bytes INTEGER NOT NULL);
         CREATE TABLE lifecycle_records (sequence INTEGER PRIMARY KEY, retention_at INTEGER NOT NULL, observation_json TEXT NOT NULL);
         CREATE TABLE thread_addresses (address_key TEXT PRIMARY KEY, entry_json TEXT NOT NULL, payload_bytes INTEGER NOT NULL);
         CREATE TABLE journal_checkpoint (singleton INTEGER PRIMARY KEY CHECK(singleton=1), sequence INTEGER NOT NULL);
         CREATE TABLE checkpoint_addresses (address_key TEXT PRIMARY KEY, entry_json TEXT NOT NULL);
         INSERT INTO journal_checkpoint VALUES (1,0);",
    )
    .execute(&mut seed)
    .await
    .expect("seed old journal schema");
    sqlx::query("INSERT INTO journal_metadata VALUES (1,1,?,0,0,0)")
        .bind(journal_id)
        .execute(&mut seed)
        .await
        .expect("seed journal identity");
    assert!(
        REGISTRY_NAMES
            .iter()
            .all(|name| root.path().join(name).is_file())
    );
    assert_present_paths_untouched(root.path());
    seed.close().await.expect("close journal fixture");
}

#[test]
fn host_leaves_a_damaged_registry_unopened() {
    let root = private_root();
    fs::write(root.path().join(REGISTRY_NAMES[0]), b"damaged old registry")
        .expect("seed damaged file");
    assert_present_paths_untouched(root.path());
}

#[test]
fn host_leaves_a_directory_at_the_registry_path_unopened() {
    let root = private_root();
    fs::create_dir(root.path().join(REGISTRY_NAMES[0])).expect("seed registry directory");
    assert_present_paths_untouched(root.path());
}

#[tokio::test]
async fn registry_host_child() {
    let Some(root) = std::env::var_os(CHILD_ROOT) else {
        return;
    };
    let _log = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_test_writer()
            .finish(),
    );
    start_and_stop_host(Path::new(&root)).await;
    println!("{CHILD_COMPLETE}");
}

fn assert_present_paths_untouched(root: &Path) {
    let root = root.canonicalize().expect("canonical fixture path");
    let before = registry_contents(&root);
    let selector = crate::child_test_selector(module_path!(), "registry_host_child");
    let executable = std::env::current_exe().expect("Host fixture executable");
    #[cfg(target_os = "macos")]
    let mut child = {
        let paths = REGISTRY_NAMES.map(|name| {
            let path = root.join(name);
            let path = path.to_str().expect("fixture path is UTF-8");
            assert!(!path.contains(['"', '\\']));
            format!("(literal \"{path}\")")
        });
        let profile = format!(
            "(version 1) (allow default) (deny file-read-data file-write* {} (with send-signal SIGKILL))",
            paths.join(" ")
        );
        let mut child = std::process::Command::new("/usr/bin/sandbox-exec");
        child.args(["-p", &profile]).arg(executable);
        child
    };
    #[cfg(not(target_os = "macos"))]
    let mut child = std::process::Command::new(executable);
    #[cfg(target_os = "linux")]
    let access = watch_registry_access(&root);

    let output = child
        .args(["--exact", &selector, "--nocapture"])
        .env(CHILD_ROOT, &root)
        .output()
        .expect("run isolated Host fixture");

    assert!(
        output.status.success(),
        "Host fixture exited {}. stdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    crate::assert_one_child_test_passed(&output.stdout);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(CHILD_COMPLETE));
    for retired_warning in [
        "lifecycle storage",
        "lifecycle maintenance",
        "native observer",
    ] {
        assert!(!stdout.contains(retired_warning), "{stdout}");
    }
    #[cfg(target_os = "linux")]
    assert_no_registry_access(&access);
    assert_eq!(
        registry_contents(&root),
        before,
        "registry paths changed during Host run"
    );
}

#[cfg(target_os = "linux")]
fn watch_registry_access(root: &Path) -> std::os::fd::OwnedFd {
    use rustix::fs::inotify::{self, CreateFlags, WatchFlags};
    let access = inotify::init(CreateFlags::CLOEXEC | CreateFlags::NONBLOCK)
        .expect("open registry access queue");
    inotify::add_watch(
        &access,
        root,
        WatchFlags::OPEN
            | WatchFlags::MODIFY
            | WatchFlags::CREATE
            | WatchFlags::DELETE
            | WatchFlags::MOVED_FROM
            | WatchFlags::MOVED_TO,
    )
    .expect("watch owned root before starting Host");
    access
}

#[cfg(target_os = "linux")]
fn assert_no_registry_access(access: &std::os::fd::OwnedFd) {
    use rustix::fs::inotify::{ReadFlags, Reader};
    let mut buffer = [std::mem::MaybeUninit::uninit(); 4096];
    let mut reader = Reader::new(access, &mut buffer);
    loop {
        match reader.next() {
            Ok(event) => {
                let flags = event.events();
                assert!(!flags.intersects(ReadFlags::IGNORED | ReadFlags::QUEUE_OVERFLOW));
                assert!(
                    event.file_name().is_none_or(|name| {
                        REGISTRY_NAMES
                            .iter()
                            .all(|path| name.to_bytes() != path.as_bytes())
                    }),
                    "Host accessed a retired registry path: {event:?}"
                );
            }
            Err(rustix::io::Errno::INTR) => continue,
            Err(rustix::io::Errno::AGAIN) => break,
            Err(error) => panic!("read registry access queue: {error}"),
        }
    }
}
