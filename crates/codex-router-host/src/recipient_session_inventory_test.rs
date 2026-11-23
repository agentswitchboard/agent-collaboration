use super::{
    ClaudeTerminalSessionInventory, CodexSessionInventory, ManagedProviderSessionInventory,
    UNAVAILABLE, complete,
};
use claude_code_peer_messaging::ClaudeCodeSessionRegistry;
use collaboration_protocol::{
    ProviderIdentity, ProviderRequestedPolicy, ProviderWorkingDirectory, RouterAccess,
};
use collaboration_runtime::UnmaterializedThreadHolder;
use serde_json::json;
use sqlx::{Connection as _, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
};
use switchboard_message_custody::recipient_sessions::{
    CombinedRecipientSessionDirectory, RecipientSessionAnswer, RecipientSessionDirectory,
    SessionId, SessionInventory,
};
use switchboard_session_store::{ProviderOperationStore, ProviderSessionRecord};
use tokio::sync::Mutex;

const SAVED: &str = "019f0000-0000-7000-8000-000000000031";
const ABSENT: &str = "019f0000-0000-7000-8000-000000000032";
const ARCHIVED: &str = "019f0000-0000-7000-8000-000000000033";
const ROUTER: &str = "019f0000-0000-7000-8000-000000000034";

fn session_id(id: &str) -> SessionId {
    id.parse().unwrap()
}

async fn create_codex_catalog(root: &Path) -> SqliteConnection {
    let options = SqliteConnectOptions::new()
        .filename(root.join("state_5.sqlite"))
        .create_if_missing(true);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT, cwd TEXT, model_provider TEXT,
            model TEXT, reasoning_effort TEXT, source TEXT, thread_source TEXT, git_branch TEXT,
            git_origin_url TEXT, name TEXT, title TEXT, preview TEXT,
            first_user_message TEXT NOT NULL DEFAULT '', created_at_ms INTEGER, updated_at_ms INTEGER,
            recency_at_ms INTEGER, archived INTEGER);
         CREATE INDEX idx_threads_created_at_ms ON threads(created_at_ms DESC, id DESC);",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    connection
}

async fn seed_codex_thread(
    connection: &mut SqliteConnection,
    thread_id: &str,
    created_at_ms: i64,
    archived: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO threads (id, title, created_at_ms, updated_at_ms, archived) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(thread_id)
    .bind(ABSENT)
    .bind(created_at_ms)
    .bind(created_at_ms)
    .bind(archived)
    .execute(connection)
    .await
    .map(|_| ())
}

#[tokio::test]
async fn codex_saved_session_counts_across_pages_with_primary_key_uniqueness() {
    let root = tempfile::tempdir().unwrap();
    let mut connection = create_codex_catalog(root.path()).await;
    seed_codex_thread(&mut connection, SAVED, 1, false)
        .await
        .unwrap();
    seed_codex_thread(&mut connection, ARCHIVED, 2, true)
        .await
        .unwrap();
    for number in 100..205 {
        let id = format!("019f0000-0000-7000-8000-{number:012x}");
        seed_codex_thread(&mut connection, &id, number, false)
            .await
            .unwrap();
    }
    let duplicate = seed_codex_thread(&mut connection, SAVED, 3, false).await;
    assert!(matches!(duplicate, Err(sqlx::Error::Database(error)) if error.is_unique_violation()));
    connection.close().await.unwrap();
    let inventory = CodexSessionInventory::new(
        root.path().to_owned(),
        Arc::new(UnmaterializedThreadHolder::new()),
    );

    assert_eq!(
        inventory.lookup_session(&session_id(SAVED)).await,
        complete(1)
    );
    assert_eq!(
        inventory.lookup_session(&session_id(ABSENT)).await,
        UNAVAILABLE
    );
    assert_eq!(
        inventory.lookup_session(&session_id(ARCHIVED)).await,
        UNAVAILABLE
    );
}

#[tokio::test]
async fn codex_catalog_cannot_assert_absence_on_missing_or_unreadable_state() {
    let root = tempfile::tempdir().unwrap();
    let inventory = CodexSessionInventory::new(
        root.path().to_owned(),
        Arc::new(UnmaterializedThreadHolder::new()),
    );
    let target = session_id(SAVED);

    assert_eq!(inventory.lookup_session(&target).await, UNAVAILABLE);
    tokio::fs::write(root.path().join("state_5.sqlite"), b"not a SQLite database")
        .await
        .unwrap();
    assert_eq!(inventory.lookup_session(&target).await, UNAVAILABLE);
}

fn managed_inventory(
    store: Option<Arc<Mutex<ProviderOperationStore>>>,
) -> ManagedProviderSessionInventory {
    ManagedProviderSessionInventory::new(ROUTER.to_owned().try_into().unwrap(), store, None)
        .unwrap()
}

fn provider_record(
    inventory: &ManagedProviderSessionInventory,
    endpoint: usize,
    folder: &Path,
) -> ProviderSessionRecord {
    let target = collaboration_protocol::SessionRef {
        endpoint: inventory.endpoints.get(endpoint).unwrap().clone(),
        session_id: SAVED.to_owned().try_into().unwrap(),
    };
    let actor = ProviderIdentity::Session(target.clone());
    ProviderSessionRecord {
        target,
        working_directory: ProviderWorkingDirectory::try_from(folder.display().to_string())
            .unwrap(),
        requested_policy: ProviderRequestedPolicy {
            access: RouterAccess::WriteRestricted,
        },
        created_by: actor.clone(),
        approver: actor,
        updated_at_ms: 1_763_380_800_000,
        resume: Default::default(),
    }
}

#[tokio::test]
async fn managed_inventory_counts_saved_unloaded_sessions_once_per_endpoint() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Mutex::new(
        ProviderOperationStore::open(&root.path().join("provider-operations.sqlite"))
            .await
            .unwrap(),
    ));
    let inventory = managed_inventory(Some(store.clone()));
    let target = session_id(SAVED);
    let claude = provider_record(&inventory, 0, root.path());
    let cursor = provider_record(&inventory, 1, root.path());

    assert_eq!(inventory.lookup_session(&target).await, complete(0));
    store.lock().await.record_session(&claude).await.unwrap();
    store.lock().await.record_session(&claude).await.unwrap();
    assert_eq!(inventory.lookup_session(&target).await, complete(1));
    store.lock().await.record_session(&cursor).await.unwrap();
    assert_eq!(inventory.lookup_session(&target).await, complete(2));
    assert_eq!(
        inventory.lookup_session(&session_id(ABSENT)).await,
        complete(0)
    );
    assert_eq!(
        managed_inventory(None).lookup_session(&target).await,
        UNAVAILABLE
    );
}

fn seed_peer(directory: &Path, pid: u32, id: &str, writable: bool) {
    let mut record = json!({"pid": pid, "sessionId": id});
    if writable {
        record["peerProtocol"] = json!(1);
        record["messagingSocketPath"] = json!(directory.join("not-open.sock"));
    }
    std::fs::write(
        directory.join(format!("{pid}.json")),
        serde_json::to_vec(&record).unwrap(),
    )
    .unwrap();
}

struct LivePeer(Child);

impl LivePeer {
    fn new() -> Self {
        Self(
            Command::new("/bin/cat")
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for LivePeer {
    fn drop(&mut self) {
        drop(self.0.stdin.take());
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn claude_peer_inventory_counts_writable_unsupported_and_multiple_live_claims() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("peers");
    let inventory =
        ClaudeTerminalSessionInventory::new(Arc::new(ClaudeCodeSessionRegistry::new(path.clone())));
    let target = session_id(SAVED);
    assert_eq!(inventory.lookup_session(&target).await, complete(0));
    std::fs::create_dir(&path).unwrap();

    seed_peer(&path, u32::MAX, SAVED, true);
    assert_eq!(inventory.lookup_session(&target).await, complete(0));
    seed_peer(&path, std::process::id(), SAVED, false);
    assert_eq!(inventory.lookup_session(&target).await, complete(1));
    seed_peer(&path, std::process::id(), SAVED, true);
    assert_eq!(inventory.lookup_session(&target).await, complete(1));

    let live = LivePeer::new();
    seed_peer(&path, live.0.id(), SAVED, false);
    assert_eq!(inventory.lookup_session(&target).await, complete(2));
    assert_eq!(
        inventory.lookup_session(&session_id(ABSENT)).await,
        complete(0)
    );
}

#[tokio::test]
async fn claude_peer_inventory_cannot_assert_absence_on_registry_failures() {
    let root = tempfile::tempdir().unwrap();
    let file_path = root.path().join("not-a-directory");
    std::fs::write(&file_path, []).unwrap();
    let broken =
        ClaudeTerminalSessionInventory::new(Arc::new(ClaudeCodeSessionRegistry::new(file_path)));
    assert_eq!(
        broken.lookup_session(&session_id(ABSENT)).await,
        UNAVAILABLE
    );

    let inventory = ClaudeTerminalSessionInventory::new(Arc::new(ClaudeCodeSessionRegistry::new(
        root.path().to_owned(),
    )));
    std::fs::write(
        root.path().join(format!("{}.json", std::process::id())),
        b"invalid JSON",
    )
    .unwrap();
    assert_eq!(
        inventory.lookup_session(&session_id(ABSENT)).await,
        UNAVAILABLE
    );
}

#[tokio::test]
async fn managed_and_terminal_claims_share_no_authority_to_deduplicate() {
    let root = tempfile::tempdir().unwrap();
    let store = Arc::new(Mutex::new(
        ProviderOperationStore::open(&root.path().join("provider-operations.sqlite"))
            .await
            .unwrap(),
    ));
    let managed = managed_inventory(Some(store.clone()));
    let claude = provider_record(&managed, 0, root.path());
    store.lock().await.record_session(&claude).await.unwrap();
    seed_peer(root.path(), std::process::id(), SAVED, true);
    let peer = ClaudeTerminalSessionInventory::new(Arc::new(ClaudeCodeSessionRegistry::new(
        root.path().to_owned(),
    )));
    let directory = CombinedRecipientSessionDirectory::new(vec![Arc::new(managed), Arc::new(peer)]);

    assert_eq!(
        directory.query_recipient_session(&session_id(SAVED)).await,
        RecipientSessionAnswer::AmbiguousSession
    );
}

#[path = "recipient_codex_holder_test.rs"]
mod codex_holder;

#[path = "recipient_provider_owner_test.rs"]
mod provider_owner;

#[tokio::test]
async fn damaged_endpoint_preserves_other_matches_and_prevents_authoritative_absence() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("operations.sqlite");
    let store = Arc::new(Mutex::new(
        ProviderOperationStore::open(&path).await.unwrap(),
    ));
    let inventory = Arc::new(managed_inventory(Some(store.clone())));
    let claude = provider_record(&inventory, 0, root.path());
    let cursor = provider_record(&inventory, 1, root.path());
    store.lock().await.record_session(&claude).await.unwrap();
    store.lock().await.record_session(&cursor).await.unwrap();

    let mut writer = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&path))
        .await
        .unwrap();
    let changed = sqlx::query("UPDATE provider_session_records SET requested_policy_json = ? WHERE target_endpoint_id = ?")
        .bind("invalid JSON")
        .bind("cursor-local")
        .execute(&mut writer)
        .await
        .unwrap();
    assert_eq!(changed.rows_affected(), 1);
    writer.close().await.unwrap();
    let id = session_id(SAVED);
    assert!(
        store
            .lock()
            .await
            .session_record(&cursor.target)
            .await
            .is_err()
    );
    assert_eq!(
        inventory.lookup_session(&id).await,
        switchboard_message_custody::recipient_sessions::SessionInventoryAnswer {
            at_least: 1,
            complete: false
        }
    );
    let directory = CombinedRecipientSessionDirectory::new(vec![inventory]);
    assert_eq!(
        directory.query_recipient_session(&id).await,
        RecipientSessionAnswer::CouldNotTell
    );
}
