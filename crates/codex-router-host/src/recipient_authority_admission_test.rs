use super::*;
use admission::Admission;
use codex_native_integration::NativeProtocolConnection;
use collaboration_runtime::{NativeGenerationGate, NativeRelayListener};
use futures_util::{FutureExt, StreamExt};
use std::os::unix::fs::PermissionsExt;
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum CodexCatalogue {
    Missing,
    Empty,
    HasTarget,
    Unreadable,
}

struct HostAdmission {
    inbox: Admission,
    codex: Arc<CodexSessionInventory>,
    managed: Arc<ManagedProviderSessionInventory>,
    terminal: Arc<ClaudeTerminalSessionInventory>,
    provider_store: Arc<Mutex<ProviderOperationStore>>,
}

impl HostAdmission {
    async fn new(root: &Path, state: CodexCatalogue) -> Self {
        match state {
            CodexCatalogue::Missing => {}
            CodexCatalogue::Unreadable => {
                std::fs::write(root.join("state_5.sqlite"), b"not SQLite").unwrap();
            }
            CodexCatalogue::Empty | CodexCatalogue::HasTarget => {
                let mut catalog = create_codex_catalog(root).await;
                if matches!(state, CodexCatalogue::HasTarget) {
                    seed_codex_thread(&mut catalog, SAVED, 1, false)
                        .await
                        .unwrap();
                }
                catalog.close().await.unwrap();
            }
        }
        let codex = Arc::new(CodexSessionInventory::new(
            root.to_owned(),
            Arc::new(UnmaterializedThreadHolder::new()),
        ));
        let provider_store = Arc::new(Mutex::new(
            ProviderOperationStore::open(&root.join("provider.sqlite"))
                .await
                .unwrap(),
        ));
        let managed = Arc::new(managed_inventory(Some(provider_store.clone())));
        let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let terminal = Arc::new(ClaudeTerminalSessionInventory::new(
            Arc::new(ClaudeCodeSessionRegistry::new(root.join("registry"))),
            clock.clone(),
        ));
        let inbox = Admission::new(
            root,
            clock,
            RecipientSessionSources {
                codex: codex.clone(),
                managed_providers: managed.clone(),
                claude_terminal: terminal.clone(),
            },
        )
        .await;
        Self {
            inbox,
            codex,
            managed,
            terminal,
            provider_store,
        }
    }

    async fn save_managed(&self, endpoint: usize, root: &Path, target: &SessionId) {
        let record = provider_record_for_session(&self.managed, endpoint, root, target);
        self.provider_store
            .lock()
            .await
            .record_session(&record)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn managed_claude_is_kept_only_with_a_conclusive_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Missing).await;
    prove_managed_admission(&mut host, 0, root.path()).await;
}

#[tokio::test]
async fn managed_cursor_is_kept_only_with_a_conclusive_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    prove_managed_admission(&mut host, 1, root.path()).await;
}

async fn prove_managed_admission(host: &mut HostAdmission, endpoint: usize, root: &Path) {
    let uuid_target = session_id(SAVED);
    host.save_managed(endpoint, root, &uuid_target).await;

    assert_eq!(
        host.codex.lookup_session(&uuid_target).await,
        NoneFoundAbsenceUnproven
    );
    host.inbox.expect_deferred(&uuid_target).await;

    let target = session_id("managed-provider-only-session");
    host.save_managed(endpoint, root, &target).await;
    assert_eq!(host.codex.lookup_session(&target).await, complete(0));
    host.inbox.expect_accepted(&target).await;
}

#[tokio::test]
async fn live_terminal_is_kept_only_with_a_conclusive_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("registry");
    std::fs::create_dir(&path).unwrap();
    seed_peer(&path, std::process::id(), SAVED, true);
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let uuid_target = session_id(SAVED);

    assert_eq!(
        host.codex.lookup_session(&uuid_target).await,
        NoneFoundAbsenceUnproven
    );
    assert_eq!(
        host.terminal.lookup_session(&uuid_target).await,
        complete(1)
    );
    host.inbox.expect_deferred(&uuid_target).await;

    let target = session_id("terminal-provider-only-session");
    seed_peer(&path, std::process::id(), target.as_str(), true);
    assert_eq!(host.codex.lookup_session(&target).await, complete(0));
    assert_eq!(host.terminal.lookup_session(&target).await, complete(1));
    host.inbox.expect_accepted(&target).await;
}

#[tokio::test]
async fn uncovered_codex_miss_with_zero_matches_leaves_no_refusal() {
    for state in [CodexCatalogue::Missing, CodexCatalogue::Empty] {
        let root = tempfile::tempdir().unwrap();
        let mut host = HostAdmission::new(root.path(), state).await;
        let target = session_id(SAVED);

        assert_eq!(
            host.codex.lookup_session(&target).await,
            NoneFoundAbsenceUnproven
        );
        host.inbox.expect_deferred(&target).await;
    }
}

#[tokio::test]
async fn non_uuid_address_with_no_matches_gets_a_durable_unknown_session_refusal() {
    for state in [CodexCatalogue::Missing, CodexCatalogue::Empty] {
        let root = tempfile::tempdir().unwrap();
        let mut host = HostAdmission::new(root.path(), state).await;
        let target = session_id("provider-session-without-a-uuid");

        assert_eq!(host.codex.lookup_session(&target).await, complete(0));
        host.inbox.expect_unknown(&target).await;
    }
}

#[tokio::test]
async fn saved_codex_row_is_evidence_even_with_a_non_uuid_id() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let target = session_id("historical-catalogue-id");
    let mut catalog = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(root.path().join("state_5.sqlite")),
    )
    .await
    .unwrap();
    seed_codex_thread(&mut catalog, target.as_str(), 1, false)
        .await
        .unwrap();
    catalog.close().await.unwrap();

    assert_eq!(host.codex.lookup_session(&target).await, complete(1));
    host.inbox.expect_accepted(&target).await;
}

#[tokio::test]
async fn same_uuid_opaque_codex_and_visible_claude_defer_without_custody() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let backend_path = root.join("native.sock");
    let backend = UnixListener::bind(&backend_path).unwrap();
    let generations = NativeGenerationGate::default();
    generations
        .activate(
            serde_json::from_value(json!({"serviceEpoch": ROUTER, "generation": 1})).unwrap(),
            backend_path,
            None,
        )
        .unwrap();
    let relay_path = root.join("relay.sock");
    let relay = NativeRelayListener::bind(&relay_path, generations).unwrap();
    let stop = CancellationToken::new();
    let relay_task = tokio::spawn(relay.run(stop.clone()));
    let client = async {
        let mut client = NativeProtocolConnection::connect(&relay_path)
            .await
            .unwrap();
        let created = client.request("thread/start", json!({})).await.unwrap();
        (client, created)
    };
    let ((client, created), mut backend_wire) = tokio::join!(
        client,
        super::codex_holder::native_setup(&backend, root, "thread/start")
    );
    assert_eq!(created["thread"]["id"], SAVED);
    let mut host = HostAdmission::new(root, CodexCatalogue::Empty).await;
    let target = session_id(SAVED);
    host.save_managed(0, root, &target).await;
    assert_eq!(host.managed.lookup_session(&target).await, complete(1));
    assert_eq!(
        host.codex.lookup_session(&target).await,
        NoneFoundAbsenceUnproven
    );

    host.inbox.expect_deferred(&target).await;
    assert!(
        backend_wire.next().now_or_never().is_none(),
        "custody probed the opaque thread"
    );
    drop(client);
    stop.cancel();
    relay_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn failed_codex_read_with_a_managed_claude_match_still_defers() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Unreadable).await;
    let target = session_id("managed-provider-only-session");
    host.save_managed(0, root.path(), &target).await;

    assert_eq!(host.codex.lookup_session(&target).await, AtLeast(0));
    host.inbox.expect_deferred(&target).await;
}

#[tokio::test]
async fn two_managed_matches_are_ambiguous_with_an_uncovered_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let target = session_id(SAVED);
    host.save_managed(0, root.path(), &target).await;
    host.save_managed(1, root.path(), &target).await;

    assert_eq!(
        host.codex.lookup_session(&target).await,
        NoneFoundAbsenceUnproven
    );
    host.inbox.expect_ambiguous(&target).await;
}

#[tokio::test]
async fn unidentified_live_terminal_defers_otherwise_single_codex_match() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("registry");
    std::fs::create_dir(&path).unwrap();
    let pid = std::process::id();
    std::fs::write(
        path.join(format!("{pid}.json")),
        json!({"pid":pid}).to_string(),
    )
    .unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::HasTarget).await;
    let target = session_id(SAVED);

    assert_eq!(host.terminal.lookup_session(&target).await, UNAVAILABLE);
    host.inbox.expect_deferred(&target).await;
}

#[tokio::test]
async fn verified_terminal_claim_survives_unreadable_live_peer_through_custody() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("registry");
    std::fs::create_dir(&path).unwrap();
    let unreadable = LivePeer::new();
    std::fs::write(
        path.join(format!("{}.json", unreadable.0.id())),
        b"not JSON",
    )
    .unwrap();
    seed_peer(&path, std::process::id(), SAVED, true);
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::HasTarget).await;
    let target = session_id(SAVED);

    assert_eq!(host.terminal.lookup_session(&target).await, AtLeast(1));
    host.inbox.expect_ambiguous(&target).await;
}

#[tokio::test]
async fn reused_terminal_pid_cannot_create_a_permanent_custody_refusal() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("registry");
    std::fs::create_dir(&path).unwrap();
    let unrelated_process = LivePeer::new();
    let pid = unrelated_process.0.id();
    let target = session_id("terminal-provider-only-session");
    std::fs::write(
        path.join(format!("{pid}.json")),
        json!({"pid":pid,"sessionId":target,"startedAt":0}).to_string(),
    )
    .unwrap();
    seed_peer(&path, std::process::id(), target.as_str(), true);
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    assert_eq!(host.terminal.lookup_session(&target).await, complete(1));
    host.inbox.expect_accepted(&target).await;
}
