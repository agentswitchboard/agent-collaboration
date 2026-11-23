use super::*;
use admission::Admission;

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

    async fn save_managed(&self, endpoint: usize, root: &Path) {
        let record = provider_record(&self.managed, endpoint, root);
        self.provider_store
            .lock()
            .await
            .record_session(&record)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn managed_claude_is_kept_on_a_mac_with_no_codex_state_file() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Missing).await;
    let target = session_id(SAVED);
    host.save_managed(0, root.path()).await;

    assert_eq!(
        host.codex.lookup_session(&target).await,
        NoneFoundAbsenceUnproven
    );
    host.inbox.expect_accepted(&target).await;
}

#[tokio::test]
async fn managed_cursor_is_kept_when_the_codex_catalogue_is_empty() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let target = session_id(SAVED);
    host.save_managed(1, root.path()).await;

    assert_eq!(
        host.codex.lookup_session(&target).await,
        NoneFoundAbsenceUnproven
    );
    host.inbox.expect_accepted(&target).await;
}

#[tokio::test]
async fn live_terminal_is_kept_when_the_codex_catalogue_is_empty() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("registry");
    std::fs::create_dir(&path).unwrap();
    seed_peer(&path, std::process::id(), SAVED, true);
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let target = session_id(SAVED);

    assert_eq!(
        host.codex.lookup_session(&target).await,
        NoneFoundAbsenceUnproven
    );
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
async fn failed_codex_read_with_a_managed_claude_match_still_defers() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Unreadable).await;
    let target = session_id(SAVED);
    host.save_managed(0, root.path()).await;

    assert_eq!(host.codex.lookup_session(&target).await, AtLeast(0));
    host.inbox.expect_deferred(&target).await;
}

#[tokio::test]
async fn two_managed_matches_are_ambiguous_with_an_uncovered_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let target = session_id(SAVED);
    host.save_managed(0, root.path()).await;
    host.save_managed(1, root.path()).await;

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
    std::fs::write(
        path.join(format!("{pid}.json")),
        json!({"pid":pid,"sessionId":SAVED,"startedAt":0}).to_string(),
    )
    .unwrap();
    seed_peer(&path, std::process::id(), SAVED, true);
    let mut host = HostAdmission::new(root.path(), CodexCatalogue::Empty).await;
    let target = session_id(SAVED);

    assert_eq!(host.terminal.lookup_session(&target).await, complete(1));
    host.inbox.expect_accepted(&target).await;
}
