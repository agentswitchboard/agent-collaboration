use super::*;
use admission::Admission;
use switchboard_message_custody::recipient_sessions::SessionInventoryAnswer;

async fn with_real_sources(
    root: &Path,
    codex_has_match: bool,
) -> (Admission, Arc<ClaudeTerminalSessionInventory>) {
    let mut catalog = create_codex_catalog(root).await;
    if codex_has_match {
        seed_codex_thread(&mut catalog, SAVED, 1, false)
            .await
            .unwrap();
    }
    catalog.close().await.unwrap();
    let codex =
        CodexSessionInventory::new(root.to_owned(), Arc::new(UnmaterializedThreadHolder::new()));
    let managed = managed_inventory(Some(Arc::new(Mutex::new(
        ProviderOperationStore::open(&root.join("provider.sqlite"))
            .await
            .unwrap(),
    ))));
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let terminal = Arc::new(ClaudeTerminalSessionInventory::new(
        Arc::new(ClaudeCodeSessionRegistry::new(root.join("registry"))),
        clock.clone(),
    ));
    let admission = Admission::new(
        root,
        clock,
        RecipientSessionSources {
            codex: Arc::new(codex),
            managed_providers: Arc::new(managed),
            claude_terminal: terminal.clone(),
        },
    )
    .await;
    (admission, terminal)
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
    let (mut admission, terminal) = with_real_sources(root.path(), true).await;
    let target = session_id(SAVED);

    assert_eq!(terminal.lookup_session(&target).await, UNAVAILABLE);
    admission.expect_deferred(&target).await;
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
    let (mut admission, terminal) = with_real_sources(root.path(), true).await;
    let target = session_id(SAVED);

    assert_eq!(
        terminal.lookup_session(&target).await,
        SessionInventoryAnswer {
            at_least: 1,
            complete: false,
        }
    );
    admission.expect_ambiguous(&target).await;
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
    let (mut admission, terminal) = with_real_sources(root.path(), false).await;
    let target = session_id(SAVED);

    assert_eq!(terminal.lookup_session(&target).await, complete(1));
    admission.expect_deferred(&target).await;
}
