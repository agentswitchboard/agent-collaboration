use super::*;
use admission::Admission;
use codex_holder::native_reply;
use codex_native_integration::NativeProtocolConnection;
use collaboration_runtime::{NativeGenerationGate, NativeRelayListener};
use futures_util::{FutureExt, StreamExt};
use native_list_fixture::NativeListFixture;
use std::{os::unix::fs::PermissionsExt, pin::pin, time::Duration};
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
    clock: Arc<router_clock::ManualClock>,
}

impl HostAdmission {
    async fn new(root: &Path, state: CodexCatalogue) -> Self {
        Self::with_gate(root, state, NativeGenerationGate::default()).await
    }

    async fn with_gate(
        root: &Path,
        state: CodexCatalogue,
        native_gate: NativeGenerationGate,
    ) -> Self {
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
            native_gate,
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
            clock.clone(),
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
            clock,
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
    prove_managed_admission(0, root.path(), CodexCatalogue::Missing).await;
}

#[tokio::test]
async fn managed_cursor_is_kept_only_with_a_conclusive_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    prove_managed_admission(1, root.path(), CodexCatalogue::Empty).await;
}

async fn prove_managed_admission(endpoint: usize, root: &Path, catalogue: CodexCatalogue) {
    let native = NativeListFixture::bind(root);
    let mut host = HostAdmission::with_gate(root, catalogue, native.gate.clone()).await;
    let target = session_id(SAVED);
    host.save_managed(endpoint, root, &target).await;
    native
        .run(host.inbox.expect_accepted(&target), loaded(&[]))
        .await;
}

fn loaded(ids: &[&str]) -> serde_json::Value {
    json!({"data":ids, "nextCursor":null})
}

#[tokio::test]
async fn live_terminal_is_kept_only_with_a_conclusive_codex_miss() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("registry");
    std::fs::create_dir(&path).unwrap();
    seed_peer(&path, std::process::id(), SAVED, true);
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    let target = session_id(SAVED);
    assert_eq!(host.terminal.lookup_session(&target).await, complete(1));
    native
        .run(host.inbox.expect_accepted(&target), loaded(&[]))
        .await;
}

#[tokio::test]
async fn uncovered_codex_miss_with_zero_matches_leaves_no_refusal() {
    for state in [CodexCatalogue::Missing, CodexCatalogue::Empty] {
        let root = tempfile::tempdir().unwrap();
        let mut host = HostAdmission::new(root.path(), state).await;
        let target = session_id(SAVED);

        assert_eq!(host.codex.lookup_session(&target).await, AtLeast(0));
        host.inbox.expect_deferred(&target).await;
    }
}

#[tokio::test]
async fn non_uuid_address_with_no_matches_gets_a_durable_unknown_session_refusal() {
    for state in [CodexCatalogue::Missing, CodexCatalogue::Empty] {
        let root = tempfile::tempdir().unwrap();
        let native = NativeListFixture::bind(root.path());
        let mut host = HostAdmission::with_gate(root.path(), state, native.gate.clone()).await;
        let target = session_id("provider-session-without-a-uuid");

        tokio::select! {
            answer = host.codex.lookup_session(&target) => assert_eq!(answer, complete(0)),
            _ = native.listener.accept() => panic!("non-UUID inventory lookup opened a native connection"),
        }
        tokio::select! {
            () = host.inbox.expect_unknown(&target) => {},
            _ = native.listener.accept() => panic!("non-UUID custody offer opened a native connection"),
        }
        native.assert_no_connection();
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
async fn relay_thread_with_no_turn_is_kept_then_same_uuid_in_claude_is_ambiguous() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let native = NativeListFixture::bind(root);
    let relay_path = root.join("relay.sock");
    let relay = NativeRelayListener::bind(&relay_path, native.gate.clone()).unwrap();
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
        super::codex_holder::native_setup(&native.listener, root, "thread/start")
    );
    assert_eq!(created["thread"]["id"], SAVED);
    let mut host = HostAdmission::with_gate(root, CodexCatalogue::Empty, native.gate.clone()).await;
    let target = session_id(SAVED);

    native
        .run(host.inbox.expect_accepted(&target), loaded(&[SAVED]))
        .await;
    host.save_managed(0, root, &target).await;
    assert_eq!(host.managed.lookup_session(&target).await, complete(1));
    native
        .run(host.inbox.expect_ambiguous(&target), loaded(&[SAVED]))
        .await;
    assert!(
        backend_wire.next().now_or_never().is_none(),
        "custody probed the opaque thread"
    );
    drop(client);
    stop.cancel();
    relay_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn materialize_and_unload_after_first_catalogue_miss_is_kept() {
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let native = NativeListFixture::bind(root);
    let mut host = HostAdmission::with_gate(root, CodexCatalogue::Empty, native.gate.clone()).await;
    let target = session_id(SAVED);
    let mut offer = pin!(host.inbox.expect_accepted(&target));
    let (mut wire, request) = tokio::select! {
        _ = &mut offer => panic!("first stored miss must reach the live list"),
        request = native.request() => request,
    };
    let mut catalog = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(root.join("state_5.sqlite")),
    )
    .await
    .unwrap();
    seed_codex_thread(&mut catalog, SAVED, 1, false)
        .await
        .unwrap();
    catalog.close().await.unwrap();
    // The live thread materialized into the catalogue, then unloaded before
    // the managed process took its loaded-thread observation.
    native_reply(&mut wire, &request, loaded(&[])).await;
    offer.await;
    native.assert_no_connection();
}

#[tokio::test]
async fn continued_native_list_defers_even_with_a_matching_id_on_the_page() {
    let root = tempfile::tempdir().unwrap();
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    native
        .run(
            host.inbox.expect_deferred(&session_id(SAVED)),
            json!({"data":[SAVED],"nextCursor":"unexpected-continuation"}),
        )
        .await;
}

#[tokio::test]
async fn malformed_native_list_cannot_authorize_a_refusal() {
    let root = tempfile::tempdir().unwrap();
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    native
        .run(
            host.inbox.expect_deferred(&session_id(SAVED)),
            json!({"data":[42],"nextCursor":null}),
        )
        .await;
}

#[tokio::test]
async fn injected_custody_deadline_defers_when_native_list_is_held() {
    let root = tempfile::tempdir().unwrap();
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    let target = session_id(SAVED);
    let clock = host.clock.clone();
    let mut offer = pin!(host.inbox.expect_deferred(&target));
    let (_wire, _request) = tokio::select! {
        _ = &mut offer => panic!("the native response must still be outstanding"),
        request = native.request() => request,
    };
    clock.advance(Duration::from_secs(30)).unwrap();
    offer.await;
    native.assert_no_connection();
}

#[tokio::test]
async fn generation_replacement_makes_the_outstanding_native_list_inconclusive() {
    let root = tempfile::tempdir().unwrap();
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    let target = session_id(SAVED);
    let mut offer = pin!(host.inbox.expect_deferred(&target));
    let (mut wire, request) = tokio::select! {
        _ = &mut offer => panic!("the native response must still be outstanding"),
        request = native.request() => request,
    };
    native.gate.retire().unwrap();
    native.activate(root.path(), 2);
    native_reply(&mut wire, &request, loaded(&[])).await;
    offer.await;
    native.assert_no_connection();
}

#[tokio::test]
async fn all_authorities_empty_refuse_once_and_a_fresh_lookup_can_see_a_new_thread() {
    let root = tempfile::tempdir().unwrap();
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    let target = session_id(SAVED);
    native
        .run(host.inbox.expect_unknown(&target), loaded(&[]))
        .await;
    native.assert_no_connection();
    native
        .run(host.inbox.expect_accepted(&target), loaded(&[SAVED]))
        .await;
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

    assert_eq!(host.codex.lookup_session(&target).await, AtLeast(0));
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
    let target = session_id(SAVED);
    std::fs::write(
        path.join(format!("{pid}.json")),
        json!({"pid":pid,"sessionId":target,"startedAt":0}).to_string(),
    )
    .unwrap();
    seed_peer(&path, std::process::id(), target.as_str(), true);
    let native = NativeListFixture::bind(root.path());
    let mut host =
        HostAdmission::with_gate(root.path(), CodexCatalogue::Empty, native.gate.clone()).await;
    assert_eq!(host.terminal.lookup_session(&target).await, complete(1));
    native
        .run(host.inbox.expect_accepted(&target), loaded(&[]))
        .await;
}
