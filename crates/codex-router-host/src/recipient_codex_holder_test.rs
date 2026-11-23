use super::{
    ROUTER, SAVED, admission::Admission, complete, create_codex_catalog, seed_codex_thread,
    session_id,
};
use crate::recipient_session_inventory::{ClaudeTerminalSessionInventory, CodexSessionInventory};
use claude_code_peer_messaging::ClaudeCodeSessionRegistry;
use codex_acp_adapter::{
    AcpConnectionInputs, CodexSessionPresence, ConversationOperationRecorder,
    ConversationRecordFuture, HeldBindingCheckout, NativeStoredSessions,
    UnmaterializedBindingStore, serve_acp_connection,
};
use codex_native_integration::{NativePayloadSchemas, NativeSchemaBundle};
use collaboration_protocol::{CodexGeneration, CodexPermissionModeReport, OperationId, SessionId};
use collaboration_runtime::{OpenScratchScopes, UnmaterializedThreadHolder};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::{Value, json};
use sqlx::Connection as _;
use std::{
    collections::BTreeMap, future::ready, io, os::unix::fs::PermissionsExt, path::Path, sync::Arc,
};
use switchboard_message_custody::recipient_sessions::{
    RecipientSessionSources, SessionInventory, SessionInventoryAnswer::NoneFoundAbsenceUnproven,
};
use switchboard_session_store::ProviderOperationStore;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream},
    net::{UnixListener, UnixStream},
    sync::Mutex,
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tokio_util::sync::CancellationToken;

const OPERATION: &str = "019f0000-0000-7000-8000-000000000051";

struct ObservingRecorder(CodexSessionPresence);

impl ConversationOperationRecorder for ObservingRecorder {
    fn admit_create<'a>(
        &'a self,
        _: &'a OperationId,
        _: &'a CodexGeneration,
    ) -> ConversationRecordFuture<'a> {
        Box::pin(ready(Ok(())))
    }

    fn before_native_dispatch<'a>(&'a self, _: &'a OperationId) -> ConversationRecordFuture<'a> {
        Box::pin(ready(Ok(())))
    }

    fn record_created<'a>(
        &'a self,
        _: &'a OperationId,
        session: &'a SessionId,
        _: &'a CodexPermissionModeReport,
    ) -> ConversationRecordFuture<'a> {
        assert!(
            self.0.contains(session),
            "presence must precede record_created"
        );
        Box::pin(ready(Ok(())))
    }

    fn record_failure<'a>(&'a self, _: &'a OperationId, _: bool) -> ConversationRecordFuture<'a> {
        Box::pin(ready(Ok(())))
    }
}

fn native_schemas() -> Arc<NativePayloadSchemas> {
    let mut definitions = serde_json::Map::new();
    for name in [
        "ThreadRead",
        "ThreadFork",
        "ThreadResume",
        "ThreadStart",
        "ThreadLoadedList",
        "TurnInterrupt",
        "TurnStart",
        "TurnSteer",
    ] {
        definitions.insert(format!("{name}Params"), json!({"type": "object"}));
        definitions.insert(format!("{name}Response"), json!({"type": "object"}));
    }
    let schemas = json!({"definitions":{
        "v2": definitions,
        "ServerRequest":{"type":"object"},
        "ServerNotification":{"type":"object"}
    }});
    let bundle = NativeSchemaBundle::from_documents(BTreeMap::from([(
        "codex_app_server_protocol.schemas.json".to_owned(),
        serde_json::to_vec(&schemas).unwrap(),
    )]))
    .unwrap();
    Arc::new(NativePayloadSchemas::from_bundle(&bundle).unwrap())
}

fn new_params(root: &Path) -> Value {
    let scope = format!("session-{ROUTER}");
    let scratch = root.join("scratch").join(&scope);
    std::fs::create_dir_all(&scratch).unwrap();
    std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700)).unwrap();
    let actor = json!({
        "endpoint": {"serviceId": ROUTER, "endpointId": "codex-local"},
        "sessionId": ROUTER,
    });
    json!({"cwd": root, "mcpServers": [], "_meta": {"codexRouter": {
        "operationId": OPERATION, "model": "fixture-model", "effort": "medium",
        "access": "write-restricted", "scratchScope": scope, "scratchPath": scratch,
        "createdBy": actor, "approver": actor,
    }}})
}

struct Frontend {
    stream: BufReader<DuplexStream>,
    route: JoinHandle<io::Result<()>>,
}

impl Frontend {
    async fn open(
        root: &Path,
        holder: Arc<UnmaterializedThreadHolder>,
        retired: CancellationToken,
    ) -> Self {
        let (front, router) = tokio::io::duplex(16 * 1024);
        let recorder = Arc::new(ObservingRecorder(holder.session_presence()));
        let route = tokio::spawn(serve_acp_connection(
            router,
            AcpConnectionInputs {
                backend_path: root.join("native.sock"),
                generation: serde_json::from_value(
                    json!({"serviceEpoch": ROUTER, "generation": 1}),
                )
                .unwrap(),
                schemas: native_schemas(),
                stored_sessions: Arc::new(NativeStoredSessions::new(
                    root.to_owned(),
                    "codex-local".to_owned(),
                )),
                retired,
                holder,
                recorder,
                scratch_scopes: Arc::new(OpenScratchScopes::new()),
            },
        ));
        let mut frontend = Self {
            stream: BufReader::new(front),
            route,
        };
        frontend
            .send(1, "initialize", json!({"protocolVersion":1}))
            .await;
        let reply = frontend.receive(1).await;
        assert!(reply.get("result").is_some(), "{reply}");
        frontend
    }

    async fn send(&mut self, id: u64, method: &str, params: Value) {
        let mut frame =
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
                .unwrap();
        frame.push(b'\n');
        self.stream.write_all(&frame).await.unwrap();
    }

    async fn receive(&mut self, id: u64) -> Value {
        let observed = self.read_frame().await;
        assert_eq!(observed["id"], id);
        observed
    }

    async fn read_frame(&mut self) -> Value {
        let mut line = String::new();
        assert!(self.stream.read_line(&mut line).await.unwrap() > 0);
        serde_json::from_str(&line).unwrap()
    }

    async fn close(self) {
        drop(self.stream);
        self.route.await.unwrap().unwrap();
    }
}

async fn native_request(wire: &mut WebSocketStream<UnixStream>, method: &str) -> Value {
    let frame = wire.next().await.unwrap().unwrap();
    let received: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
    assert_eq!(received["method"], method);
    received
}

async fn native_reply(wire: &mut WebSocketStream<UnixStream>, request: &Value, result: Value) {
    wire.send(Message::text(
        json!({"id": request["id"], "result": result}).to_string(),
    ))
    .await
    .unwrap();
}

fn thread_result(root: &Path) -> Value {
    json!({
        "cwd": root, "model": "fixture-model",
        "thread": {"id": SAVED, "cwd": root, "status": {"type": "idle"}, "turns": []}
    })
}

async fn native_setup(
    listener: &UnixListener,
    root: &Path,
    method: &str,
) -> WebSocketStream<UnixStream> {
    let mut wire = tokio_tungstenite::accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    let initialize = native_request(&mut wire, "initialize").await;
    native_reply(&mut wire, &initialize, json!({})).await;
    native_request(&mut wire, "initialized").await;
    let request = native_request(&mut wire, method).await;
    native_reply(&mut wire, &request, thread_result(root)).await;
    wire
}

#[tokio::test]
async fn attached_session_new_admits_with_three_real_host_sources_and_no_native_lookup() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let mut catalog = create_codex_catalog(root).await;
    let holder = Arc::new(UnmaterializedThreadHolder::new());
    let retirement = CancellationToken::new();
    let listener = UnixListener::bind(root.join("native.sock")).unwrap();
    let mut frontend = Frontend::open(root, holder.clone(), retirement.clone()).await;
    frontend.send(2, "session/new", new_params(root)).await;
    let (mut native, created) = tokio::join!(
        native_setup(&listener, root, "thread/start"),
        frontend.receive(2)
    );
    assert_eq!(created["result"]["sessionId"], SAVED, "{created}");
    assert!(!holder.contains(SAVED), "the frontend owns the binding");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM threads")
            .fetch_one(&mut catalog)
            .await
            .unwrap(),
        0
    );

    let codex = Arc::new(CodexSessionInventory::new(root.to_owned(), holder.clone()));
    let clock = Arc::new(router_clock::ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let managed = super::managed_inventory(Some(Arc::new(Mutex::new(
        ProviderOperationStore::open(&root.join("provider.sqlite"))
            .await
            .unwrap(),
    ))));
    let mut admission = Admission::new(
        root,
        clock.clone(),
        RecipientSessionSources {
            codex: codex.clone(),
            managed_providers: Arc::new(managed),
            claude_terminal: Arc::new(ClaudeTerminalSessionInventory::new(
                Arc::new(ClaudeCodeSessionRegistry::new(
                    root.join("terminal-registry"),
                )),
                clock,
            )),
        },
    )
    .await;
    admission.expect_accepted(&session_id(SAVED)).await;
    admission.expect_deferred(&session_id(super::ABSENT)).await;
    assert!(
        native.next().now_or_never().is_none(),
        "custody sent a native frame"
    );

    // Detaching this frontend returns the same unmaterialized binding to the Host.
    frontend.close().await;
    assert!(holder.contains(SAVED));
    let HeldBindingCheckout::Ready(binding) = holder.checkout(SAVED) else {
        panic!("detached unmaterialized binding must be available for checkout");
    };
    assert_eq!(codex.lookup_session(&session_id(SAVED)).await, complete(1));
    holder.restore(*binding);
    seed_codex_thread(&mut catalog, SAVED, 1, false)
        .await
        .unwrap();
    assert_eq!(codex.lookup_session(&session_id(SAVED)).await, complete(1));

    // A stored row still establishes the identity after the native generation retires.
    retirement.cancel();
    assert_eq!(codex.lookup_session(&session_id(SAVED)).await, complete(1));
    sqlx::query("DELETE FROM threads")
        .execute(&mut catalog)
        .await
        .unwrap();
    catalog.close().await.unwrap();
    assert_eq!(
        codex.lookup_session(&session_id(SAVED)).await,
        NoneFoundAbsenceUnproven
    );
}

#[tokio::test]
async fn ordinary_load_presence_is_fenced_by_generation_before_frontend_drop() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    let holder = Arc::new(UnmaterializedThreadHolder::new());
    let retirement = CancellationToken::new();
    let listener = UnixListener::bind(root.join("native.sock")).unwrap();
    let mut frontend = Frontend::open(root, holder.clone(), retirement.clone()).await;
    frontend
        .send(
            2,
            "session/load",
            json!({"sessionId": SAVED, "cwd":root, "mcpServers": []}),
        )
        .await;
    let (_native, reply) = tokio::join!(
        native_setup(&listener, root, "thread/resume"),
        frontend.receive(2)
    );
    assert!(reply.get("result").is_some(), "{reply}");
    assert!(!holder.contains(SAVED));
    let codex = CodexSessionInventory::new(root.to_owned(), holder);
    assert_eq!(codex.lookup_session(&session_id(SAVED)).await, complete(1));

    retirement.cancel();
    assert_eq!(
        codex.lookup_session(&session_id(SAVED)).await,
        NoneFoundAbsenceUnproven
    );
    frontend.close().await;
}

#[tokio::test]
async fn materialized_prompt_keeps_presence_through_frontend_eof_and_detached_drain() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path();
    create_codex_catalog(root).await.close().await.unwrap();
    let holder = Arc::new(UnmaterializedThreadHolder::new());
    let retirement = CancellationToken::new();
    let listener = UnixListener::bind(root.join("native.sock")).unwrap();
    let mut frontend = Frontend::open(root, holder.clone(), retirement.clone()).await;
    frontend.send(2, "session/new", new_params(root)).await;
    let (mut native, created) = tokio::join!(
        native_setup(&listener, root, "thread/start"),
        frontend.receive(2),
    );
    assert_eq!(created["result"]["sessionId"], SAVED, "{created}");
    let codex = CodexSessionInventory::new(root.to_owned(), holder.clone());
    let target = session_id(SAVED);

    frontend
        .send(
            3,
            "session/prompt",
            json!({
                "sessionId": SAVED, "prompt": [{"type":"text","text":"presence proof"}]
            }),
        )
        .await;
    let read = native_request(&mut native, "thread/read").await;
    native_reply(&mut native, &read, thread_result(root)).await;
    let turn = native_request(&mut native, "turn/start").await;
    native_reply(
        &mut native,
        &turn,
        json!({
            "turn": {"id":"presence-turn","status":"inProgress"}
        }),
    )
    .await;
    native.send(Message::text(json!({
        "method":"item/agentMessage/delta",
        "params":{"threadId":SAVED,"turnId":"presence-turn","itemId":"presence-message","delta":"materialized"}
    }).to_string())).await.unwrap();
    let update = frontend.read_frame().await;
    assert_eq!(update["method"], "session/update", "{update}");
    assert_eq!(codex.lookup_session(&target).await, complete(1));

    frontend.close().await;
    assert!(!holder.contains(SAVED));
    assert_eq!(codex.lookup_session(&target).await, complete(1));
    retirement.cancel();
    assert_eq!(
        codex.lookup_session(&target).await,
        NoneFoundAbsenceUnproven
    );
    holder.drain_host_tasks().await;
}
