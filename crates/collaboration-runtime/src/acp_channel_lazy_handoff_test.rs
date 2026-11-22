use super::*;
use codex_acp_adapter::{ConversationOperationRecorder, ConversationRecordFuture};
use codex_native_integration::{NativePayloadSchemas, NativeSchemaBundle};
use collaboration_protocol::{CodexGeneration, CodexPermissionModeReport, OperationId, SessionId};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Mutex, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixListener, UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const BOUND: Duration = Duration::from_secs(5);
const SERVICE: &str = "00000000-0000-4000-8000-000000000001";
const SESSION: &str = "lazy-native-thread";

struct WaitingCatalog(Mutex<Option<oneshot::Sender<()>>>);

impl AcpStoredSessions for WaitingCatalog {
    fn list(&self, _params: Value) -> Pin<Box<dyn Future<Output = io::Result<Value>> + Send + '_>> {
        let started = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Box::pin(async move {
            if let Some(started) = started {
                let _test_gone = started.send(());
            }
            std::future::pending().await
        })
    }
}

struct RecordingCreate;

impl ConversationOperationRecorder for RecordingCreate {
    fn admit_create<'a>(
        &'a self,
        _: &'a OperationId,
        _: &'a CodexGeneration,
    ) -> ConversationRecordFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn before_native_dispatch<'a>(&'a self, _: &'a OperationId) -> ConversationRecordFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn record_created<'a>(
        &'a self,
        _: &'a OperationId,
        _: &'a SessionId,
        _: &'a CodexPermissionModeReport,
    ) -> ConversationRecordFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn record_failure<'a>(&'a self, _: &'a OperationId, _: bool) -> ConversationRecordFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}

fn native_schemas() -> Result<Arc<NativePayloadSchemas>, Box<dyn std::error::Error + Send + Sync>> {
    let mut definitions = serde_json::Map::new();
    for method in [
        "ThreadRead",
        "ThreadResume",
        "ThreadStart",
        "ThreadLoadedList",
        "TurnStart",
        "TurnSteer",
        "TurnInterrupt",
    ] {
        definitions.insert(format!("{method}Params"), json!({"type":"object"}));
        definitions.insert(format!("{method}Response"), json!({"type":"object"}));
    }
    let bundle = NativeSchemaBundle::from_documents(BTreeMap::from([(
        "codex_app_server_protocol.schemas.json".to_owned(),
        serde_json::to_vec(
            &json!({"definitions":{"v2":definitions,"ServerRequest":{"type":"object"},"ServerNotification":{"type":"object"}}}),
        )?,
    )]))?;
    Ok(Arc::new(NativePayloadSchemas::from_bundle(&bundle)?))
}

type FixtureError = Box<dyn std::error::Error + Send + Sync>;

struct Frontend {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Frontend {
    async fn connect(path: &Path) -> Result<Self, FixtureError> {
        let (reader, writer) = UnixStream::connect(path).await?.into_split();
        let mut frontend = Self {
            reader: BufReader::new(reader),
            writer,
        };
        let reply = frontend
            .call(1, "initialize", json!({"protocolVersion":1}))
            .await?;
        if reply.get("result").is_none() {
            return Err(format!("initialize: {reply}").into());
        }
        Ok(frontend)
    }

    async fn send(&mut self, id: u64, method: &str, params: Value) -> Result<(), FixtureError> {
        self.writer
            .write_all(
                format!(
                    "{}\n",
                    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                )
                .as_bytes(),
            )
            .await?;
        Ok(())
    }

    async fn call(&mut self, id: u64, method: &str, params: Value) -> Result<Value, FixtureError> {
        self.send(id, method, params).await?;
        let mut reply = String::new();
        self.reader.read_line(&mut reply).await?;
        let reply: Value = serde_json::from_str(&reply)?;
        if reply["id"] != id {
            return Err(format!("unexpected reply to {method}: {reply}").into());
        }
        Ok(reply)
    }
}

async fn native_request(
    wire: &mut WebSocketStream<UnixStream>,
    method: &str,
) -> Result<Value, FixtureError> {
    let frame = wire
        .next()
        .await
        .ok_or_else(|| format!("native socket closed before {method}"))??;
    let request: Value = serde_json::from_str(frame.to_text()?)?;
    if request["method"] != method {
        return Err(format!("expected {method}, got {request}").into());
    }
    Ok(request)
}

async fn native_reply(
    wire: &mut WebSocketStream<UnixStream>,
    request: &Value,
    result: Value,
) -> Result<(), FixtureError> {
    wire.send(Message::text(
        json!({"id":request["id"],"result":result}).to_string(),
    ))
    .await?;
    Ok(())
}

async fn open_native_thread(
    listener: UnixListener,
) -> Result<WebSocketStream<UnixStream>, FixtureError> {
    let mut wire = tokio_tungstenite::accept_async(listener.accept().await?.0).await?;
    let request = native_request(&mut wire, "initialize").await?;
    native_reply(&mut wire, &request, json!({})).await?;
    native_request(&mut wire, "initialized").await?;
    let request = native_request(&mut wire, "thread/start").await?;
    let profile = request["params"]["permissions"]
        .as_str()
        .ok_or("missing native permission profile")?;
    let config = &request["params"]["config"];
    let filesystem = config
        .get(format!("permissions.{profile}.filesystem"))
        .and_then(Value::as_object)
        .ok_or("missing native filesystem profile")?;
    let writable_roots: Vec<_> = filesystem
        .iter()
        .filter_map(|(path, access)| (access == "write").then_some(path))
        .collect();
    let sandbox = json!({"type":"workspaceWrite","writableRoots":writable_roots,"networkAccess":true,
        "excludeTmpdirEnvVar":filesystem.get(":tmpdir").is_some_and(|access| access == "read"),
        "excludeSlashTmp":filesystem.get(":slash_tmp").is_some_and(|access| access == "read")});
    native_reply(&mut wire, &request, json!({
        "cwd":"/tmp","model":"gpt-5.6-sol","approvalPolicy":"on-request","approvalsReviewer":"auto_review",
        "activePermissionProfile":{"id":"router-workspace-write","extends":":workspace"},
        "sandbox":sandbox,
        "thread":{"id":SESSION,"cwd":"/tmp","status":{"type":"idle"},"turns":[]}
    })).await?;
    Ok(wire)
}

struct ListenerFixture {
    root: tempfile::TempDir,
    permit: Arc<Semaphore>,
    holder: Arc<crate::UnmaterializedThreadHolder>,
    stop: CancellationToken,
    catalogue_started: oneshot::Receiver<()>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

impl ListenerFixture {
    fn start() -> Result<(Self, UnixListener), FixtureError> {
        use std::os::unix::fs::PermissionsExt as _;
        let root = tempfile::tempdir()?;
        std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
        let backend_socket = root.path().join("native.sock");
        let backend = UnixListener::bind(&backend_socket)?;
        let gate = NativeGenerationGate::default();
        gate.activate(
            serde_json::from_value(json!({"serviceEpoch":SERVICE,"generation":1}))?,
            backend_socket,
            Some(native_schemas()?),
        )?;
        let holder = Arc::new(crate::UnmaterializedThreadHolder::new());
        let permit = Arc::new(Semaphore::new(1));
        let (catalogue_entry, catalogue_started) = oneshot::channel();
        let listener = AcpChannelListener::bind(
            &root.path().join("acp.sock"),
            SERVICE.to_owned().try_into()?,
            gate,
            Arc::new(WaitingCatalog(Mutex::new(Some(catalogue_entry)))),
            Arc::clone(&holder),
            Arc::new(RecordingCreate),
            Arc::new(crate::OpenScratchScopes::new()),
        )?
        .with_connection_budget(Arc::clone(&permit));
        let stop = CancellationToken::new();
        let task = tokio::spawn(listener.run(stop.clone()));
        Ok((
            Self {
                root,
                permit,
                holder,
                stop,
                catalogue_started,
                task,
            },
            backend,
        ))
    }

    async fn open_session(&self) -> Result<Frontend, FixtureError> {
        use std::os::unix::fs::PermissionsExt as _;
        let scratch_id = "session-00000000-0000-4000-8000-000000000099";
        let scratch = self.root.path().join("scratch").join(scratch_id);
        std::fs::create_dir_all(&scratch)?;
        std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700))?;
        let actor = json!({"endpoint":{"serviceId":SERVICE,"endpointId":"codex-local"},"sessionId":"creator"});
        let mut client = Frontend::connect(&self.root.path().join("acp.sock")).await?;
        let reply = client.call(2, "session/new", json!({"cwd":"/tmp","mcpServers":[],"_meta":{"codexRouter":{
            "operationId":OperationId::generate(),"model":"gpt-5.6-sol","effort":"medium","access":"workspace-write",
            "scratchScope":scratch_id,"scratchPath":scratch,"createdBy":actor,"approver":actor
        }}})).await?;
        if reply["result"]["sessionId"] != SESSION {
            return Err(format!("create binding: {reply}").into());
        }
        Ok(client)
    }

    async fn hold_catalogue(&mut self, client: &mut Frontend) -> Result<(), FixtureError> {
        client.send(4, "session/list", json!({})).await?;
        (&mut self.catalogue_started).await?;
        Ok(())
    }

    async fn await_frontend_return(&self) -> Result<(), FixtureError> {
        drop(
            tokio::time::timeout(BOUND, self.permit.acquire())
                .await
                .map_err(|_| "Host listener did not release the frontend permit")??,
        );
        if self.stop.is_cancelled() {
            return Err("Host stopped before frontend handoff".into());
        }
        Ok(())
    }

    async fn stop(self) -> Result<(), FixtureError> {
        self.stop.cancel();
        tokio::time::timeout(BOUND, self.task)
            .await
            .map_err(|_| "Host listener did not drain")???;
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn lazy_frontend_eof_returns_ready_binding_before_reconnect() {
    tokio::time::timeout(BOUND, async {
        let (mut fixture, backend) = ListenerFixture::start().unwrap();
        let backend = tokio::spawn(open_native_thread(backend));
        let mut client = fixture.open_session().await.unwrap();
        let _native_wire = backend.await.unwrap().unwrap();
        assert!(
            !fixture.holder.contains(SESSION),
            "ready binding belongs to the first frontend"
        );
        fixture.hold_catalogue(&mut client).await.unwrap();
        drop(client);
        fixture.await_frontend_return().await.unwrap();
        assert!(
            fixture.holder.contains(SESSION),
            "lazy frontend returned before handing off the Ready binding"
        );
        let mut client = Frontend::connect(&fixture.root.path().join("acp.sock"))
            .await
            .unwrap();
        let reply = client
            .call(
                3,
                "session/load",
                json!({"sessionId":SESSION,"cwd":"/tmp","mcpServers":[]}),
            )
            .await
            .unwrap();
        assert!(
            reply.get("result").is_some(),
            "reconnect must adopt the original native binding: {reply}"
        );
        assert!(
            !fixture.holder.contains(SESSION),
            "reconnected frontend took the original binding"
        );
        drop(client);
        fixture.await_frontend_return().await.unwrap();
        fixture.stop().await.unwrap();
    })
    .await
    .expect("lazy Ready binding fixture did not finish");
}

#[tokio::test(flavor = "current_thread")]
async fn lazy_frontend_eof_hands_pending_native_turn_to_host() {
    tokio::time::timeout(BOUND, async {
        let (mut fixture, backend) = ListenerFixture::start().unwrap();
        let (started, native_started) = oneshot::channel();
        let (finish, release) = oneshot::channel();
        let backend = tokio::spawn(async move {
            let mut wire = open_native_thread(backend).await?;
            let request = native_request(&mut wire, "thread/read").await?;
            native_reply(&mut wire, &request, json!({"thread":{"id":SESSION,"status":{"type":"idle"},"turns":[]}})).await?;
            let request = native_request(&mut wire, "turn/start").await?;
            native_reply(&mut wire, &request, json!({"turn":{"id":"held-turn","status":"inProgress"}})).await?;
            let _receiver_gone = started.send(());
            release.await?;
            wire.send(Message::text(json!({"method":"turn/completed","params":{"threadId":SESSION,"turn":{"id":"held-turn","status":"completed"}}}).to_string())).await?;
            let request = native_request(&mut wire, "thread/read").await?;
            native_reply(&mut wire, &request, json!({"thread":{"id":SESSION,"model":"gpt-5.6-sol","reasoningEffort":"medium","createdAt":1,"updatedAt":1}})).await?;
            Ok::<_, FixtureError>(())
        });
        let mut client = fixture.open_session().await.unwrap();
        client.send(3, "session/prompt", json!({"sessionId":SESSION,"prompt":[{"type":"text","text":"hold the native turn"}],"_meta":{"codexRouter":{"effort":"medium"}}})).await.unwrap();
        native_started.await.unwrap();
        fixture.hold_catalogue(&mut client).await.unwrap();
        drop(client);
        fixture.await_frontend_return().await.unwrap();
        finish.send(()).unwrap();
        backend.await.unwrap().expect("detached native turn must read back its settled result");
        fixture.stop().await.unwrap();
    }).await.expect("lazy detached prompt fixture did not finish");
}
