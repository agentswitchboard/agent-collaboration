use super::*;
use codex_acp_adapter::{
    NativeStoredSessions, UnmaterializedBindingStore,
    test_support::{
        binding_preservation::{self, PreservationPhase, PreservationProbe},
        conversation_operation_recorder::AcceptingConversationRecorder,
        native_permission_echo::applied_router_sandbox,
    },
};
use codex_native_integration::{NativePayloadSchemas, NativeSchemaBundle};
use collaboration_protocol::OperationId;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixListener, UnixStream,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const SERVICE: &str = "00000000-0000-4000-8000-000000000001";
const SESSION: &str = "lazy-native-thread";
const MODEL: &str = "gpt-5.6-sol";

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

#[path = "acp_channel_preservation_scenario_test.rs"]
mod scenario;
use scenario::{proof_stage, run_scenario};

struct Frontend {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Frontend {
    async fn connect(path: &Path) -> Result<Self, FixtureError> {
        proof_stage("connecting frontend Unix socket");
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
        proof_stage(format!("sending ACP {method} ({id})"));
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
        self.receive(id).await
    }

    async fn receive(&mut self, id: u64) -> Result<Value, FixtureError> {
        proof_stage(format!("reading ACP response {id}"));
        self.read_reply(id).await
    }

    async fn read_reply(&mut self, id: u64) -> Result<Value, FixtureError> {
        let mut reply = String::new();
        if self.reader.read_line(&mut reply).await? == 0 {
            return Err(format!("frontend closed before response {id}").into());
        }
        let reply: Value = serde_json::from_str(&reply)?;
        if reply["id"] != id {
            return Err(format!("unexpected reply to request {id}: {reply}").into());
        }
        Ok(reply)
    }
}

async fn native_request(
    wire: &mut WebSocketStream<UnixStream>,
    method: &str,
) -> Result<Value, FixtureError> {
    proof_stage(format!("reading native {method}"));
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
    proof_stage(format!("replying to native {}", request["method"]));
    wire.send(Message::text(
        json!({"id":request["id"],"result":result}).to_string(),
    ))
    .await?;
    Ok(())
}

async fn prepare_native_thread(
    listener: UnixListener,
) -> Result<(WebSocketStream<UnixStream>, Value), FixtureError> {
    proof_stage("accepting original native Unix socket");
    let (stream, _) = listener.accept().await?;
    proof_stage("negotiating original native WebSocket");
    // Reload must reuse the original native client. A new dial fails immediately.
    drop(listener);
    let mut wire = tokio_tungstenite::accept_async(stream).await?;
    let request = native_request(&mut wire, "initialize").await?;
    native_reply(&mut wire, &request, json!({})).await?;
    native_request(&mut wire, "initialized").await?;
    let request = native_request(&mut wire, "thread/start").await?;
    Ok((wire, request))
}

async fn reply_started(
    wire: &mut WebSocketStream<UnixStream>,
    request: &Value,
) -> Result<(), FixtureError> {
    native_reply(wire, request, json!({
        "cwd":"/tmp","model":MODEL,"approvalPolicy":"on-request","approvalsReviewer":"auto_review",
        "activePermissionProfile":{"id":"router-workspace-write","extends":":workspace"},
        "sandbox":applied_router_sandbox(request),
        "thread":{"id":SESSION,"cwd":"/tmp","status":{"type":"idle"},"turns":[]}
    })).await
}

struct ListenerFixture {
    root: tempfile::TempDir,
    permit: Arc<Semaphore>,
    holder: Arc<crate::UnmaterializedThreadHolder>,
    stop: CancellationToken,
    gate: NativeGenerationGate,
    task: tokio::task::JoinHandle<io::Result<()>>,
    listener_exited: CancellationToken,
    probe: Arc<PreservationProbe>,
}

impl ListenerFixture {
    fn start(probe: &Arc<PreservationProbe>) -> Result<(Self, UnixListener), FixtureError> {
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
        let listener = AcpChannelListener::bind(
            &root.path().join("acp.sock"),
            SERVICE.to_owned().try_into()?,
            gate.clone(),
            Arc::new(NativeStoredSessions::new(
                root.path().to_owned(),
                "fixture".into(),
            )),
            Arc::clone(&holder),
            Arc::new(AcceptingConversationRecorder),
            Arc::new(crate::OpenScratchScopes::new()),
        )?
        .with_connection_budget(Arc::clone(&permit));
        let stop = CancellationToken::new();
        let listener_exited = CancellationToken::new();
        let on_exit = listener_exited.clone().drop_guard();
        let run = listener.run(stop.clone());
        let task = tokio::spawn(binding_preservation::scope(
            Some(Arc::clone(probe)),
            async move {
                let _on_exit = on_exit;
                run.await
            },
        ));
        Ok((
            Self {
                root,
                permit,
                holder,
                stop,
                gate,
                task,
                listener_exited,
                probe: Arc::clone(probe),
            },
            backend,
        ))
    }

    async fn connect(&self) -> Result<Frontend, FixtureError> {
        Frontend::connect(&self.root.path().join("acp.sock")).await
    }

    fn create_params(&self) -> Result<Value, FixtureError> {
        use std::os::unix::fs::PermissionsExt as _;
        let scratch_id = format!("session-{}", uuid::Uuid::now_v7());
        let scratch = self.root.path().join("scratch").join(&scratch_id);
        std::fs::create_dir_all(&scratch)?;
        std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700))?;
        let actor = json!({"endpoint":{"serviceId":SERVICE,"endpointId":"codex-local"},"sessionId":"creator"});
        Ok(json!({"cwd":"/tmp","mcpServers":[],"_meta":{"codexRouter":{
            "operationId":OperationId::generate(),"model":MODEL,"effort":"medium","access":"workspace-write",
            "scratchScope":scratch_id,"scratchPath":scratch,"createdBy":actor,"approver":actor
        }}}))
    }

    async fn await_frontend_return(&self) -> Result<(), FixtureError> {
        proof_stage("waiting for the frontend connection permit to return");
        tokio::select! {
            biased;
            permit = self.permit.acquire() => drop(permit?),
            () = self.listener_exited.cancelled() => return Err("listener exited before frontend permit returned".into()),
        }
        assert!(!self.stop.is_cancelled(), "the Host outlives the frontend");
        Ok(())
    }

    async fn wait_for_gate(
        &self,
        stage: &str,
        reached: &CancellationToken,
        participant_exited: &CancellationToken,
    ) -> Result<(), FixtureError> {
        proof_stage(stage);
        tokio::select! {
            biased;
            () = reached.cancelled() => Ok(()),
            () = participant_exited.cancelled() => Err(format!("participant exited before {stage}").into()),
            () = self.listener_exited.cancelled() => Err(format!("listener exited before {stage}").into()),
        }
    }

    fn begin_host_stop(&self) -> Result<(), FixtureError> {
        self.stop.cancel();
        self.gate.retire()?;
        Ok(())
    }

    async fn join_listener(&mut self) -> Result<(), FixtureError> {
        proof_stage("joining listener after Host stop");
        (&mut self.task).await??;
        Ok(())
    }

    async fn stop(mut self) -> Result<(), FixtureError> {
        self.begin_host_stop()?;
        self.join_listener().await
    }
}

impl Drop for ListenerFixture {
    fn drop(&mut self) {
        self.probe.abandoned.cancel();
        self.probe.release_outer_abort.cancel();
        self.probe.release_handoff.cancel();
        self.probe.release_update_write.cancel();
        self.stop.cancel();
        let _retired = self.gate.retire();
        if !self.task.is_finished() {
            self.task.abort();
        }
    }
}

fn load_params() -> Value {
    json!({"sessionId":SESSION,"cwd":"/tmp","mcpServers":[]})
}

async fn reconnect_original_binding(
    frontend: &mut Frontend,
    native_wire: &mut WebSocketStream<UnixStream>,
) -> Result<Value, FixtureError> {
    let reply = frontend.call(3, "session/load", load_params());
    tokio::pin!(reply);
    tokio::select! {
        response = &mut reply => response,
        activity = native_wire.next() => {
            if let Some(Ok(frame)) = activity && frame.is_text() {
                return Err(format!("held binding sent unexpected native request: {frame}").into());
            }
            reply.await
        }
    }
}

async fn assert_interrupted_load_preserves_binding(
    phase: PreservationPhase,
) -> Result<(), FixtureError> {
    let probe = PreservationProbe::new(phase);
    let (fixture, backend) = ListenerFixture::start(&probe)?;

    // Leave the Host-owned create in flight until its frontend has closed.
    // The completed create then publishes the original Ready binding.
    let mut creator = fixture.connect().await?;
    creator
        .send(2, "session/new", fixture.create_params()?)
        .await?;
    let (mut native_wire, request) = prepare_native_thread(backend).await?;
    drop(creator);
    fixture.await_frontend_return().await?;
    reply_started(&mut native_wire, &request).await?;
    let host_tasks = fixture.holder.host_tasks();
    host_tasks.close();
    proof_stage("waiting for Host create to publish its held binding");
    host_tasks.wait().await;
    host_tasks.reopen();
    assert!(
        fixture.holder.contains(SESSION),
        "Host create must publish Ready"
    );

    probe.arm();
    let mut interrupted = fixture.connect().await?;
    interrupted.send(2, "session/load", load_params()).await?;
    let setup_checkpoint = format!("waiting for held setup gate {phase:?}");
    proof_stage(&setup_checkpoint);
    tokio::select! {
        biased;
        () = probe.reached.cancelled() => {},
        reply = interrupted.read_reply(2) => return Err(format!("{setup_checkpoint}: frontend answered before the gate: {reply:?}").into()),
        () = fixture.listener_exited.cancelled() => return Err(format!("{setup_checkpoint}: listener exited").into()),
    }
    drop(interrupted);
    fixture.await_frontend_return().await?;
    fixture
        .wait_for_gate(
            "waiting for interrupted load generation disposal",
            &probe.generation_disposed,
            &fixture.listener_exited,
        )
        .await?;

    probe.disarm();
    let mut reconnected = fixture.connect().await?;
    let response = reconnect_original_binding(&mut reconnected, &mut native_wire).await;
    drop(reconnected);
    fixture.stop().await?;
    let response = response?;
    assert!(
        response.get("result").is_some(),
        "reconnect after {phase:?} must adopt the original native binding: {response}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn checked_out_binding_survives_eof_before_setup_first_poll() {
    run_scenario(assert_interrupted_load_preserves_binding(
        PreservationPhase::BeforeSetupPoll,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn checked_out_binding_survives_eof_during_directory_normalization() {
    run_scenario(assert_interrupted_load_preserves_binding(
        PreservationPhase::DuringDirectoryNormalization,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn successful_uncollected_setup_survives_eof() {
    run_scenario(assert_interrupted_load_preserves_binding(
        PreservationPhase::BeforeSetupCollection,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn lazy_generation_eof_preserves_native_prompt_terminal_observation() {
    run_scenario(assert_native_observer_survives_close(
        PreservationPhase::BeforeRegistryHandoff,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn peer_write_failure_preserves_native_prompt_terminal_observation() {
    run_scenario(assert_native_observer_survives_close(
        PreservationPhase::PeerWriteFailure,
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_peer_request_preserves_native_prompt_terminal_observation() {
    run_scenario(assert_native_observer_survives_close(
        PreservationPhase::TruncatedPeerRequest,
    ))
    .await;
}

async fn assert_native_observer_survives_close(
    phase: PreservationPhase,
) -> Result<(), FixtureError> {
    let probe = PreservationProbe::new(phase);
    probe.arm();
    let (fixture, backend) = ListenerFixture::start(&probe)?;
    let mut frontend = fixture.connect().await?;
    frontend
        .send(2, "session/new", fixture.create_params()?)
        .await?;
    let (mut native_wire, request) = prepare_native_thread(backend).await?;
    reply_started(&mut native_wire, &request).await?;
    assert_eq!(frontend.receive(2).await?["result"]["sessionId"], SESSION);
    frontend.send(3, "session/prompt", json!({"sessionId":SESSION,"prompt":[{"type":"text","text":"hold the native turn"}],"_meta":{"codexRouter":{"effort":"medium"}}})).await?;
    let request = native_request(&mut native_wire, "thread/read").await?;
    native_reply(
        &mut native_wire,
        &request,
        json!({"thread":{"id":SESSION,"status":{"type":"idle"},"turns":[]}}),
    )
    .await?;
    let request = native_request(&mut native_wire, "turn/start").await?;
    native_reply(
        &mut native_wire,
        &request,
        json!({"turn":{"id":"held-turn","status":"inProgress"}}),
    )
    .await?;

    if phase == PreservationPhase::PeerWriteFailure {
        native_wire.send(Message::text(json!({"method":"item/agentMessage/delta","params":{"threadId":SESSION,"turnId":"held-turn","itemId":"message","delta":"queued update"}}).to_string())).await?;
        fixture
            .wait_for_gate(
                "waiting for the queued update at the transport writer",
                &probe.update_waiting_to_write,
                &probe.generation_disposed,
            )
            .await?;
    }
    if phase == PreservationPhase::TruncatedPeerRequest {
        frontend.writer.write_all(b"{\"jsonrpc\":\"2.0\",").await?;
    }
    drop(frontend);
    if phase == PreservationPhase::PeerWriteFailure {
        probe.release_update_write.cancel();
        fixture
            .wait_for_gate(
                "waiting for the real BrokenPipe at the transport writer",
                &probe.peer_write_failed,
                &probe.generation_disposed,
            )
            .await?;
    }
    fixture
        .wait_for_gate(
            "waiting for the registry handoff gate",
            &probe.reached,
            &probe.generation_disposed,
        )
        .await?;
    fixture
        .wait_for_gate(
            "waiting for the lazy route close decision",
            &probe.lazy_close_issued,
            &probe.generation_disposed,
        )
        .await?;
    probe.release_handoff.cancel();
    fixture
        .wait_for_gate(
            "waiting for the active generation to dispose",
            &probe.generation_disposed,
            &fixture.listener_exited,
        )
        .await?;
    probe.release_outer_abort.cancel();
    fixture.await_frontend_return().await?;

    let terminal = async {
        proof_stage("sending the native terminal after frontend return");
        native_wire.send(Message::text(json!({"method":"turn/completed","params":{"threadId":SESSION,"turn":{"id":"held-turn","status":"completed"}}}).to_string())).await?;
        let exact_readback = native_request(&mut native_wire, "thread/read").await?;
        assert_eq!(exact_readback["params"]["threadId"], SESSION);
        native_reply(&mut native_wire, &exact_readback, json!({"thread":{"id":SESSION,"model":MODEL,"reasoningEffort":"medium","createdAt":1,"updatedAt":1}})).await
    }.await;
    fixture.stop().await?;
    assert!(
        terminal.is_ok(),
        "detached native turn must read back its settled result: {terminal:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn host_stop_counts_generation_before_registry_handoff() {
    run_scenario(assert_host_stop_counts_generation()).await;
}

async fn assert_host_stop_counts_generation() -> Result<(), FixtureError> {
    let probe = PreservationProbe::new(PreservationPhase::HostStopBeforeHandoff);
    probe.arm();
    let (mut fixture, backend) = ListenerFixture::start(&probe)?;
    let mut frontend = fixture.connect().await?;
    frontend
        .send(2, "session/new", fixture.create_params()?)
        .await?;
    let (mut native_wire, request) = prepare_native_thread(backend).await?;
    reply_started(&mut native_wire, &request).await?;
    assert_eq!(frontend.receive(2).await?["result"]["sessionId"], SESSION);

    drop(frontend);
    fixture
        .wait_for_gate(
            "waiting for the generation before Host handoff",
            &probe.reached,
            &probe.generation_disposed,
        )
        .await?;
    fixture.await_frontend_return().await?;
    fixture.begin_host_stop()?;
    fixture
        .wait_for_gate(
            "waiting for the listener to enter Host drain",
            &probe.listener_draining,
            &fixture.listener_exited,
        )
        .await?;
    let tracked_at_drain = fixture.holder.host_tasks().len();
    probe.release_handoff.cancel();
    fixture.join_listener().await?;

    assert_eq!(
        tracked_at_drain, 1,
        "Host drain must already count the exact generation behind the handoff gate"
    );
    Ok(())
}

async fn host_stop_with_detached_prompt(start_acknowledged: bool) -> Result<(), FixtureError> {
    let probe = PreservationProbe::new(PreservationPhase::HostStopAfterPromptDetach);
    probe.arm();
    let (mut fixture, backend) = ListenerFixture::start(&probe)?;
    let mut frontend = fixture.connect().await?;
    frontend
        .send(2, "session/new", fixture.create_params()?)
        .await?;
    let (mut native_wire, request) = prepare_native_thread(backend).await?;
    reply_started(&mut native_wire, &request).await?;
    assert_eq!(frontend.receive(2).await?["result"]["sessionId"], SESSION);
    frontend.send(3, "session/prompt", json!({"sessionId":SESSION,"prompt":[{"type":"text","text":"hold native work through peer EOF"}],"_meta":{"codexRouter":{"effort":"medium"}}})).await?;
    let request = native_request(&mut native_wire, "thread/read").await?;
    native_reply(
        &mut native_wire,
        &request,
        json!({"thread":{"id":SESSION,"status":{"type":"idle"},"turns":[]}}),
    )
    .await?;
    let turn_start = native_request(&mut native_wire, "turn/start").await?;
    if start_acknowledged {
        native_reply(
            &mut native_wire,
            &turn_start,
            json!({"turn":{"id":"held-turn","status":"inProgress"}}),
        )
        .await?;
        fixture
            .wait_for_gate(
                "waiting for native start acknowledgement",
                &probe.prompt_started,
                &probe.prompt_exited,
            )
            .await?;
    }

    drop(frontend);
    fixture.await_frontend_return().await?;
    fixture
        .wait_for_gate(
            "waiting for prompt actor detachment",
            &probe.prompt_detached,
            &probe.prompt_exited,
        )
        .await?;
    // Force one real prompt poll after retirement without sending the native reply.
    fixture.begin_host_stop()?;
    assert!(
        probe.check_prompt_after_retirement(),
        "the detached prompt must have suspended on native work"
    );
    fixture
        .wait_for_gate(
            "waiting for the post-retirement prompt poll",
            &probe.prompt_checked,
            &probe.prompt_exited,
        )
        .await?;
    assert!(
        probe.prompt_completed_after_retirement(),
        "Host shutdown waited for a withheld native reply, start_acknowledged={start_acknowledged}"
    );
    fixture.join_listener().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn host_stop_finishes_while_detached_native_start_ack_is_held() {
    run_scenario(host_stop_with_detached_prompt(false)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn host_stop_finishes_while_detached_native_terminal_is_held() {
    run_scenario(host_stop_with_detached_prompt(true)).await;
}

#[derive(Default)]
struct PauseAfterDeliveryCheckout {
    calls: std::sync::atomic::AtomicUsize,
    checked_out: CancellationToken,
    release: CancellationToken,
}

impl collaboration_contracts::AttemptEvidenceSink for PauseAfterDeliveryCheckout {
    fn record(
        &self,
        _: agent_automation::RouteEffectEvidence<
            collaboration_protocol::SessionRef,
            collaboration_protocol::CodexGeneration,
        >,
    ) -> collaboration_contracts::DeliveryFuture<'_, ()> {
        Box::pin(async {
            if self
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                == 1
            {
                self.checked_out.cancel();
                self.release.cancelled().await;
            }
            Ok(())
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryReturnOrder {
    BeforeSuccessor,
    DuringSuccessorResume,
}

#[tokio::test(flavor = "current_thread")]
async fn delivery_restore_cannot_make_a_retired_binding_shadow_its_successor() {
    run_scenario(Box::pin(assert_retired_delivery_does_not_shadow_successor(
        DeliveryReturnOrder::BeforeSuccessor,
    )))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn retired_busy_delivery_cannot_shadow_its_successor() {
    run_scenario(Box::pin(assert_retired_delivery_does_not_shadow_successor(
        DeliveryReturnOrder::DuringSuccessorResume,
    )))
    .await;
}

async fn assert_retired_delivery_does_not_shadow_successor(
    order: DeliveryReturnOrder,
) -> Result<(), FixtureError> {
    use collaboration_contracts::{
        DeliveryPrecondition, LoadPolicy, RoutePresence, SessionDeliveryRoute,
        layer_zero::{DeliveryRequest, PreparedPush},
    };
    use collaboration_protocol::{DeliveryOutcome, MessageDelivery, PushId, SessionRef};

    let probe = PreservationProbe::new(PreservationPhase::BeforeSetupPoll);
    let (fixture, backend) = ListenerFixture::start(&probe)?;
    let mut frontend = fixture.connect().await?;
    frontend
        .send(2, "session/new", fixture.create_params()?)
        .await?;
    let (_original_wire, _) = {
        let (mut wire, request) = prepare_native_thread(backend).await?;
        drop(frontend);
        fixture.await_frontend_return().await?;
        reply_started(&mut wire, &request).await?;
        let tracker = fixture.holder.host_tasks();
        tracker.close();
        proof_stage("waiting for Host create to publish the delivery binding");
        tracker.wait().await;
        tracker.reopen();
        assert!(fixture.holder.contains(SESSION));
        (wire, request)
    };

    let target: SessionRef = serde_json::from_value(
        json!({"endpoint":{"serviceId":SERVICE,"endpointId":"codex-local"},"sessionId":SESSION}),
    )?;
    let service_id: UuidIdentity = SERVICE.to_owned().try_into()?;
    let route = Arc::new(crate::CodexAppServerDeliveryRoute::new(
        service_id.clone(),
        crate::EndpointDirectory::new(service_id),
        crate::NativeControlBackend {
            endpoint: target.endpoint.clone(),
            gate: fixture.gate.clone(),
            codex_home: fixture.root.path().to_owned(),
        },
        Arc::clone(&fixture.holder),
    ));
    let push_id: PushId = uuid::Uuid::now_v7().to_string().try_into()?;
    let push = push_id.as_str();
    let sink = Arc::new(PauseAfterDeliveryCheckout::default());
    let mut delivery = tokio_util::task::AbortOnDropHandle::new(tokio::spawn({
        let route = Arc::clone(&route);
        let sink = Arc::clone(&sink);
        let request = DeliveryRequest {
            target: target.clone(),
            mode: MessageDelivery::Queue,
            precondition: DeliveryPrecondition::Unpinned,
            correlation: push.to_owned().try_into()?,
            attempt: agent_automation::AttemptId::generate(),
            payload: PreparedPush {
                line: format!("fixture · router://{SERVICE}/push/{push}").try_into()?,
                push_id,
                load_policy: LoadPolicy::MayLoad,
            },
        };
        async move { route.deliver(request, sink.as_ref()).await }
    }));

    // The second evidence record is inside Ready checkout. An empty endpoint
    // directory makes the actual delivery path return the known-unsent binding.
    proof_stage("waiting for delivery to check out generation A");
    tokio::select! {
        biased;
        () = sink.checked_out.cancelled() => {},
        _ = &mut delivery => return Err("delivery completed before checkout gate".into()),
        () = fixture.listener_exited.cancelled() => return Err("listener exited while awaiting delivery checkout".into()),
    }
    fixture.gate.retire()?;
    let settle_delivery = async {
        proof_stage("waiting for generation A to return its checkout");
        sink.release.cancel();
        let receipt = delivery.await??;
        assert!(matches!(
            receipt.receipt().outcome,
            DeliveryOutcome::NotSubmitted { .. }
        ));
        Ok::<(), FixtureError>(())
    };
    let mut delayed_return = Some(settle_delivery);
    if order == DeliveryReturnOrder::BeforeSuccessor
        && let Some(settle) = delayed_return.take()
    {
        settle.await?;
    }
    let successor_path = fixture.root.path().join("successor.sock");
    let successor = UnixListener::bind(&successor_path)?;
    fixture.gate.activate(
        serde_json::from_value(json!({"serviceEpoch":SERVICE,"generation":2}))?,
        successor_path,
        Some(native_schemas()?),
    )?;

    let presence = route.presence(&target).await?;
    let mut frontend = fixture.connect().await?;
    frontend.send(3, "session/load", load_params()).await?;
    let (backend, early_reply) = tokio::select! {
        reply = frontend.receive(3) => (None, Some(reply?)),
        backend = receive_successor_resume(&successor) => (Some(backend?), None),
    };
    // In the overlap case, A is released only after B has reached its native resume,
    // or after the frontend has proved that it could not get that far.
    if let Some(settle) = delayed_return {
        settle.await?;
    }
    let response = if let Some((mut wire, resume)) = backend {
        reply_resumed(&mut wire, &resume).await?;
        let response = frontend.receive(3).await?;
        if order == DeliveryReturnOrder::DuringSuccessorResume && response.get("result").is_some() {
            frontend.send(4, "session/load", load_params()).await?;
            let reply = frontend.receive(4);
            tokio::pin!(reply);
            let (used_successor, reloaded) = tokio::select! {
                reply = &mut reply => (false, reply?),
                resume = native_request(&mut wire, "thread/resume") => {
                    reply_resumed(&mut wire, &resume?).await?;
                    (true, reply.await?)
                },
            };
            assert!(
                used_successor && reloaded.get("result").is_some(),
                "B must remain usable after A returns: {reloaded}"
            );
        }
        response
    } else {
        early_reply.ok_or("frontend and backend returned no resume result")?
    };
    drop(frontend);
    if order == DeliveryReturnOrder::DuringSuccessorResume && response.get("result").is_some() {
        fixture.await_frontend_return().await?;
        let presence_after_return = route.presence(&target).await?;
        let mut fresh = fixture.connect().await?;
        fresh.send(5, "session/load", load_params()).await?;
        let fresh_reply = fresh.receive(5);
        tokio::pin!(fresh_reply);
        let (used_successor, fresh_load) = tokio::select! {
            early = &mut fresh_reply => (false, early?),
            backend = receive_successor_resume(&successor) => {
                let (mut wire, resume) = backend?;
                reply_resumed(&mut wire, &resume).await?;
                (true, fresh_reply.await?)
            },
        };
        assert!(
            presence_after_return != RoutePresence::Running
                && used_successor
                && fresh_load.get("result").is_some(),
            "late old return shadowed a fresh generation B frontend: presence={presence_after_return:?}, load={fresh_load}"
        );
    }
    fixture.stop().await?;
    assert!(
        presence != RoutePresence::Running && response.get("result").is_some(),
        "retired delivery shadowed generation B with {order:?}: presence={presence:?}, load={response}"
    );
    Ok(())
}

async fn reply_resumed(
    wire: &mut WebSocketStream<UnixStream>,
    resume: &Value,
) -> Result<(), FixtureError> {
    assert_eq!(resume["params"]["threadId"], SESSION);
    native_reply(wire, resume, json!({"cwd":"/tmp","thread":{"id":SESSION,"cwd":"/tmp","status":{"type":"idle"},"turns":[]}})).await
}

async fn receive_successor_resume(
    listener: &UnixListener,
) -> Result<(WebSocketStream<UnixStream>, Value), FixtureError> {
    proof_stage("accepting successor native Unix socket");
    let (stream, _) = listener.accept().await?;
    proof_stage("negotiating successor native WebSocket");
    let mut wire = tokio_tungstenite::accept_async(stream).await?;
    let request = native_request(&mut wire, "initialize").await?;
    native_reply(&mut wire, &request, json!({})).await?;
    native_request(&mut wire, "initialized").await?;
    let resume = native_request(&mut wire, "thread/resume").await?;
    Ok((wire, resume))
}
