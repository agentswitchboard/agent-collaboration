#![expect(
    clippy::unwrap_used,
    reason = "Wire fixtures fail at the first missing response or receipt."
)]

use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use acp_client_runtime::{
    AgentSessionClient, ExternalProviderRuntimeError, ManagedAcpToolSetup, SessionWorkRoots,
    acp_transport::TransportRun, owner_session_open::OwnerSessionOpenSetup,
};
use provider_session_machines::{
    SessionGeneration,
    session_lifecycle_machine::{LoadAttempt, RestoreVia},
    session_owner::OwnerIncarnation,
};
use router_clock::ManualClock;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
    sync::mpsc,
};
use tracing_subscriber::layer::SubscriberExt as _;

use super::{
    create_settings_order::{NoopEventSink, NoopInteractionPort},
    test_behavior::TestProviderBehavior,
};

const PROVIDER_SESSION: &str = "019f0000-0000-7000-8000-000000000001";
const TEST_URL: &str = "http://127.0.0.1:12345/mcp/managed/019f0000-0000-7000-8000-000000000002";
const OWNER: OwnerIncarnation = OwnerIncarnation(uuid::Uuid::from_u128(
    0x019f_0000_0000_7000_8000_0000_0000_0003,
));

struct ProviderFixture {
    client: AgentSessionClient<NoopInteractionPort>,
    setup_frames: mpsc::UnboundedReceiver<Value>,
}

impl ProviderFixture {
    async fn start(http_mcp: bool, before_reply: impl Fn() + Send + 'static) -> Self {
        let (client, agent) = tokio::io::duplex(16_384);
        let (frames, setup_frames) = mpsc::unbounded_channel();
        let agent = tokio::spawn(async move {
            let (reader, mut writer) = tokio::io::split(agent);
            let mut messages = BufReader::new(reader).lines();
            while let Some(line) = messages.next_line().await.unwrap() {
                let frame: Value = serde_json::from_str(&line).unwrap();
                let result = match frame.get("method").and_then(Value::as_str) {
                    Some("initialize") => json!({"protocolVersion":1,"agentCapabilities":{
                        "loadSession":true,"sessionCapabilities":{"resume":{}},"mcpCapabilities":{"http":http_mcp,"sse":false}
                    }}),
                    Some("session/new" | "session/load" | "session/resume") => {
                        frames.send(frame.clone()).unwrap();
                        before_reply();
                        json!({"sessionId":PROVIDER_SESSION})
                    }
                    _ => continue,
                };
                let reply = json!({"jsonrpc":"2.0","id":frame.get("id").unwrap(),"result":result});
                let write = writer.write_all(format!("{reply}\n").as_bytes()).await;
                if write.is_err() {
                    break;
                }
            }
        });
        let clock = Arc::new(ManualClock::new("2030-03-17T17:46:40Z".parse().unwrap()));
        let client = AgentSessionClient::initialize_in_process(
            client,
            agent,
            Arc::new(NoopInteractionPort),
            Arc::new(NoopEventSink),
            Arc::new(TestProviderBehavior),
            clock,
        )
        .await
        .unwrap();
        Self {
            client,
            setup_frames,
        }
    }
}

fn session_setup() -> OwnerSessionOpenSetup {
    OwnerSessionOpenSetup {
        cwd: std::env::current_dir().unwrap(),
        scratch_folder: None,
        work_roots: SessionWorkRoots::default(),
    }
}

fn fixture_bearer(canary: u8) -> String {
    format!("test-only-managed-credential-{canary}")
}

fn claude_tools(canary: u8) -> ManagedAcpToolSetup {
    ManagedAcpToolSetup::claude(TEST_URL.to_owned(), &fixture_bearer(canary))
}

fn assert_managed_server(frame: &Value, method: &str, canary: u8) {
    assert_eq!(frame.get("method").and_then(Value::as_str), Some(method));
    let definitions = frame
        .pointer("/params/mcpServers")
        .and_then(Value::as_array)
        .unwrap();
    assert_eq!(definitions.len(), 1);
    let server = definitions.first().unwrap();
    assert_eq!(
        server.get("name").and_then(Value::as_str),
        Some("agent-router")
    );
    assert_eq!(server.get("type").and_then(Value::as_str), Some("http"));
    assert_eq!(server.get("url").and_then(Value::as_str), Some(TEST_URL));
    let expected = format!("Bearer {}", fixture_bearer(canary));
    assert_eq!(
        server.pointer("/headers/0/name").and_then(Value::as_str),
        Some("Authorization")
    );
    assert!(
        server.pointer("/headers/0/value").and_then(Value::as_str) == Some(expected.as_str()),
        "adapter received the wrong descriptor"
    );
}

#[tokio::test]
async fn claude_new_load_and_resume_forward_strict_setup_fails_on_connection_wide_mcp() {
    let mut provider = ProviderFixture::start(true, || {}).await;
    let wire = provider.client.owner_wire();
    let incarnation = OWNER;
    let opened = wire
        .create_managed(incarnation, session_setup(), claude_tools(1))
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(opened.provider_id(), PROVIDER_SESSION);
    drop(opened);
    let new = provider.setup_frames.recv().await.unwrap();
    assert_managed_server(&new, "session/new", 1);
    assert_eq!(
        new["params"]["_meta"]["claudeCode"]["options"]["strictMcpConfig"],
        true
    );
    for (generation, (via, method, canary)) in [
        (RestoreVia::Load, "session/load", 2),
        (RestoreVia::Resume, "session/resume", 3),
    ]
    .into_iter()
    .enumerate()
    {
        let run = TransportRun {
            incarnation,
            generation: SessionGeneration::from_number(generation as u64 + 1),
        };
        wire.open_route(run, PROVIDER_SESSION.to_owned());
        let restored = wire
            .restore_managed(
                run,
                LoadAttempt(uuid::Uuid::from_u128(
                    0x019f_0000_0000_7000_8000_0000_0000_0010 + generation as u128,
                )),
                PROVIDER_SESSION.to_owned(),
                via,
                session_setup(),
                claude_tools(canary),
            )
            .unwrap()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored.provider_id(), PROVIDER_SESSION);
        let restore = provider.setup_frames.recv().await.unwrap();
        assert_managed_server(&restore, method, canary);
        assert_eq!(restore["params"]["sessionId"], PROVIDER_SESSION);
        assert_eq!(
            restore["params"]["_meta"]["claudeCode"]["options"]["strictMcpConfig"],
            true
        );
    }
    provider.client.shutdown().await;
}

#[tokio::test]
async fn cursor_requires_fresh_setup_fails_on_fallback_or_cached_evidence() {
    for new_handshake in [false, true] {
        let handshakes = Arc::new(AtomicU64::new(2));
        let witness = handshakes.clone();
        let mut provider = ProviderFixture::start(true, move || {
            if new_handshake {
                witness.fetch_add(1, Ordering::Release);
            }
        })
        .await;
        let observe = move || handshakes.load(Ordering::Acquire);
        let wire = provider.client.owner_wire();
        let incarnation = OWNER;
        let result = wire
            .create_managed(
                incarnation,
                session_setup(),
                ManagedAcpToolSetup::cursor(
                    TEST_URL.to_owned(),
                    &fixture_bearer(4),
                    observe.clone(),
                ),
            )
            .unwrap()
            .await
            .unwrap();
        assert_cursor_open(result, new_handshake);
        let new = provider.setup_frames.recv().await.unwrap();
        assert_managed_server(&new, "session/new", 4);
        assert!(new["params"]["_meta"]["claudeCode"].is_null());
        let run = TransportRun {
            incarnation,
            generation: SessionGeneration::from_number(1),
        };
        wire.open_route(run, PROVIDER_SESSION.to_owned());
        let result = wire
            .restore_managed(
                run,
                LoadAttempt(uuid::Uuid::from_u128(
                    0x019f_0000_0000_7000_8000_0000_0000_0012,
                )),
                PROVIDER_SESSION.to_owned(),
                RestoreVia::Load,
                session_setup(),
                ManagedAcpToolSetup::cursor(TEST_URL.to_owned(), &fixture_bearer(5), observe),
            )
            .unwrap()
            .await
            .unwrap();
        assert_cursor_open(result, new_handshake);
        let load = provider.setup_frames.recv().await.unwrap();
        assert_managed_server(&load, "session/load", 5);
        provider.client.shutdown().await;
    }
}

fn assert_cursor_open(
    result: Result<
        acp_client_runtime::owner_session_open::OwnerOpenedSession,
        ExternalProviderRuntimeError,
    >,
    expected: bool,
) {
    match result {
        Ok(opened) => assert!(expected && opened.provider_id() == PROVIDER_SESSION),
        Err(ExternalProviderRuntimeError::ManagedToolsNotConnected {
            provider_session_id,
        }) => {
            assert!(!expected && provider_session_id == PROVIDER_SESSION);
        }
        Err(other) => assert!(
            matches!(
                other,
                ExternalProviderRuntimeError::ManagedToolsNotConnected { .. }
            ),
            "unexpected failure: {other}"
        ),
    }
}

#[tokio::test]
async fn managed_open_requires_advertised_http_mcp_before_dispatch() {
    let mut provider = ProviderFixture::start(false, || {}).await;
    let result = provider
        .client
        .owner_wire()
        .create_managed(OWNER, session_setup(), claude_tools(6))
        .unwrap()
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(ExternalProviderRuntimeError::UnsupportedCapability {
            capability: "HTTP MCP for managed sessions"
        })
    ));
    assert!(provider.setup_frames.try_recv().is_err());
    provider.client.shutdown().await;
}

#[derive(Clone, Default)]
struct TraceCapture(Arc<Mutex<Vec<u8>>>);

impl io::Write for TraceCapture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("trace capture poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn acp_bearer_never_reaches_target_specific_trace_fails_without_connection_filter() {
    let captured = TraceCapture::default();
    let writer = captured.clone();
    let filter = tracing_subscriber::EnvFilter::new(
        "acp_client_runtime=debug,agent_client_protocol::jsonrpc::outgoing_actor=trace,agent_client_protocol::jsonrpc::incoming_actor=trace,agent_client_protocol::jsonrpc::transport_actor=trace",
    );
    let trace = tracing_subscriber::registry().with(filter).with(
        tracing_subscriber::fmt::layer()
            .without_time()
            .with_writer(move || writer.clone()),
    );
    let _subscriber = tracing::subscriber::set_default(trace);
    let mut provider = ProviderFixture::start(true, || {}).await;
    let wire = provider.client.owner_wire();
    let incarnation = OWNER;
    let opened = wire
        .create_managed(incarnation, session_setup(), claude_tools(7))
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    drop(opened);
    assert_managed_server(
        &provider.setup_frames.recv().await.unwrap(),
        "session/new",
        7,
    );
    provider.client.shutdown().await;
    let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(
        output.contains("outgoingFlushed"),
        "Router transport diagnostics were disabled"
    );
    assert!(
        !output.contains(&fixture_bearer(7)),
        "ACP SDK leaked the managed bearer"
    );
    assert!(!output.contains(TEST_URL), "ACP SDK leaked the managed URL");
}
