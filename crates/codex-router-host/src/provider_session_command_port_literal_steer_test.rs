//! Literal native and ACP callers cross the real Host owner and ACP writer.

use super::*;
use crate::external_provider_runtime::acp_scripted_fixture::AcpFixtureScript;
use codex_acp_adapter::{
    AcpConnectionContext, AcpOutputSender, AcpRouterChannels, AcpSessionRoute as _, QueuedAcpFrame,
    bounded_acp_output,
};
use collaboration_runtime::{
    ProviderAcpSessionRoute, RouterSessionAppServerContext,
    serve_router_session_app_server_connection,
};
use futures_util::{SinkExt as _, StreamExt as _};
use provider_session_machines::session_turn_machine::SessionTurn;
use serde_json::{Value, json};
use std::path::Path;
use tokio::{
    net::{UnixListener, UnixStream},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;

#[path = "provider_session_command_port_steer_admission_test.rs"]
mod admission;

#[derive(Clone, Copy, Debug)]
pub(super) enum AdmissionTurnChange {
    Idle,
    Successor,
    ReplacedRun,
    ReplacedOwnerIdle,
    ReplacedOwnerRunning,
}

#[derive(Clone, Copy)]
pub(super) enum PeerSteerBehavior {
    TurnEndsBeforeTake,
    StartedNewTurn,
    Unsupported,
    AdmissionFence(AdmissionTurnChange),
    AutoAdmissionFallback,
}

pub(super) fn launch_peer(receipt: &Path, behavior: PeerSteerBehavior) -> ExternalProviderLaunch {
    let script = AcpFixtureScript::new()
        .expect_request("init", "initialize", json!({}))
        .respond("init", json!({
            "protocolVersion": 1, "agentCapabilities": {"loadSession": true},
            "agentInfo": {"name":"literal-steer-fixture", "version":"1"},
            "_meta": {"steering": {"supported": !matches!(behavior, PeerSteerBehavior::Unsupported)}}
        }))
        .expect_request("new", "session/new", json!({}))
        .respond("new", json!({"sessionId":"fixture-session"}));
    let script = if matches!(behavior, PeerSteerBehavior::AutoAdmissionFallback) {
        script
            .expect_request(
                "attach",
                "session/load",
                json!({"sessionId":"fixture-session"}),
            )
            .respond("attach", json!({}))
    } else {
        script
    };
    let script = script.expect_recorded_request(
        "foreground",
        "session/prompt",
        json!({}),
        &receipt.with_extension("prompt"),
    );
    let script = match behavior {
        PeerSteerBehavior::TurnEndsBeforeTake
        | PeerSteerBehavior::Unsupported
        | PeerSteerBehavior::AdmissionFence(_)
        | PeerSteerBehavior::AutoAdmissionFallback => {
            script.wait_for_socket_signal(&receipt.with_extension("foreground.sock"))
        }
        PeerSteerBehavior::StartedNewTurn => script
            .expect_recorded_request(
                "steer",
                "_session/steering",
                json!({}),
                &receipt.with_extension("steer"),
            )
            .respond("steer", json!({"outcome":"startedNewTurn"})),
    };
    let script = script.respond("foreground", json!({"stopReason":"end_turn"}));
    let script = match behavior {
        PeerSteerBehavior::AdmissionFence(
            AdmissionTurnChange::ReplacedRun
            | AdmissionTurnChange::ReplacedOwnerIdle
            | AdmissionTurnChange::ReplacedOwnerRunning,
        ) => script
            .expect_request(
                "reload",
                "session/load",
                json!({"sessionId":"fixture-session"}),
            )
            .respond("reload", json!({})),
        _ => script,
    };
    let script = match behavior {
        PeerSteerBehavior::AdmissionFence(
            AdmissionTurnChange::Successor | AdmissionTurnChange::ReplacedOwnerRunning,
        ) => script
            .expect_recorded_request(
                "successor",
                "session/prompt",
                json!({}),
                &receipt.with_extension("successor"),
            )
            .wait_for_socket_signal(&receipt.with_extension("successor.sock"))
            .respond("successor", json!({"stopReason":"end_turn"})),
        PeerSteerBehavior::AutoAdmissionFallback => script
            .expect_recorded_request(
                "fallback",
                "session/prompt",
                json!({}),
                &receipt.with_extension("successor"),
            )
            .respond("fallback", json!({"stopReason":"end_turn"})),
        _ => script,
    };
    script
        .record_diagnostics(receipt.with_extension("unexpected.jsonl"))
        .launch()
}

async fn start_foreground(rig: &CommandPortRig) -> SessionTurnHandle {
    rig.port
        .prompt(PromptSessionCommand {
            target: rig.board_target.clone(),
            input_id: InputId::generate(),
            actor: rig.actor.clone(),
            submission: ProviderSubmission::plain(vec![
                PromptContent::text("foreground".into()).expect("text"),
            ]),
        })
        .await
        .expect("real foreground prompt took the provider wire")
}

fn provider_runtime(rig: &CommandPortRig) -> Arc<ExternalProviderRuntime> {
    crate::provider_test_support::fixture_runtime_for(
        &rig.supervisor,
        &protocol_target("fixture-session").endpoint,
    )
    .expect("fixture runtime")
}

async fn release_foreground(rig: &CommandPortRig, listener: &UnixListener) {
    let (peer, _) = listener
        .accept()
        .await
        .expect("peer awaits foreground completion signal");
    drop(peer);
    let owner = rig
        .supervisor
        .session_owner_for(&protocol_target("fixture-session"))
        .expect("real session owner");
    owner
        .snapshots()
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .expect("foreground ended before the offered steer was taken");
}

async fn native_send(
    client: &mut WebSocketStream<UnixStream>,
    id: u64,
    method: &str,
    params: Value,
) {
    client
        .send(Message::Text(
            json!({"id":id,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await
        .expect("native request");
}

async fn native_reply(client: &mut WebSocketStream<UnixStream>, id: u64) -> (Value, String) {
    loop {
        let frame = client
            .next()
            .await
            .expect("native reply frame")
            .expect("native transport");
        if let Message::Text(text) = frame {
            let reply: Value = serde_json::from_str(&text).expect("native JSON");
            if reply.get("id") == Some(&json!(id)) {
                return (reply, text.to_string());
            }
        }
    }
}

struct AcpCaller {
    input: AcpOutputSender,
    replies: mpsc::Receiver<QueuedAcpFrame>,
    closed: CancellationToken,
    route: JoinHandle<std::io::Result<()>>,
}

impl AcpCaller {
    fn start(rig: &CommandPortRig) -> Self {
        let closed = CancellationToken::new();
        let (input, requests) = bounded_acp_output(closed.clone());
        let (output, replies) = bounded_acp_output(closed.clone());
        let route = Box::new(ProviderAcpSessionRoute::new(
            rig.board_target.endpoint.clone(),
            rig.port.clone(),
            rig.hub.clone(),
            rig.port.clock.clone(),
        ));
        let route = tokio::spawn(route.run(
            AcpRouterChannels {
                input: requests,
                output,
                closed: closed.clone(),
            },
            AcpConnectionContext {
                actor: Some(rig.actor.clone()),
                client_profile: None,
                client_supports_elicitation_form: false,
                frontend_close: Default::default(),
            },
        ));
        Self {
            input,
            replies,
            closed,
            route,
        }
    }

    async fn steer(&self, id: u64) {
        self.input
            .send(
                json!({"jsonrpc":"2.0","id":id,"method":"_session/steering","params":{
                    "sessionId":"fixture-session","prompt":[{"type":"text","text":"follow-up"}]
                }}),
            )
            .await
            .expect("ACP request");
    }

    async fn reply(&mut self, id: u64) -> Value {
        loop {
            let frame = self.replies.recv().await.expect("ACP response");
            if frame.get("id") == Some(&json!(id)) && frame.get("method").is_none() {
                return (*frame).clone();
            }
        }
    }

    async fn close(self) {
        self.closed.cancel();
        self.route
            .await
            .expect("join ACP route")
            .expect("close ACP route");
    }
}

fn assert_peer_had_only_foreground(rig: &CommandPortRig) {
    let received = std::fs::read_to_string(rig.receipt.with_extension("prompt"))
        .expect("peer foreground capture");
    assert_eq!(received.lines().count(), 1);
    assert!(!rig.receipt.with_extension("steer").exists());
    assert!(
        !rig.receipt.with_extension("unexpected.jsonl").exists(),
        "peer received unexpected work"
    );
}

#[tokio::test]
async fn literal_native_steer_reports_idle_after_the_real_take_fence() {
    let rig = command_port_rig(AgentFixture::LiteralSteering(
        PeerSteerBehavior::TurnEndsBeforeTake,
    ))
    .await;
    let foreground_gate =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    let started = start_foreground(&rig).await;
    let native_socket = rig.receipt.with_extension("native.sock");
    let listener = UnixListener::bind(&native_socket).expect("native listener");
    let context = Arc::new(RouterSessionAppServerContext::new(
        rig.board_target.endpoint.clone(),
        rig.actor.clone(),
        rig.port.clone(),
        rig.hub.clone(),
        Arc::new(
            crate::provider_model_catalog_adapter::SupervisorModelCatalog::new(
                rig.supervisor.clone(),
            ),
        ),
    ));
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept native caller");
        serve_router_session_app_server_connection(stream, context)
            .await
            .expect("serve native caller");
    });
    let (mut client, _) = client_async(
        "ws://localhost/rpc",
        UnixStream::connect(native_socket)
            .await
            .expect("native connection"),
    )
    .await
    .expect("native websocket");
    native_send(&mut client, 1, "thread/list", json!({})).await;
    let (listed, _) = native_reply(&mut client, 1).await;
    let thread = listed
        .pointer("/result/data/0/id")
        .and_then(Value::as_str)
        .expect("native thread ID");
    let command = |expected: &str| {
        json!({"threadId":thread,"expectedTurnId":expected,
        "input":[{"type":"text","text":"follow-up"}]})
    };
    native_send(&mut client, 2, "turn/steer", command("different-turn")).await;
    let (mismatch, _) = native_reply(&mut client, 2).await;
    let (offered, take) = provider_runtime(&rig)
        .preparation_fixture
        .pause_named_steer_take();
    native_send(&mut client, 3, "turn/steer", command(&started.turn_id)).await;
    offered
        .await
        .expect("real marked steer present in ACP transport");

    release_foreground(&rig, &foreground_gate).await;
    take.send(()).expect("allow the stale transport take");
    let (ended, _) = native_reply(&mut client, 3).await;
    native_send(
        &mut client,
        4,
        "turn/interrupt",
        json!({"threadId":thread,"turnId":started.turn_id}),
    )
    .await;
    let (_, idle_interrupt) = native_reply(&mut client, 4).await;
    client.close(None).await.expect("close native client");
    server.await.expect("join native connection");
    rig.supervisor.shutdown().await.expect("fixture shutdown");

    assert_eq!(
        mismatch.get("error"),
        Some(&json!({"code": -32600,"message":format!(
            "expected active turn id `different-turn` but found `{}`", started.turn_id
        )}))
    );
    assert_eq!(
        ended.get("error"),
        Some(&json!({"code": -32600,"message":"no active turn to steer"}))
    );
    assert_eq!(
        idle_interrupt,
        json!({"id":4,"error":{"code":-32000,"message":"provider session unavailable"}})
            .to_string()
    );
    assert_peer_had_only_foreground(&rig);
}

#[tokio::test]
async fn literal_acp_steer_reports_prompt_required_after_the_real_take_fence() {
    let rig = command_port_rig(AgentFixture::LiteralSteering(
        PeerSteerBehavior::TurnEndsBeforeTake,
    ))
    .await;
    let foreground_gate =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    let mut caller = AcpCaller::start(&rig);
    caller.steer(1).await;
    let idle = caller.reply(1).await;
    start_foreground(&rig).await;
    let (offered, take) = provider_runtime(&rig)
        .preparation_fixture
        .pause_named_steer_take();
    caller.steer(2).await;
    offered
        .await
        .expect("real marked steer present in ACP transport");

    release_foreground(&rig, &foreground_gate).await;
    take.send(()).expect("allow the stale transport take");
    let ended = caller.reply(2).await;
    caller.close().await;
    rig.supervisor.shutdown().await.expect("fixture shutdown");

    assert_eq!(
        idle.get("error"),
        Some(&json!({"code": -32000,"message":"No running turn to steer"}))
    );
    assert_eq!(
        ended.get("result"),
        Some(&json!({"outcome":"promptRequired"}))
    );
    assert_peer_had_only_foreground(&rig);
}

#[tokio::test]
async fn literal_acp_steer_reports_that_the_provider_started_its_own_turn() {
    let rig = command_port_rig(AgentFixture::LiteralSteering(
        PeerSteerBehavior::StartedNewTurn,
    ))
    .await;
    let started = start_foreground(&rig).await;
    let mut caller = AcpCaller::start(&rig);

    caller.steer(1).await;
    let reply = caller.reply(1).await;
    caller.close().await;
    rig.supervisor.shutdown().await.expect("fixture shutdown");

    assert_eq!(
        reply.pointer("/result/outcome"),
        Some(&json!("startedNewTurn"))
    );
    let turn = reply
        .pointer("/result/turnId")
        .and_then(Value::as_str)
        .expect("provider-started turn ID");
    assert_ne!(turn, started.turn_id);
    let received =
        std::fs::read_to_string(rig.receipt.with_extension("steer")).expect("the peer saw a steer");
    assert_eq!(received.lines().count(), 1);
    assert!(
        !rig.receipt.with_extension("unexpected.jsonl").exists(),
        "peer received a Router replacement"
    );
}

#[tokio::test]
async fn literal_acp_steer_preserves_cannot_steer_while_the_agent_is_running() {
    let rig = command_port_rig(AgentFixture::LiteralSteering(
        PeerSteerBehavior::Unsupported,
    ))
    .await;
    let foreground_gate =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    start_foreground(&rig).await;
    let mut caller = AcpCaller::start(&rig);

    caller.steer(1).await;
    let reply = caller.reply(1).await;
    release_foreground(&rig, &foreground_gate).await;
    caller.close().await;
    rig.supervisor.shutdown().await.expect("fixture shutdown");

    assert_eq!(reply.pointer("/error/code"), Some(&json!(-32601)));
    assert_eq!(
        reply.pointer("/error/data/kind"),
        Some(&json!("cannotSteer"))
    );
    assert_peer_had_only_foreground(&rig);
}
