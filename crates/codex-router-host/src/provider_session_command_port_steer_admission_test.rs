//! Exercise the real exact-turn admission boundary after the caller sampled A.

use super::*;
use crate::external_provider_supervisor::ExactTurnSteer;
use collaboration_protocol::ConversationOperationFailureKind;
use provider_session_machines::{
    SessionGeneration, session_lifecycle_machine::SessionLifecycle, session_turn_machine::TurnId,
};
use std::future::Future;

fn live_generation(rig: &CommandPortRig) -> SessionGeneration {
    let snapshot = rig
        .supervisor
        .owner_session_snapshot(&protocol_target("fixture-session"))
        .expect("owner snapshot");
    let SessionLifecycle::Live { generation } = snapshot.lifecycle else {
        panic!("expected a live provider run: {:?}", snapshot.lifecycle);
    };
    generation
}

async fn change_turn_after_sample(
    rig: &CommandPortRig,
    gate: &UnixListener,
    change: AdmissionTurnChange,
    original: SessionGeneration,
) -> Option<(String, UnixListener)> {
    release_foreground(rig, gate).await;
    match change {
        AdmissionTurnChange::Idle => {
            assert_eq!(live_generation(rig), original);
            None
        }
        AdmissionTurnChange::Successor => {
            let successor_end = UnixListener::bind(rig.receipt.with_extension("successor.sock"))
                .expect("successor gate");
            let successor = start_foreground(rig).await.turn_id;
            assert_eq!(live_generation(rig), original);
            Some((successor, successor_end))
        }
        AdmissionTurnChange::ReplacedRun => {
            rig.port
                .close(SessionTargetCommand {
                    target: rig.board_target.clone(),
                    actor: rig.actor.clone(),
                })
                .await
                .expect("close the sampled generation");
            let owner = rig
                .supervisor
                .session_owner_for(&protocol_target("fixture-session"))
                .expect("session owner");
            owner
                .restore_for_delivery(uuid::Uuid::now_v7())
                .await
                .expect("real recovery establishes live G2");
            assert_ne!(live_generation(rig), original);
            None
        }
    }
}

async fn fenced_reply(
    rig: &CommandPortRig,
    operation: OperationId,
    release: tokio::sync::oneshot::Sender<()>,
    reply: impl Future<Output = Value>,
) -> (Value, ConversationOperationFailureKind) {
    let target = protocol_target("fixture-session");
    let (marked, _release_mark) = rig
        .supervisor
        .pause_steer_mark_receipt_for_test(&target, operation.clone());
    let (authorized, _release_take) = provider_runtime(rig)
        .preparation_fixture
        .pause_named_steer_take();
    release
        .send(())
        .expect("let the original sampled input reach owner admission");
    let reply = tokio::select! {
        biased;
        mark = marked => panic!("a refused exact steer reached the durable mark: {mark:?}"),
        offer = authorized => panic!("a refused exact steer reached the ACP offer: {offer:?}"),
        answer = reply => answer,
    };
    let failure = rig
        .supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: operation.clone(),
            timeout_seconds: PositiveSeconds::try_from(2).expect("fixture wait budget"),
        })
        .await
        .expect_err("the exact input has a public refusal");
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    let operation = rig
        .port
        .store
        .lock()
        .await
        .inspect(&operation)
        .await
        .expect("read operation")
        .expect("the sampled input reached owner admission");
    assert_eq!(operation.dispatched_at_ms, None);
    (reply, failure.kind)
}

fn failure_for(change: AdmissionTurnChange) -> ConversationOperationFailureKind {
    match change {
        AdmissionTurnChange::Idle => ConversationOperationFailureKind::NoActiveTurn,
        AdmissionTurnChange::Successor => ConversationOperationFailureKind::Unavailable,
        AdmissionTurnChange::ReplacedRun => ConversationOperationFailureKind::StaleGeneration,
    }
}

async fn check_peer_receipt(rig: &CommandPortRig, successor: Option<(String, UnixListener)>) {
    if let Some((_, gate)) = successor {
        release_foreground(rig, &gate).await;
        let capture = std::fs::read_to_string(rig.receipt.with_extension("successor"))
            .expect("successor wire receipt");
        assert_eq!(
            capture.lines().count(),
            1,
            "only the separately started B reached the wire"
        );
    }
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_peer_had_only_foreground(rig);
}

async fn connect_native(
    rig: &CommandPortRig,
) -> (WebSocketStream<UnixStream>, JoinHandle<()>, String) {
    let path = rig.receipt.with_extension("native.sock");
    let listener = UnixListener::bind(&path).expect("native listener");
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
        UnixStream::connect(path).await.expect("native connection"),
    )
    .await
    .expect("native websocket");
    native_send(&mut client, 1, "thread/list", json!({})).await;
    let (reply, _) = native_reply(&mut client, 1).await;
    let thread = reply
        .pointer("/result/data/0/id")
        .and_then(Value::as_str)
        .expect("native thread ID")
        .to_owned();
    (client, server, thread)
}

#[tokio::test]
async fn literal_native_steer_admission_uses_the_sampled_run_and_the_deciding_turn() {
    for change in [
        AdmissionTurnChange::Idle,
        AdmissionTurnChange::Successor,
        AdmissionTurnChange::ReplacedRun,
    ] {
        let rig = command_port_rig(AgentFixture::LiteralSteering(
            PeerSteerBehavior::AdmissionFence(change),
        ))
        .await;
        let gate = UnixListener::bind(rig.receipt.with_extension("foreground.sock"))
            .expect("foreground gate");
        let foreground = start_foreground(&rig).await;
        let generation = live_generation(&rig);
        let (mut client, server, thread) = connect_native(&rig).await;
        let (sampled, release) = provider_runtime(&rig)
            .preparation_fixture
            .pause_exact_steer_admission();
        native_send(
            &mut client,
            2,
            "turn/steer",
            json!({"threadId":thread,"expectedTurnId":foreground.turn_id,
            "input":[{"type":"text","text":"named steer"}]}),
        )
        .await;
        let (operation, placement) = sampled.await.expect("exact sampled operation");
        assert_eq!(
            placement,
            ExactTurnSteer::Named {
                generation,
                turn: TurnId(
                    uuid::Uuid::parse_str(&foreground.turn_id).expect("sampled turn UUID")
                ),
            }
        );
        let successor = change_turn_after_sample(&rig, &gate, change, generation).await;
        let (reply, failure_kind) = fenced_reply(&rig, operation, release, async {
            native_reply(&mut client, 2).await.0
        })
        .await;
        let message = match change {
            AdmissionTurnChange::Idle => "no active turn to steer".into(),
            AdmissionTurnChange::Successor => format!(
                "expected active turn id `{}` but found `{}`",
                foreground.turn_id,
                successor.as_ref().expect("active B").0
            ),
            AdmissionTurnChange::ReplacedRun => "provider session is busy".into(),
        };
        client.close(None).await.expect("close native client");
        server.await.expect("join native connection");
        check_peer_receipt(&rig, successor).await;
        assert_eq!(
            reply.get("error"),
            Some(&json!({"code":-32600,"message":message})),
            "{change:?}"
        );
        assert_eq!(failure_kind, failure_for(change));
    }
}

#[tokio::test]
async fn literal_acp_steer_admission_uses_the_sampled_run_and_the_deciding_turn() {
    for change in [
        AdmissionTurnChange::Idle,
        AdmissionTurnChange::Successor,
        AdmissionTurnChange::ReplacedRun,
    ] {
        let rig = command_port_rig(AgentFixture::LiteralSteering(
            PeerSteerBehavior::AdmissionFence(change),
        ))
        .await;
        let gate = UnixListener::bind(rig.receipt.with_extension("foreground.sock"))
            .expect("foreground gate");
        let foreground = start_foreground(&rig).await;
        let generation = live_generation(&rig);
        let mut caller = AcpCaller::start(&rig);
        let (sampled, release) = provider_runtime(&rig)
            .preparation_fixture
            .pause_exact_steer_admission();
        caller.steer(1).await;
        let (operation, placement) = sampled.await.expect("exact sampled operation");
        assert_eq!(
            placement,
            ExactTurnSteer::Named {
                generation,
                turn: TurnId(
                    uuid::Uuid::parse_str(&foreground.turn_id).expect("sampled turn UUID")
                ),
            }
        );
        let successor = change_turn_after_sample(&rig, &gate, change, generation).await;
        let (reply, failure_kind) = fenced_reply(&rig, operation, release, caller.reply(1)).await;
        caller.close().await;
        check_peer_receipt(&rig, successor).await;
        match change {
            AdmissionTurnChange::Idle => assert_eq!(
                reply.get("result"),
                Some(&json!({"outcome":"promptRequired"}))
            ),
            AdmissionTurnChange::Successor => assert_eq!(
                reply.get("result"),
                Some(
                    &json!({"outcome":"failed","reason":"the turn you steered had already ended; send again"})
                )
            ),
            AdmissionTurnChange::ReplacedRun => assert_eq!(
                reply.get("error"),
                Some(&json!({
                    "code":-32000, "message":"the session was restored again; send again if still needed", "data":{"kind":"staleGeneration"},
                }))
            ),
        }
        assert_eq!(failure_kind, failure_for(change));
    }
}

#[tokio::test]
async fn acp_busy_prompt_rereads_after_exact_steer_admission_and_prompts_once() {
    let rig = command_port_rig(AgentFixture::LiteralSteering(
        PeerSteerBehavior::AutoAdmissionFallback,
    ))
    .await;
    let gate =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    rig.port
        .close(SessionTargetCommand {
            target: rig.board_target.clone(),
            actor: rig.actor.clone(),
        })
        .await
        .expect("retire the seed so ACP can load and attach its observer");
    let mut caller = AcpCaller::start(&rig);
    caller
        .input
        .send(
            json!({"jsonrpc":"2.0","id":1,"method":"session/load","params":{
                "sessionId":"fixture-session","cwd":rig._root.path(),"mcpServers":[]
            }}),
        )
        .await
        .expect("load through the ACP caller");
    let attached = caller.reply(1).await;
    assert!(
        attached.get("result").is_some(),
        "failed to attach ACP prompt observer: {attached}"
    );
    let foreground = start_foreground(&rig).await;
    let (sampled, release) = provider_runtime(&rig)
        .preparation_fixture
        .pause_exact_steer_admission();
    caller
        .input
        .send(
            json!({"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{
                "sessionId":"fixture-session","prompt":[{"type":"text","text":"follow-up"}]
            }}),
        )
        .await
        .expect("send busy prompt");
    let (operation, placement) = tokio::select! {
        sample = sampled => sample.expect("busy prompt chose an exact steer"),
        early = caller.reply(2) => panic!("busy prompt answered before its exact turn sample: {early}"),
    };
    assert_eq!(
        placement,
        ExactTurnSteer::Auto {
            turn: TurnId(uuid::Uuid::parse_str(&foreground.turn_id).expect("sampled turn UUID")),
        }
    );
    release_foreground(&rig, &gate).await;
    release
        .send(())
        .expect("allow automatic steer admission against idle");
    let reply = caller.reply(2).await;
    let failed_steer = rig
        .supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: operation,
            timeout_seconds: PositiveSeconds::try_from(2).expect("fixture wait budget"),
        })
        .await
        .expect_err("Auto's old exact placement missed A");
    let classification = CommandFailure::ProviderSteerOperation(Box::new(failed_steer))
        .classification()
        .into_owned();
    caller.close().await;
    rig.supervisor.shutdown().await.expect("fixture shutdown");

    assert!(matches!(classification, CommandFailure::Busy));
    assert_eq!(
        reply.pointer("/result/stopReason"),
        Some(&json!("end_turn"))
    );
    assert_peer_had_only_foreground(&rig);
    let prompts = std::fs::read_to_string(rig.receipt.with_extension("successor"))
        .expect("peer fallback prompt");
    assert_eq!(
        prompts.lines().count(),
        1,
        "the busy-prompt helper uses one prompt"
    );
}
