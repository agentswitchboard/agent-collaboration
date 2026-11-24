use super::{CollaborationRuntime, CollaborationRuntimeInputs, RouterExecutableRelation};
use crate::{ExternalProviderLaunchBinding, ExternalProviderStartup};
use collaboration_contracts::ProviderConversationBackend as _;
use collaboration_protocol::{
    ConversationPromptRequest, EndpointId, EndpointRef, MessageContent, MessageDelivery,
    MessageText, OperationId, ProviderIdentity, SessionId, SessionRef,
};
use futures_util::{SinkExt as _, StreamExt as _};
use router_clock::{ManualClock, RuntimeClock as _};
use serde_json::{Value, json};
use std::{os::unix::fs::PermissionsExt as _, path::Path, sync::Arc, time::Duration};
use tokio::net::UnixStream;
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

type NativeSocket = WebSocketStream<UnixStream>;
type FixtureResult<T> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Wire-order assertions are separate from fixture setup and transport errors."
)]
async fn native_start_waits_for_the_previous_terminal_writer() -> FixtureResult<()> {
    let HeldNativeTurn {
        _root,
        runtime,
        mut native,
        thread,
        first_turn,
        release_write,
        start_held,
        ..
    } = hold_native_turn().await?;
    send_next_start(&mut native, &thread).await?;

    let early_frame = tokio::select! {
        biased;
        frame = unexpected_while_held(&mut native, &first_turn) => Some(frame?),
        held = start_held => {
            held?;
            None
        }
    };
    let _ = release_write.send(());
    assert!(
        early_frame.is_none(),
        "the terminal writer was still held: {early_frame:?}"
    );

    let mut previous_end = false;
    let mut admitted = false;
    while !previous_end || !admitted {
        let frame = receive(&mut native).await?;
        if frame["method"] == "turn/completed" && turn_id(&frame)? == first_turn {
            previous_end = true;
        }
        if frame["method"] == "turn/started" {
            assert!(
                previous_end,
                "the next turn started before the previous turn ended"
            );
        }
        if frame["id"] == 2 {
            assert!(
                frame.pointer("/result/turn/id").is_some(),
                "next start was refused: {frame}"
            );
            admitted = true;
        }
    }
    native.close(None).await?;
    runtime.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn native_terminal_reaches_its_deadline_with_the_writer_still_held() -> FixtureResult<()> {
    assert_deadline_fence(hold_native_turn().await?, false).await
}

#[tokio::test]
async fn the_start_gets_the_owner_fence_with_the_actual_store_mutex_held() -> FixtureResult<()> {
    assert_deadline_fence(hold_native_turn().await?, true).await
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "Recovery assertions are distinct from fixture transport errors."
)]
async fn a_dead_settling_owner_fails_its_waiting_caller_and_uses_normal_recovery()
-> FixtureResult<()> {
    use provider_session_machines::{
        session_lifecycle_machine::{CloseAttempt, SessionLifecycle},
        session_owner::SessionOwnerCommand,
        session_turn_machine::SessionTurn,
    };

    let HeldNativeTurn {
        _root,
        runtime,
        mut native,
        clock,
        thread,
        target,
        first_turn,
        release_write,
        start_held,
        ..
    } = hold_native_turn().await?;
    let supervisor = runtime
        .external_provider_supervisor
        .as_deref()
        .ok_or("fixture supervisor")?;
    let owner = supervisor.session_owner_for(&target)?;
    let before = owner.snapshots().borrow().clone();
    assert!(matches!(before.turn, SessionTurn::Settling(_)));
    let SessionLifecycle::Live { generation } = before.lifecycle else {
        return Err("fixture owner must be live".into());
    };
    let provider = supervisor
        .runtime_for(&target.endpoint)
        .ok_or("fixture provider")?;
    let (replacement_started, finish_recovery) = provider.preparation_fixture.pause_recovery();
    send_next_start(&mut native, &thread).await?;
    start_held.await?;

    owner
        .admit(SessionOwnerCommand::Close {
            generation,
            attempt: CloseAttempt(uuid::Uuid::now_v7()),
        })
        .await?;
    let capacity = owner.hold_completion_capacity();
    assert!(!capacity.is_empty());
    let budget = Duration::from_secs(10);
    clock
        .not_due_check_now(clock.monotonic_now().after(budget))
        .await;
    clock.advance(budget)?;

    let response = unexpected_while_held(&mut native, &first_turn).await?;
    assert_eq!(
        response["id"], 2,
        "the dead incarnation published a terminal: {response}"
    );
    assert!(
        response.get("error").is_some(),
        "the held caller unexpectedly succeeded"
    );
    drop(capacity);
    let _ = release_write.send(());
    replacement_started.await?;
    let replacement = supervisor
        .session_owner_for(&target)?
        .snapshots()
        .borrow()
        .clone();
    assert_ne!(replacement.owner, before.owner);
    assert!(matches!(
        replacement.lifecycle,
        SessionLifecycle::NotLoaded { .. }
    ));
    let _ = finish_recovery.send(());
    drop(owner);
    drop(provider);
    native.close(None).await?;
    runtime.shutdown().await?;
    Ok(())
}

struct HeldNativeTurn {
    _root: tempfile::TempDir,
    runtime: CollaborationRuntime,
    native: NativeSocket,
    clock: Arc<ManualClock>,
    thread: String,
    target: SessionRef,
    first_turn: String,
    first_operation: OperationId,
    release_write: tokio::sync::oneshot::Sender<()>,
    start_held: tokio::sync::oneshot::Receiver<()>,
}

async fn hold_native_turn() -> FixtureResult<HeldNativeTurn> {
    let root = tempfile::tempdir()?;
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))?;
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let runtime = start_fixture(root.path(), clock.clone()).await?;
    let socket = root.path().join("router-sessions/cursor-local.sock");
    let transport = UnixStream::connect(&socket)
        .await
        .map_err(|error| format!("connect to fixture socket {}: {error}", socket.display()))?;
    let (mut native, _) = client_async("ws://localhost/rpc", transport).await?;
    send(&mut native, 1, "thread/start", json!({"cwd": root.path()})).await?;
    let thread = loop {
        let frame = receive(&mut native).await?;
        if frame["id"] == 1 {
            break frame
                .pointer("/result/thread/id")
                .and_then(Value::as_str)
                .ok_or_else(|| format!("missing native thread ID: {frame}"))?
                .to_owned();
        }
    };

    let supervisor = runtime
        .external_provider_supervisor
        .as_deref()
        .ok_or("fixture supervisor")?;
    let target = SessionRef {
        endpoint: EndpointRef {
            service_id: runtime.service_id.clone(),
            endpoint_id: EndpointId::try_from("cursor-local".to_owned())?,
        },
        session_id: SessionId::try_from("session-1".to_owned())?,
    };
    let actor: ProviderIdentity = serde_json::from_value(json!({"humanId":"fixture-owner"}))?;
    let first = OperationId::generate();
    let (write_held, release_write) = supervisor.pause_terminal_write_of(&target, &first);
    let start_held = supervisor.observe_prompt_start_held(&target);
    supervisor
        .prompt(ConversationPromptRequest {
            operation_id: first.clone(),
            input_id: None,
            target: target.clone(),
            generation: None,
            requested_by: actor.clone(),
            approver: actor,
            delivery: MessageDelivery::Auto,
            prompt: MessageContent::Router {
                text: MessageText::try_from("first prompt".to_owned())?,
            },
        })
        .await
        .map_err(|refused| format!("first prompt was refused: {refused:?}"))?;
    let first_turn = loop {
        let frame = receive(&mut native).await?;
        if frame["method"] == "turn/started" {
            break turn_id(&frame)?.to_owned();
        }
    };
    write_held.await?;
    Ok(HeldNativeTurn {
        _root: root,
        runtime,
        native,
        clock,
        thread,
        target,
        first_turn,
        first_operation: first,
        release_write,
        start_held,
    })
}

async fn assert_deadline_fence(held: HeldNativeTurn, hold_store: bool) -> FixtureResult<()> {
    let HeldNativeTurn {
        _root,
        runtime,
        mut native,
        clock,
        thread,
        first_turn,
        first_operation,
        release_write,
        ..
    } = held;
    let mut release_write = Some(release_write);
    let store = if hold_store {
        let store = runtime
            .provider_store
            .as_ref()
            .ok_or("fixture provider store")?
            .lock()
            .await;
        if let Some(release) = release_write.take() {
            let _ = release.send(());
        }
        Some(store)
    } else {
        None
    };
    let budget = Duration::from_secs(10);
    clock
        .not_due_check_now(clock.monotonic_now().after(budget))
        .await;
    clock.advance(budget)?;
    loop {
        let frame = receive(&mut native).await?;
        if frame["method"] == "turn/completed" && turn_id(&frame)? == first_turn {
            if frame.pointer("/params/turn/status").and_then(Value::as_str) != Some("completed") {
                return Err(
                    format!("deadline lost the witnessed provider outcome: {frame}").into(),
                );
            }
            break;
        }
    }
    send_next_start(&mut native, &thread).await?;
    loop {
        let frame = receive(&mut native).await?;
        if frame["id"] == 2 {
            if frame.pointer("/error/data/kind").and_then(Value::as_str)
                != Some("previous_turn_settlement_unresolved")
                || frame
                    .pointer("/error/data/operationId")
                    .and_then(Value::as_str)
                    != Some(first_operation.as_str())
            {
                return Err(format!("expected the exact owner fence: {frame}").into());
            }
            break;
        }
    }
    drop(store);
    if let Some(release) = release_write {
        let _ = release.send(());
    }
    native.close(None).await?;
    runtime.shutdown().await?;
    Ok(())
}

async fn start_fixture(
    root: &Path,
    clock: Arc<ManualClock>,
) -> FixtureResult<CollaborationRuntime> {
    let peers = root.join("peers");
    std::fs::create_dir(&peers)?;
    let launch = ExternalProviderLaunchBinding::cursor(
        node_test_fixture::node_executable()?,
        vec![
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../acp-client-runtime/tests/support/behavior-agent.ts"
            )
            .to_owned(),
            root.join("wire.jsonl").display().to_string(),
            "manual".to_owned(),
            "cursor_login".to_owned(),
        ],
    )?;
    Ok(Box::pin(
        CollaborationRuntime::start_with_optional_router_proxy_endpoint(
            CollaborationRuntimeInputs {
                owner_human_id: Some(message_board::HumanId::try_from(
                    "fixture-owner".to_owned(),
                )?),
                directory: root.to_owned(),
                codex_home: root.to_owned(),
                backend_socket: root.join("backend.sock"),
                mcp_bind: "127.0.0.1:0".parse()?,
                native_schema: None,
                peer_registry_directory: Some(peers),
                remote_control_server_name: None,
            },
            vec![ExternalProviderStartup::Launch(launch)],
            Arc::new(collaboration_mcp::FixedRouterExecutableRelation(
                RouterExecutableRelation::Match,
            )),
            None,
            None,
            clock,
            None,
        ),
    )
    .await?)
}

async fn send_next_start(socket: &mut NativeSocket, thread: &str) -> FixtureResult<()> {
    send(
        socket,
        2,
        "turn/start",
        json!({"threadId": thread, "input": [{"type": "text", "text": "next prompt"}]}),
    )
    .await
}

async fn send(
    socket: &mut NativeSocket,
    id: u64,
    method: &str,
    params: Value,
) -> FixtureResult<()> {
    socket
        .send(Message::text(
            json!({"id":id,"method":method,"params":params}).to_string(),
        ))
        .await?;
    Ok(())
}

async fn receive(socket: &mut NativeSocket) -> FixtureResult<Value> {
    let frame = socket.next().await.ok_or("native connection closed")??;
    Ok(serde_json::from_str(frame.to_text()?)?)
}

fn turn_id(frame: &Value) -> FixtureResult<&str> {
    Ok(frame
        .pointer("/params/turn/id")
        .and_then(Value::as_str)
        .ok_or("missing notification turn ID")?)
}

async fn unexpected_while_held(
    native: &mut NativeSocket,
    first_turn: &str,
) -> FixtureResult<Value> {
    loop {
        let frame = receive(native).await?;
        if frame["id"] == 2
            || (frame["method"] == "turn/completed" && turn_id(&frame)? == first_turn)
        {
            return Ok(frame);
        }
    }
}
