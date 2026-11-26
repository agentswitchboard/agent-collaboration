//! Real MCP prompt placement while the previous terminal write is held.

use super::*;
use collaboration_mcp::test_support::ServedCollaborationApi;
use collaboration_protocol::ProviderOperationStage;

fn serve_prompt_tool(rig: &CommandPortRig, target: &SessionRef) -> ServedCollaborationApi {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(rig._root.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private MCP socket directory");
    }
    let binding = provider_binding(target);
    let endpoints = endpoint_directory(&binding)
        .inventory()
        .expect("endpoints")
        .endpoints;
    let application = collaboration_service::ApplicationFixture::new(
        &String::from(target.endpoint.service_id.clone()),
        &String::from(binding.generation.service_epoch),
    )
    .expect("application identity")
    .with_provider_conversation_backend(rig.supervisor.clone())
    .with_endpoints(endpoints)
    .expect("register fixture provider")
    .build();
    ServedCollaborationApi::start(rig._root.path(), application).expect("serve native MCP tool")
}

fn prompt_tool_arguments(target: &SessionRef, operation: &OperationId) -> Value {
    json!({
        "target": target,
        "requestedBy": target,
        "approver": target,
        "operationId": operation,
        "message": {"kind":"humanUser","text":"follow-up"},
    })
}

#[tokio::test]
async fn mcp_auto_still_steers_a_running_turn() {
    let rig = command_port_rig(AgentFixture::ScriptedSteering).await;
    start_foreground(&rig).await;
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let reply = server
        .call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        )
        .await
        .expect("MCP steer result");
    assert_eq!(
        reply["result"]["settlement"]["detail"]["kind"], "providerSteered",
        "{reply}"
    );
    assert_eq!(
        reply["result"]["operationId"].as_str(),
        Some(operation.as_str())
    );
    let wire = std::fs::read_to_string(&rig.receipt).expect("steer wire receipt");
    let mut requests = wire.lines();
    let frame: Value =
        serde_json::from_str(requests.next().expect("one ACP request")).expect("ACP request JSON");
    assert_eq!(frame["method"], "_session/steering");
    assert!(requests.next().is_none());
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
}

async fn assert_tool_has_no_dispatch(
    rig: &CommandPortRig,
    operation: &OperationId,
    stage: ProviderOperationStage,
) {
    let row = rig
        .port
        .store
        .lock()
        .await
        .inspect(operation)
        .await
        .expect("read tool operation")
        .expect("tool operation row");
    assert_eq!(row.stage, stage);
    assert_eq!(row.effect, ProviderOperationEffect::None);
    assert!(row.dispatched_at_ms.is_none());
    assert!(!rig.receipt.with_extension("successor").exists());
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn mcp_auto_waits_for_the_ended_prompt_and_dispatches_once() {
    use acp_client_runtime::handover_test_control::HandoverPausePoint;

    let rig = follow_up_rig().await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (_previous, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let blocked = rig
        .supervisor
        .observe_direct_dispatch_block_for_test(&target, operation.clone());
    let reply = {
        let call = server.call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        );
        tokio::pin!(call);
        tokio::select! {
            result = &mut call => panic!("Auto answered before the lane decided: {result:?}"),
            reached = waiting => reached.expect("Auto waits on the ended holder"),
            reached = blocked => {
                reached.expect("Auto hit the live-work refusal");
                release.send(()).expect("release terminal write after refusal decision");
                let response = call.await.expect("MCP refusal");
                panic!("Auto refused the ended operation: {response}");
            }
        }
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        let rechecked = rig
            .supervisor
            .observe_direct_dispatch_wait_for_test(&target, operation.clone());
        rig.supervisor.wake_eligibility_for_test(&target);
        tokio::select! {
            result = &mut call => panic!("Auto answered while the earlier holder remained live: {result:?}"),
            checked = rechecked => checked.expect("another eligibility check keeps waiting"),
        }
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        let operation_uuid = uuid::Uuid::parse_str(operation.as_str()).expect("operation UUID");
        let owner = rig
            .supervisor
            .session_owner_for(&target)
            .expect("session owner");
        let observed = owner
            .snapshots()
            .wait_for(|snapshot| {
                snapshot
                    .queue
                    .iter()
                    .any(|queued| queued.input.operation == operation_uuid)
            })
            .await
            .expect("the deciding owner records its input")
            .clone();
        let input = observed
            .queue
            .iter()
            .find(|queued| queued.input.operation == operation_uuid)
            .expect("same operation")
            .input
            .clone();
        let pause = provider_runtime(&rig).pause_work_handover_for_test(
            HandoverPausePoint::BeforePublication,
            input.input.clone(),
        );
        release.send(()).expect("release previous terminal write");
        let authorization = tokio::select! {
            result = &mut call => panic!("Auto answered before its provider dispatch: {result:?}"),
            reached = pause.reached => reached.expect("one authorization before provider take"),
        };
        assert_eq!(authorization.input_id, input.input);
        let marked = rig
            .port
            .store
            .lock()
            .await
            .inspect(&operation)
            .await
            .expect("read mark")
            .expect("operation row");
        assert_eq!(marked.stage, ProviderOperationStage::MayHaveDispatched);
        assert!(marked.dispatched_at_ms.is_some());
        assert!(!rig.receipt.with_extension("successor").exists());
        rig.supervisor.wake_eligibility_for_test(&target);
        pause.release.send(()).expect("release provider take");
        let (peer, _) = tokio::select! {
            result = &mut call => panic!("Auto answered before the peer saw its prompt: {result:?}"),
            reached = successor.accept() => reached.expect("successor reached the provider"),
        };
        drop(peer);
        call.await.expect("MCP completion")
    };
    assert_completed_tool_prompt(&rig, &operation, &reply).await;
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn mcp_explicit_queue_stays_queued_behind_the_ended_prompt() {
    let rig = follow_up_rig().await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (_previous, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let parked = rig
        .supervisor
        .observe_queued_dispatch_fixture(&target, &operation);
    let mut params = prompt_tool_arguments(&target, &operation);
    params["delivery"] = json!("queue");
    let reply = {
        let call = server.call_structured("conversation_prompt", params);
        tokio::pin!(call);
        tokio::select! {
            reached = parked => reached.expect("queued input parked for the live blocker"),
            result = &mut call => panic!("queued call answered before its dispatch: {result:?}"),
        }
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        let owner = rig
            .supervisor
            .session_owner_for(&target)
            .expect("session owner");
        let operation_uuid = uuid::Uuid::parse_str(operation.as_str()).expect("operation UUID");
        owner
            .snapshots()
            .wait_for(|snapshot| {
                snapshot
                    .queue
                    .iter()
                    .any(|queued| queued.input.operation == operation_uuid && !queued.started)
            })
            .await
            .expect("same operation is waiting in the owner queue");
        release.send(()).expect("release previous terminal write");
        let (peer, _) = tokio::select! {
            result = &mut call => panic!("queued call answered before the peer saw its prompt: {result:?}"),
            reached = successor.accept() => reached.expect("queued input reached the provider"),
        };
        drop(peer);
        call.await.expect("queued MCP completion")
    };
    assert_completed_tool_prompt(&rig, &operation, &reply).await;
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
}

async fn assert_completed_tool_prompt(
    rig: &CommandPortRig,
    operation: &OperationId,
    reply: &Value,
) {
    assert_eq!(reply["result"]["kind"], "completed", "{reply}");
    assert_eq!(
        reply["result"]["operationId"].as_str(),
        Some(operation.as_str())
    );
    let row = rig
        .port
        .store
        .lock()
        .await
        .inspect(operation)
        .await
        .expect("read tool operation")
        .expect("operation row");
    assert_eq!(row.stage, ProviderOperationStage::Terminal);
    assert_eq!(row.effect, ProviderOperationEffect::Applied);
    assert!(row.dispatched_at_ms.is_some());
    let wire =
        std::fs::read_to_string(rig.receipt.with_extension("successor")).expect("wire receipt");
    assert_eq!(wire.lines().count(), 1);
}

async fn wait_for_tool_input(
    rig: &CommandPortRig,
    target: &SessionRef,
    operation: &OperationId,
) -> OwnerInput {
    let operation = uuid::Uuid::parse_str(operation.as_str()).expect("operation UUID");
    let owner = rig
        .supervisor
        .session_owner_for(target)
        .expect("session owner");
    let observed = owner
        .snapshots()
        .wait_for(|snapshot| {
            snapshot
                .queue
                .iter()
                .any(|queued| queued.input.operation == operation)
        })
        .await
        .expect("the deciding owner records its input")
        .clone();
    observed
        .queue
        .into_iter()
        .find(|queued| queued.input.operation == operation)
        .expect("same operation")
        .input
}

async fn wait_for_tool_input_to_leave(
    rig: &CommandPortRig,
    target: &SessionRef,
    input: &OwnerInput,
) {
    let owner = rig
        .supervisor
        .session_owner_for(target)
        .expect("session owner");
    owner
        .snapshots()
        .wait_for(|snapshot| !snapshot.queue.iter().any(|queued| queued.input == *input))
        .await
        .expect("owner removed the unsent input");
}

#[tokio::test]
async fn mcp_auto_ended_prompt_wait_uses_the_existing_dispatch_expiry() {
    let before = observe_dispatch_expiry(true).await;
    let after = observe_dispatch_expiry(false).await;
    assert_eq!(before, after);
    eprintln!("MCP dispatch expiry with writer still held: {after:?}");
}

async fn observe_dispatch_expiry(legacy_dispatch_intent: bool) -> (Value, Value, Value, Value) {
    let rig = follow_up_rig().await;
    let (previous, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let pending = legacy_dispatch_intent.then(|| {
        rig.supervisor
            .hold_pending_dispatch_for_test(&target, previous)
    });
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let reply = {
        let call = server.call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        );
        tokio::pin!(call);
        tokio::select! {
            reached = waiting => reached.expect("follow-up is held in the dispatch-offer wait"),
            reply = &mut call => panic!("MCP answered before dispatch deadline: {reply:?}"),
        }
        let input = wait_for_tool_input(&rig, &target, &operation).await;
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        rig.clock
            .advance(crate::session_store_lane::STORE_WAIT)
            .expect("reach the dispatch cutoff");
        wait_for_tool_input_to_leave(&rig, &target, &input).await;
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        // The terminal reply has its own existing storage budget behind the held writer.
        rig.clock
            .advance(crate::session_store_lane::STORE_WAIT)
            .expect("reach the terminal reply cutoff");
        call.await
            .expect("MCP expiry with earlier writer still held")
    };
    let error = &reply["error"];
    assert_eq!(error["kind"], "unavailable", "{reply}");
    assert_eq!(error["stage"], "dispatch", "{reply}");
    assert_eq!(error["effect"], "none", "{reply}");
    assert_eq!(error["operationId"].as_str(), Some(operation.as_str()));
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("provider input was not submitted")),
        "{reply}"
    );
    assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
    drop(pending);
    release
        .send(())
        .expect("release previous terminal write after the reply");
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
    assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Terminal).await;
    (
        error["kind"].clone(),
        error["stage"].clone(),
        error["effect"].clone(),
        error["message"].clone(),
    )
}

#[tokio::test]
async fn mcp_auto_rechecks_unknown_outcomes_before_waiting_on_the_ended_prompt() {
    let rig = follow_up_rig().await;
    let (previous, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let reply = {
        let call = server.call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        );
        tokio::pin!(call);
        tokio::select! {
            reached = waiting => reached.expect("follow-up awaits the ended operation"),
            reply = &mut call => panic!("MCP answered before the eligibility wake: {reply:?}"),
        }
        let input = wait_for_tool_input(&rig, &target, &operation).await;
        rig.supervisor
            .record_unknown_for_test(&previous, &provider_binding(&target), Some(target.clone()))
            .await;
        rig.supervisor.wake_eligibility_for_test(&target);
        wait_for_tool_input_to_leave(&rig, &target, &input).await;
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        release
            .send(())
            .expect("release terminal write after refusal decision");
        call.await.expect("MCP refusal")
    };
    let error = &reply["error"];
    assert_eq!(error["kind"], "unavailable", "{reply}");
    assert_eq!(error["stage"], "dispatch", "{reply}");
    assert_eq!(error["effect"], "none", "{reply}");
    assert!(
        error["message"].as_str().is_some_and(|message| {
            message.contains("outcome is unknown") && message.contains(previous.as_str())
        }),
        "{reply}"
    );
    assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Terminal).await;
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
}

#[path = "provider_session_command_port_wait_recheck_test.rs"]
mod wait_recheck;
