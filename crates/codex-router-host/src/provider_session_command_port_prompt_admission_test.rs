//! Follow-up prompts across the terminal-write custody boundary.

use super::*;
use crate::external_provider_supervisor::PromptBusyPolicy;
use collaboration_protocol::ConversationOperationFailureStage;
use std::time::Duration;
use tokio::sync::oneshot;

async fn follow_up_rig() -> CommandPortRig {
    command_port_rig(AgentFixture::LiteralSteering(
        PeerSteerBehavior::AdmissionFence(AdmissionTurnChange::Successor),
    ))
    .await
}

fn hold_terminal(
    rig: &CommandPortRig,
) -> (OperationId, oneshot::Receiver<()>, oneshot::Sender<()>) {
    let target = protocol_target("fixture-session");
    let snapshot = rig
        .supervisor
        .owner_session_snapshot(&target)
        .expect("session owner");
    let foreground = snapshot.foreground.expect("running foreground");
    let operation = OperationId::try_from(foreground.operation.to_string()).expect("operation ID");
    let (reached, release) = rig.supervisor.pause_terminal_write_of(&target, &operation);
    (operation, reached, release)
}

async fn assert_ended_with_live_custody(rig: &CommandPortRig, operation: &OperationId) {
    wait_for_turn_end(rig).await;
    let target = protocol_target("fixture-session");
    let snapshot = rig
        .supervisor
        .owner_session_snapshot(&target)
        .expect("session owner");
    assert_eq!(snapshot.turn, SessionTurn::Idle);
    assert!(snapshot.foreground.is_none());
    let last = snapshot.last_live_turn.expect("last completed turn");
    assert_eq!(last.operation.to_string(), operation.as_str());
    rig.supervisor
        .assert_only_prompt_blocks_admission_for_test(&target, operation);
}

async fn begin_held_terminal(rig: &CommandPortRig) -> (OperationId, oneshot::Sender<()>) {
    let gate =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    start_foreground(rig).await;
    let (operation, reached, release) = hold_terminal(rig);
    release_foreground(rig, &gate).await;
    reached.await.expect("terminal write held");
    assert_ended_with_live_custody(rig, &operation).await;
    (operation, release)
}

fn direct_prompt(
    rig: &CommandPortRig,
    operation_id: OperationId,
) -> crate::ProviderPromptContentsRequest {
    let target = protocol_session(&rig.board_target).expect("protocol target");
    crate::ProviderPromptContentsRequest {
        operation_id,
        input_id: InputId::generate(),
        requested_by: target.clone().into(),
        approver: target.clone().into(),
        target,
        input: acp_client_runtime::ProviderTurnInput::Prompt(ProviderSubmission::plain(vec![
            PromptContent::text("follow-up".into()).expect("fixture text"),
        ])),
        busy: PromptBusyPolicy::RefuseBusy,
    }
}

async fn wait_for_native_terminal(client: &mut WebSocketStream<UnixStream>, turn: &str) {
    loop {
        let frame = client
            .next()
            .await
            .expect("native terminal frame")
            .expect("native transport");
        if let Message::Text(text) = frame {
            let event: Value = serde_json::from_str(&text).expect("native event JSON");
            if event["method"] == "turn/completed"
                && event.pointer("/params/turn/id").and_then(Value::as_str) == Some(turn)
            {
                assert_eq!(event["params"]["turn"]["status"], "completed");
                return;
            }
        }
    }
}

#[tokio::test]
async fn native_follow_up_waits_for_the_ended_prompt_to_release_custody() {
    let rig = follow_up_rig().await;
    let gate =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (mut client, server, thread) = connect_native(&rig).await;
    let params = |text: &str| json!({"threadId":thread,"input":[{"type":"text","text":text}]});
    native_send(&mut client, 2, "thread/read", json!({"threadId":thread})).await;
    let (attached, _) = native_reply(&mut client, 2).await;
    assert!(attached.get("result").is_some(), "{attached}");
    native_send(&mut client, 3, "turn/start", params("foreground")).await;
    let (started, _) = native_reply(&mut client, 3).await;
    let previous_turn = started
        .pointer("/result/turn/id")
        .and_then(Value::as_str)
        .expect("previous turn");
    let (operation, reached, release) = hold_terminal(&rig);
    release_foreground(&rig, &gate).await;
    reached.await.expect("previous terminal write held");
    wait_for_native_terminal(&mut client, previous_turn).await;
    assert_ended_with_live_custody(&rig, &operation).await;

    let waiting = rig
        .supervisor
        .observe_ended_prompt_admission_for_test(&protocol_target("fixture-session"), operation);
    native_send(&mut client, 4, "turn/start", params("successor")).await;
    let (started, _) = {
        let answer = native_reply(&mut client, 4);
        tokio::pin!(answer);
        tokio::select! {
            result = &mut answer => panic!("follow-up answered before custody release: {result:?}"),
            reached = waiting => reached.expect("the exact earlier holder blocked the reservation"),
        }
        assert!(!rig.receipt.with_extension("successor").exists());
        release.send(()).expect("release previous terminal write");
        answer.await
    };
    let next_turn = started
        .pointer("/result/turn/id")
        .and_then(Value::as_str)
        .expect("successor turn");
    assert_ne!(next_turn, previous_turn);
    let (peer, _) = successor
        .accept()
        .await
        .expect("successor reached the provider");
    drop(peer);
    assert_eq!(
        std::fs::read_to_string(rig.receipt.with_extension("successor"))
            .expect("wire")
            .lines()
            .count(),
        1
    );
    client.close(None).await.expect("close native client");
    server.await.expect("join native server");
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn direct_follow_up_waits_for_the_ended_prompt_to_release_custody() {
    let rig = follow_up_rig().await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (previous, release) = begin_held_terminal(&rig).await;
    let request = direct_prompt(&rig, OperationId::generate());
    let waiting = rig
        .supervisor
        .observe_ended_prompt_admission_for_test(&request.target, previous);
    let operation = request.operation_id.clone();
    let mut answer = rig.supervisor.prompt_contents(request);
    tokio::select! {
        result = &mut answer => panic!("follow-up answered before custody release: {result:?}"),
        reached = waiting => reached.expect("follow-up awaits eligibility for the previous write"),
    }
    assert!(
        rig.port
            .store
            .lock()
            .await
            .inspect(&operation)
            .await
            .expect("read operation")
            .is_none()
    );
    assert!(!rig.receipt.with_extension("successor").exists());
    release.send(()).expect("release previous terminal write");
    let admitted = answer.await.expect("follow-up is admitted after release");
    assert_eq!(admitted.operation.operation_id, operation);
    let (peer, _) = successor
        .accept()
        .await
        .expect("successor reached the provider");
    drop(peer);
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn direct_follow_up_expires_at_admission_deadline_without_sending() {
    let rig = follow_up_rig().await;
    let (previous, release) = begin_held_terminal(&rig).await;
    let request = direct_prompt(&rig, OperationId::generate());
    let waiting = rig
        .supervisor
        .observe_ended_prompt_admission_for_test(&request.target, previous);
    let operation = request.operation_id.clone();
    let mut answer = rig.supervisor.prompt_contents(request);
    tokio::select! {
        result = &mut answer => panic!("follow-up answered before the admission deadline: {result:?}"),
        reached = waiting => reached.expect("follow-up awaits the ended operation"),
    }
    rig.clock
        .advance(Duration::from_secs(10))
        .expect("reach the original admission deadline");
    let refusal = answer
        .await
        .expect_err("the held write outlasts the admission deadline");
    assert_eq!(refusal.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refusal.stage, ConversationOperationFailureStage::Admission);
    assert_eq!(refusal.effect, ProviderOperationEffect::None);
    assert_eq!(
        String::from(refusal.message),
        "the Router was busy and nothing was sent, so send again as a new operation"
    );
    assert!(
        rig.port
            .store
            .lock()
            .await
            .inspect(&operation)
            .await
            .expect("read operation")
            .is_none()
    );
    assert!(!rig.receipt.with_extension("successor").exists());
    release.send(()).expect("release previous terminal write");
    rig.supervisor.shutdown().await.expect("drain");
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_newer_prompt_blocks_while_dispatching_and_while_running() {
    use acp_client_runtime::handover_test_control::HandoverPausePoint;

    let rig = follow_up_rig().await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (previous, release) = begin_held_terminal(&rig).await;
    release
        .send(())
        .expect("release the completed A's terminal write");
    rig.supervisor.drain_fixture_operation_tasks().await;
    let second = OperationId::generate();
    let request = direct_prompt(&rig, second.clone());
    let target = request.target.clone();
    let pause = provider_runtime(&rig).pause_work_handover_for_test(
        HandoverPausePoint::BeforePublication,
        request.input_id.clone(),
    );
    let second_result = rig.supervisor.prompt_contents(request);
    pause
        .reached
        .await
        .expect("B reached its ACP take with its mark published");
    let snapshot = rig
        .supervisor
        .owner_session_snapshot(&target)
        .expect("idle owner before B take");
    assert_eq!(snapshot.turn, SessionTurn::Idle);
    assert_eq!(
        snapshot
            .last_live_turn
            .expect("ended A")
            .operation
            .to_string(),
        previous.as_str()
    );
    rig.supervisor
        .assert_only_prompt_blocks_admission_for_test(&target, &second);

    let third = OperationId::generate();
    let refused = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, third.clone()))
        .await
        .expect_err("a newer marked operation still blocks at once");
    assert_eq!(refused.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refused.stage, ConversationOperationFailureStage::Validation);
    assert_eq!(refused.effect, ProviderOperationEffect::None);
    assert!(String::from(refused.message).contains(second.as_str()));
    assert!(
        rig.port
            .store
            .lock()
            .await
            .inspect(&third)
            .await
            .expect("read operation")
            .is_none()
    );
    assert!(!rig.receipt.with_extension("successor").exists());
    pause.release.send(()).expect("release B take");
    second_result.await.expect("B is admitted");
    let (peer, _) = successor.accept().await.expect("B reached the provider");
    let owner = rig.supervisor.session_owner_for(&target).expect("owner");
    owner
        .snapshots()
        .wait_for(|snapshot| matches!(snapshot.turn, SessionTurn::Running { .. }))
        .await
        .expect("B running");
    let refused = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, OperationId::generate()))
        .await
        .expect_err("a running turn still refuses a direct prompt");
    assert_eq!(refused.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refused.stage, ConversationOperationFailureStage::Admission);
    assert_eq!(refused.effect, ProviderOperationEffect::None);
    drop(peer);
    rig.supervisor.shutdown().await.expect("drain");
}

#[path = "provider_session_command_port_prompt_tool_test.rs"]
mod prompt_tool;
