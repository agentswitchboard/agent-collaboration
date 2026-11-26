//! Review controls for turn completion, lagging watch, and competing callers.

use super::*;
use crate::binding_store_fixture::BindingFixtureWait;
use session_event_model::{StopReason, TurnOutcome};

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn native_follow_up_sampled_running_rechecks_the_idle_owner_at_the_lane() {
    let rig = follow_up_rig().await;
    let target = protocol_target("fixture-session");
    let foreground =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("foreground gate");
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (mut client, server, thread) = connect_native(&rig).await;
    let params = |text: &str| json!({"threadId":thread,"input":[{"type":"text","text":text}]});
    native_send(&mut client, 2, "thread/read", json!({"threadId":thread})).await;
    assert!(native_reply(&mut client, 2).await.0.get("result").is_some());
    native_send(&mut client, 3, "turn/start", params("foreground")).await;
    let (started, _) = native_reply(&mut client, 3).await;
    let first_turn = started
        .pointer("/result/turn/id")
        .and_then(Value::as_str)
        .expect("first turn");
    let (prior, terminal_held, release_terminal) = hold_terminal(&rig);
    let (published, release_watch) = provider_runtime(&rig)
        .preparation_fixture
        .pause_terminal_publication();
    let (peer, _) = foreground.accept().await.expect("running peer");
    drop(peer);
    published
        .await
        .expect("completion offered before the watch updates");
    wait_for_native_terminal(&mut client, first_turn).await;
    let stale = rig
        .supervisor
        .owner_session_snapshot(&target)
        .expect("stale owner watch");
    let prior_uuid = uuid::Uuid::parse_str(prior.as_str()).expect("previous operation UUID");
    assert_eq!(
        stale.foreground.as_ref().map(|input| input.operation),
        Some(prior_uuid)
    );
    assert!(matches!(stale.turn, SessionTurn::Running { .. }));

    let before = rig.supervisor.live_operation_ids();
    let stamped = rig.supervisor.observe_turn_start_fixture(
        &target,
        BindingFixtureWait::RefuseWhileRunningAdmission(prior_uuid),
    );
    native_send(&mut client, 4, "turn/start", params("successor")).await;
    stamped
        .await
        .expect("the real native follow-up reserved as RefuseWhileRunning(A)");
    let current = rig.supervisor.live_operation_ids();
    let [operation] = current
        .difference(&before)
        .cloned()
        .collect::<Vec<_>>()
        .try_into()
        .expect("one newly admitted native operation");
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let blocked = rig
        .supervisor
        .observe_direct_dispatch_block_for_test(&target, operation.clone());
    let marked = rig.supervisor.observe_turn_start_fixture(
        &target,
        BindingFixtureWait::DirectDispatchAccepted(operation.clone()),
    );
    release_watch
        .send(())
        .expect("let the idle owner take its next command");
    terminal_held.await.expect("hold previous terminal custody");
    let answer = {
        let answer = native_reply(&mut client, 4);
        tokio::pin!(answer);
        tokio::select! {
            reached = waiting => reached.expect("admitted on idle, waits at the lane"),
            reached = blocked => {
                reached.expect("the lane decided a refusal");
                release_terminal.send(()).expect("release terminal writer after refusal");
                let failure = rig.supervisor.wait(ConversationOperationWaitRequest {
                    operation_id: operation.clone(),
                    timeout_seconds: PositiveSeconds::try_from(5).expect("fixture wait"),
                }).await.expect_err("retained lane failure");
                let row = rig.port.store.lock().await.inspect(&operation).await.expect("operation row read").expect("admitted row");
                panic!("stale-sampled native request was refused: {:?}/{:?}/{:?}, dispatched_at={:?}", failure.kind, failure.stage, failure.effect, row.dispatched_at_ms);
            }
            result = &mut answer => panic!("native followed up before custody release: {result:?}"),
        }
        let idle = rig
            .supervisor
            .owner_session_snapshot(&target)
            .expect("now idle owner");
        assert_eq!(
            idle.live_idle_ended_turn().map(|turn| turn.operation),
            Some(prior_uuid)
        );
        let row = rig
            .port
            .store
            .lock()
            .await
            .inspect(&operation)
            .await
            .expect("operation row read")
            .expect("admitted row");
        assert!(row.dispatched_at_ms.is_none());
        assert!(!rig.receipt.with_extension("successor").exists());
        release_terminal
            .send(())
            .expect("release previous terminal custody");
        marked
            .await
            .expect("the same operation obtained the mark ticket");
        answer.await.0
    };
    let next_turn = answer
        .pointer("/result/turn/id")
        .and_then(Value::as_str)
        .expect("follow-up starts");
    assert_ne!(next_turn, first_turn);
    let (peer, _) = successor
        .accept()
        .await
        .expect("successor reached provider");
    drop(peer);
    wait_for_native_terminal(&mut client, next_turn).await;
    client.close(None).await.expect("close native client");
    server.await.expect("join server");
    rig.supervisor.shutdown().await.expect("drain");
    let row = rig
        .port
        .store
        .lock()
        .await
        .inspect(&operation)
        .await
        .expect("operation row read")
        .expect("dispatched row");
    assert!(row.dispatched_at_ms.is_some());
    assert_one_prompt(&rig, "successor");
}

fn read_requests(rig: &CommandPortRig, tag: &str) -> Vec<Value> {
    std::fs::read_to_string(rig.receipt.with_extension(tag))
        .expect("peer receipt")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wire JSON"))
        .collect()
}

fn assert_one_prompt(rig: &CommandPortRig, tag: &str) {
    let requests = read_requests(rig, tag);
    assert_eq!(
        requests
            .iter()
            .filter(|frame| frame["method"] == "session/prompt")
            .count(),
        1,
        "{requests:?}"
    );
}

fn assert_compaction_wire(rig: &CommandPortRig, tag: &str) {
    let requests = read_requests(rig, tag);
    let [request] = requests.as_slice() else {
        panic!("one compact request: {requests:?}")
    };
    assert_eq!(
        request
            .pointer("/params/prompt/0/text")
            .and_then(Value::as_str),
        Some("/compact")
    );
}

async fn compact(rig: &CommandPortRig) -> Result<SessionTurnHandle, CommandFailure> {
    rig.port
        .compact(SessionTargetCommand {
            target: rig.board_target.clone(),
            actor: rig.actor.clone(),
        })
        .await
}

#[tokio::test]
async fn compaction_successor_waits_for_an_ended_turn_operation() {
    let rig = command_port_rig(AgentFixture::TurnFollowUp(FixtureTurnEnding::EndTurn)).await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (prior, release) = begin_held_terminal(&rig).await;
    let waiting = rig
        .supervisor
        .observe_ended_turn_admission_for_test(&protocol_target("fixture-session"), prior);
    let follow_up = compact(&rig);
    tokio::pin!(follow_up);
    tokio::select! {
        reached = waiting => reached.expect("compaction waits behind the prior operation"),
        result = &mut follow_up => panic!("compaction answered during held custody: {result:?}"),
    }
    assert!(!rig.receipt.with_extension("successor").exists());
    release.send(()).expect("release terminal custody");
    follow_up.await.expect("compaction turn starts");
    let (peer, _) = successor
        .accept()
        .await
        .expect("compaction reached the peer");
    assert_compaction_wire(&rig, "successor");
    drop(peer);
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn a_compaction_predecessor_is_the_exact_ended_turn_operation() {
    let rig = command_port_rig(AgentFixture::TurnFollowUp(FixtureTurnEnding::EndTurn)).await;
    let foreground =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("first gate");
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    compact(&rig).await.expect("compaction turn starts");
    let (prior, held, release) = hold_terminal(&rig);
    release_foreground(&rig, &foreground).await;
    held.await.expect("compaction terminal held");
    assert_compaction_wire(&rig, "prompt");
    assert_ended_with_live_custody(&rig, &prior).await;
    let waiting = rig
        .supervisor
        .observe_ended_turn_admission_for_test(&protocol_target("fixture-session"), prior);
    let mut next = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, OperationId::generate()));
    tokio::select! {
        reached = waiting => reached.expect("prompt waits on the compaction operation"),
        result = &mut next => panic!("prompt answered during held custody: {result:?}"),
    }
    release.send(()).expect("release compaction terminal write");
    next.await.expect("follow-up admitted");
    let (peer, _) = successor.accept().await.expect("prompt reached the peer");
    drop(peer);
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn an_observed_refusal_qualifies_as_an_ended_turn() {
    let rig = command_port_rig(AgentFixture::TurnFollowUp(FixtureTurnEnding::Refusal)).await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (prior, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let snapshot = rig
        .supervisor
        .owner_session_snapshot(&target)
        .expect("owner");
    let ended = snapshot
        .live_idle_ended_turn()
        .expect("observed refusal is a turn ending");
    assert!(matches!(
        ended.outcome,
        TurnOutcome::Ended {
            stop_reason: StopReason::Refusal,
            ..
        }
    ));
    let waiting = rig
        .supervisor
        .observe_ended_turn_admission_for_test(&target, prior);
    let mut next = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, OperationId::generate()));
    tokio::select! {
        reached = waiting => reached.expect("the observed refusal is waitable"),
        result = &mut next => panic!("next prompt answered during held custody: {result:?}"),
    }
    release.send(()).expect("release the observed refusal");
    next.await.expect("follow-up admitted");
    let (peer, _) = successor.accept().await.expect("prompt reached the peer");
    drop(peer);
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn a_lost_turn_remains_ineligible_while_its_terminal_write_is_held() {
    let rig = command_port_rig(AgentFixture::TurnFollowUp(FixtureTurnEnding::Lost)).await;
    let foreground =
        UnixListener::bind(rig.receipt.with_extension("foreground.sock")).expect("first gate");
    start_foreground(&rig).await;
    let (prior, held, release) = hold_terminal(&rig);
    release_foreground(&rig, &foreground).await;
    held.await.expect("lost turn terminal held");
    wait_for_turn_end(&rig).await;
    let target = protocol_target("fixture-session");
    let snapshot = rig
        .supervisor
        .owner_session_snapshot(&target)
        .expect("owner");
    assert!(matches!(
        snapshot.last_live_turn.as_ref().map(|last| &last.outcome),
        Some(TurnOutcome::Lost { .. })
    ));
    assert!(snapshot.live_idle_ended_turn().is_none());
    assert!(rig.supervisor.live_operation_ids().contains(&prior));
    let refusal = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, OperationId::generate()))
        .await
        .expect_err("lost turn is refused while still held");
    assert_eq!(refusal.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refusal.stage, ConversationOperationFailureStage::Validation);
    assert_eq!(refusal.effect, ProviderOperationEffect::None);
    assert!(!rig.receipt.with_extension("successor").exists());
    release.send(()).expect("release lost terminal write");
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn two_waiters_contend_for_one_reservation_after_the_turn_ends() {
    let rig = follow_up_rig().await;
    let successor =
        UnixListener::bind(rig.receipt.with_extension("successor.sock")).expect("successor gate");
    let (prior, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let first_wait = rig
        .supervisor
        .observe_ended_turn_admission_for_test(&target, prior.clone());
    let mut first = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, OperationId::generate()));
    tokio::select! {
        reached = first_wait => reached.expect("first waiter"),
        result = &mut first => panic!("first answered during held custody: {result:?}"),
    }
    let second_wait = rig
        .supervisor
        .observe_ended_turn_admission_for_test(&target, prior);
    let mut second = rig
        .supervisor
        .prompt_contents(direct_prompt(&rig, OperationId::generate()));
    tokio::select! {
        reached = second_wait => reached.expect("second waiter"),
        result = &mut second => panic!("second answered during held custody: {result:?}"),
    }
    assert!(!rig.receipt.with_extension("successor").exists());
    release.send(()).expect("release prior terminal custody");
    let (first, second) = tokio::join!(first, second);
    let (accepted, refused) = match (first, second) {
        (Ok(accepted), Err(refused)) | (Err(refused), Ok(accepted)) => (accepted, refused),
        results => panic!("one reservation must win: {results:?}"),
    };
    assert_eq!(refused.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refused.stage, ConversationOperationFailureStage::Validation);
    assert!(String::from(refused.message).contains(accepted.operation.operation_id.as_str()));
    let (peer, _) = successor
        .accept()
        .await
        .expect("only the winner reaches the peer");
    drop(peer);
    rig.supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: accepted.operation.operation_id,
            timeout_seconds: PositiveSeconds::try_from(5).expect("fixture wait"),
        })
        .await
        .expect("winner settles");
    rig.supervisor.shutdown().await.expect("drain");
    assert_one_prompt(&rig, "successor");
}
