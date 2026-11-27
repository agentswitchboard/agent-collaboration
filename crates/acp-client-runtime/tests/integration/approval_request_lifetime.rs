#[path = "approval_request_lifetime/commit_edges.rs"]
mod commit_edges;
#[path = "approval_request_lifetime/fixture.rs"]
mod fixture;

use fixture::{DecisionPlan, Peer, Policy, PolicyEvent, SESSION, connect};
use serde_json::{Value, json};
use std::sync::Arc;
use switchboard_harness_contracts::{
    ExactScope, ExactTurn, RequestDecisionRefusal, StopRequestsCause,
};

#[derive(Clone, Copy)]
enum Callback {
    Permission,
    Plan,
    Question,
    Elicitation,
}

impl Callback {
    fn frame(self, session: &str, id: u64) -> Value {
        let (method, params) = match self {
            Self::Permission => (
                "session/request_permission",
                json!({
                    "sessionId":session,
                    "toolCall":{"toolCallId":"fixture-tool","title":"Inspect fixture", "kind":"other"},
                    "options":[{"optionId":"allow","name":"Allow once","kind":"allow_once"}]
                }),
            ),
            Self::Plan => (
                "cursor/create_plan",
                json!({"toolCallId":"fixture-tool","name":"Plan","overview":"Overview","plan":"# Steps","todos":[]}),
            ),
            Self::Question => (
                "cursor/ask_question",
                json!({"toolCallId":"fixture-tool","title":"Choose","questions":[
                    {"id":"choice","prompt":"Proceed?","options":[{"id":"yes","label":"Yes"}]}
                ]}),
            ),
            Self::Elicitation => (
                "elicitation/create",
                json!({"mode":"form","sessionId":session,"message":"Confirm",
                    "requestedSchema":{"type":"object","properties":{"name":{"type":"string"}}}}),
            ),
        };
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
    }

    fn cancelled(self) -> Value {
        match self {
            Self::Permission | Self::Question => json!({"outcome":{"outcome":"cancelled"}}),
            Self::Plan => json!({"outcome":"cancelled"}),
            Self::Elicitation => json!({"action":"cancel"}),
        }
    }
}

fn selected_permission() -> Value {
    json!({"outcome":{"outcome":"selected","optionId":"allow"}})
}

fn assert_reply(frame: &Value, id: u64, outcome: Value) {
    assert_eq!(*frame, json!({"jsonrpc":"2.0","id":id,"result":outcome}));
}

async fn add_tool(peer: &mut Peer) {
    peer.send(json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":SESSION, "update":{
            "sessionUpdate":"tool_call","toolCallId":"fixture-tool","title":"Interaction",
            "kind":"other","status":"pending"
        }
    }}))
    .await;
}

#[expect(
    clippy::panic,
    reason = "The fixture requires an exact turn at policy admission."
)]
async fn wait_started(policy: &Policy, id: u64, operation: u64) -> ExactTurn {
    let event = policy
        .observe(|event| {
            matches!(event, PolicyEvent::Started {
        request, operation: actual, ..
    } if *request == id && *actual == operation)
        })
        .await;
    let PolicyEvent::Started {
        scope: ExactScope::Turn(turn),
        ..
    } = event
    else {
        panic!("real callback belongs to an exact turn: {event:?}");
    };
    turn
}

fn assert_one_policy_stop(
    policy: &Policy,
    operation: u64,
    turn: ExactTurn,
    cause: StopRequestsCause,
) {
    let seen: Vec<_> = policy.events().into_iter()
        .filter(|event| matches!(event, PolicyEvent::Stopped { operation: actual, .. } if *actual == operation))
        .collect();
    assert_eq!(
        seen,
        [PolicyEvent::Stopped {
            operation,
            turn,
            cause
        }],
        "ACP must call the exact policy stop once"
    );
}

#[tokio::test]
async fn policy_refusals_decline_each_new_wire_request_without_cancel_or_redecision() {
    let policy = Arc::new(Policy::default());
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    add_tool(&mut peer).await;

    let mut id = 90;
    let mut expected_calls = Vec::new();
    for callback in [
        Callback::Permission,
        Callback::Plan,
        Callback::Question,
        Callback::Elicitation,
    ] {
        for refusal in [
            RequestDecisionRefusal::AlreadyAnswered,
            RequestDecisionRefusal::AnsweredElsewhere,
            RequestDecisionRefusal::ScopeEnded,
        ] {
            id += 1;
            policy.plan(id, DecisionPlan::Refuse(refusal));
            peer.send(callback.frame(SESSION, id)).await;
            assert_reply(&peer.read().await, id, callback.cancelled());
            assert!(policy.events().contains(&PolicyEvent::Refused(id, refusal)));
            expected_calls.push(id);
        }
    }
    for (callback, refusal, answer_after_reuse) in [
        (
            Callback::Permission,
            RequestDecisionRefusal::AlreadyAnswered,
            selected_permission(),
        ),
        (
            Callback::Elicitation,
            RequestDecisionRefusal::AnsweredElsewhere,
            json!({"action":"decline"}),
        ),
    ] {
        id += 1;
        policy.plan(id, DecisionPlan::Refuse(refusal));
        peer.send(json!([
            callback.frame(SESSION, id),
            Callback::Permission.frame(SESSION, id + 1),
        ]))
        .await;
        assert_eq!(
            peer.read().await,
            json!([
                {"jsonrpc":"2.0","id":id,"result":callback.cancelled()},
                {"jsonrpc":"2.0","id":id+1,"result":selected_permission()},
            ])
        );
        assert!(policy.events().contains(&PolicyEvent::Refused(id, refusal)));

        peer.send(callback.frame(SESSION, id)).await;
        assert_reply(&peer.read().await, id, answer_after_reuse);
        expected_calls.extend([id, id, id + 1]);
        id += 1;
    }
    peer.sync().await;
    let mut calls: Vec<_> = policy
        .events()
        .into_iter()
        .filter_map(|event| {
            if let PolicyEvent::Started { request, .. } = event {
                Some(request)
            } else {
                None
            }
        })
        .collect();
    calls.sort_unstable();
    assert_eq!(
        calls, expected_calls,
        "one decision for each outstanding request, including reuse after flush"
    );
    assert!(policy.events().iter().all(|event| matches!(
        event,
        PolicyEvent::Started { .. } | PolicyEvent::Finished(_) | PolicyEvent::Refused(..)
    )));
    peer.respond(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    joined
        .await
        .expect("prompt task")
        .expect("turn ended normally");
    client.shutdown().await;
}

#[tokio::test]
async fn ended_turn_fences_late_approval_and_question_until_the_next_turn_is_live() {
    let policy = Arc::new(Policy::default());
    let approval = policy.hold_answer(91);
    let question = policy.hold_answer(92);
    let stop = policy.hold_stop(1);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (first, first_done) = peer.start_prompt(&client, SESSION, 1).await;
    peer.send(Callback::Permission.frame(SESSION, 91)).await;
    peer.send(Callback::Elicitation.frame(SESSION, 92)).await;
    let turn = wait_started(&policy, 91, 1).await;
    assert_eq!(wait_started(&policy, 92, 1).await, turn);
    let (pending, drained) = client
        .observe_approval_scope_drain_for_test(SESSION)
        .expect("A still owns its responders");
    assert_eq!(pending.len(), 2);
    peer.respond(&first, json!({"stopReason":"end_turn"})).await;
    first_done
        .await
        .expect("first task")
        .expect("A ends with the pipe open");
    policy
        .observe(|event| *event == PolicyEvent::ScopeCancelled(91))
        .await;
    policy
        .observe(|event| *event == PolicyEvent::ScopeCancelled(92))
        .await;
    assert_one_policy_stop(&policy, 1, turn, StopRequestsCause::TurnEnded);

    let (second, second_done) = peer.start_prompt(&client, SESSION, 2).await;
    approval
        .send(())
        .expect("A's old approval selection is consumed");
    assert_reply(&peer.read().await, 91, Callback::Permission.cancelled());
    question
        .send(())
        .expect("A's old question decision is consumed");
    assert_reply(&peer.read().await, 92, Callback::Elicitation.cancelled());
    drained.await;
    assert!(policy.events().contains(&PolicyEvent::Finished(91)));
    assert!(policy.events().contains(&PolicyEvent::Finished(92)));
    stop.send(()).expect("release A's stopped policy");
    peer.send(Callback::Permission.frame(SESSION, 93)).await;
    assert_reply(&peer.read().await, 93, selected_permission());
    assert_ne!(wait_started(&policy, 93, 2).await, turn);
    peer.sync().await;
    peer.respond(&second, json!({"stopReason":"end_turn"}))
        .await;
    second_done
        .await
        .expect("second task")
        .expect("B remains independent");
    client.shutdown().await;
}

#[tokio::test]
async fn peer_request_cancellation_does_not_stop_the_turn_or_its_pending_sibling() {
    let policy = Arc::new(Policy::default());
    let _first = policy.hold_answer(91);
    let sibling = policy.hold_answer(92);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    peer.send(Callback::Permission.frame(SESSION, 91)).await;
    peer.send(Callback::Permission.frame(SESSION, 92)).await;
    assert_eq!(
        wait_started(&policy, 91, 1).await,
        wait_started(&policy, 92, 1).await
    );
    peer.send(json!({"jsonrpc":"2.0","method":"$/cancel_request","params":{"requestId":91}}))
        .await;
    assert_reply(&peer.read().await, 91, Callback::Permission.cancelled());
    assert!(policy.events().contains(&PolicyEvent::RequestCancelled(91)));
    let (pending, drained) = client
        .observe_approval_scope_drain_for_test(SESSION)
        .expect("turn remains live");
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending.first().expect("the sibling responder").request_id,
        "92"
    );
    sibling
        .send(())
        .expect("sibling remains independently pending");
    assert_reply(&peer.read().await, 92, selected_permission());
    drained.await;
    peer.sync().await;
    assert!(policy.events().iter().all(|event| !matches!(
        event,
        PolicyEvent::Stopped { .. } | PolicyEvent::ScopeCancelled(_)
    )));
    peer.respond(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    joined
        .await
        .expect("prompt task")
        .expect("the turn survives request cancellation");
    client.shutdown().await;
}

#[tokio::test]
async fn generation_retirement_stops_and_drains_the_exact_scope_on_a_live_pipe() {
    let policy = Arc::new(Policy::default());
    policy.plan(91, DecisionPlan::WaitForScopeEnd);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (_prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    peer.send(Callback::Permission.frame(SESSION, 91)).await;
    let turn = wait_started(&policy, 91, 1).await;
    let (pending, drained) = client
        .observe_approval_scope_drain_for_test(SESSION)
        .expect("tracked responder");
    assert_eq!(pending.len(), 1);

    client.retire_approval_run_for_test(SESSION, turn.run);
    assert_reply(&peer.read().await, 91, Callback::Permission.cancelled());
    drained.await;
    peer.sync().await;
    assert_one_policy_stop(&policy, 1, turn, StopRequestsCause::GenerationRetired);
    assert!(policy.events().contains(&PolicyEvent::ScopeCancelled(91)));
    assert!(policy.events().contains(&PolicyEvent::Finished(91)));
    drop(peer);
    client.shutdown().await;
    assert!(joined.await.expect("prompt task").is_err());
    assert_one_policy_stop(&policy, 1, turn, StopRequestsCause::GenerationRetired);
}

#[tokio::test(flavor = "current_thread")]
async fn provider_eof_calls_policy_stop_and_joins_the_pending_responder_before_exit() {
    exercise_eof_drain(StopRequestsCause::ConnectionLost).await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_eof_during_generation_drain_keeps_the_first_stop_cause() {
    exercise_eof_drain(StopRequestsCause::GenerationRetired).await;
}

#[expect(
    clippy::expect_used,
    reason = "The held ACP work and its gates are required fixture steps."
)]
async fn exercise_eof_drain(first_cause: StopRequestsCause) {
    let policy = Arc::new(Policy::default());
    let answer = policy.hold_answer_until_scope_end(91);
    let stop = policy.hold_stop(1);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    let (_prompt, joined) = peer.start_prompt(&client, SESSION, 1).await;
    peer.send(Callback::Permission.frame(SESSION, 91)).await;
    let turn = wait_started(&policy, 91, 1).await;
    let (pending, drained) = client
        .observe_approval_scope_drain_for_test(SESSION)
        .expect("tracked responder");
    assert_eq!(pending.len(), 1);
    tokio::pin!(drained);
    let (_shutdown, settled) = client.connection_fixture_signals();
    let teardown = client.connection_drain_started_for_test();
    assert!(!settled.is_cancelled());

    if first_cause == StopRequestsCause::GenerationRetired {
        client.retire_approval_run_for_test(SESSION, turn.run);
        policy
            .observe(|event| {
                *event
                    == PolicyEvent::Stopped {
                        operation: 1,
                        turn,
                        cause: first_cause,
                    }
            })
            .await;
        assert!(futures_util::poll!(drained.as_mut()).is_pending());
    }

    drop(peer);
    let reached_drain = tokio::select! {
        biased;
        () = settled.cancelled() => false,
        () = teardown.cancelled() => true,
    };
    assert!(
        reached_drain,
        "teardown acknowledged while the stop and responder were held"
    );
    assert!(futures_util::poll!(drained.as_mut()).is_pending());
    policy
        .observe(|event| *event == PolicyEvent::ScopeCancelled(91))
        .await;
    assert!(!settled.is_cancelled());

    answer.send(()).expect("release the pending responder");
    let responder_drained_first = tokio::select! {
        biased;
        () = settled.cancelled() => false,
        () = drained.as_mut() => true,
    };
    assert!(
        responder_drained_first,
        "teardown acknowledged before the responder drained"
    );
    assert!(policy.events().contains(&PolicyEvent::Finished(91)));
    assert!(!policy.events().contains(&PolicyEvent::StopFinished(1)));
    assert!(!settled.is_cancelled());

    stop.send(()).expect("release the pending policy stop");
    settled.cancelled().await;
    assert!(policy.events().contains(&PolicyEvent::StopFinished(1)));
    client.shutdown().await;
    assert!(joined.await.expect("prompt task").is_err());
    assert_one_policy_stop(&policy, 1, turn, first_cause);
}

#[tokio::test]
async fn managed_refusal_queues_before_cancel_while_an_independent_batch_sibling_is_held() {
    let policy = Arc::new(Policy::managed_batch());
    let sibling = policy.hold_answer(91);
    let (client, mut peer) = connect(policy.clone()).await;
    peer.create_session(&client, SESSION).await;
    peer.create_session(&client, "sibling-session").await;
    let (first, first_done) = peer.start_prompt(&client, SESSION, 1).await;
    let (other, other_done) = peer.start_prompt(&client, "sibling-session", 2).await;
    peer.send(json!([
        Callback::Elicitation.frame("sibling-session", 91),
        Callback::Permission.frame(SESSION, 92)
    ]))
    .await;
    wait_started(&policy, 91, 2).await;
    let event = policy
        .observe(|event| matches!(event, PolicyEvent::ManagedCancel { .. }))
        .await;
    assert_eq!(
        event,
        PolicyEvent::ManagedCancel {
            operation: 1,
            queued: Some(agent_client_protocol::schema::v1::RequestId::Number(92)),
        },
        "the real refusal enqueue must come before the policy requests cancel"
    );
    let cancel = peer.read().await;
    assert_eq!(
        cancel["method"], "session/cancel",
        "the held sibling defers the batch frame"
    );
    assert_eq!(cancel["params"]["sessionId"], SESSION);
    peer.respond(&first, json!({"stopReason":"cancelled"}))
        .await;
    let outcome = first_done
        .await
        .expect("managed task")
        .expect("original turn settles");
    assert_eq!(
        outcome.permission_refusal_reason,
        Some(acp_client_runtime::ExternalProviderApprovalRefusalReason::Managed(
            session_event_model::managed_interaction_refusal::ManagedInteractionRefusal::ApprovalsUnavailableOnBuild
        )),
        "the committed managed reply remains visible to the prompt caller"
    );
    peer.sync().await;
    sibling
        .send(())
        .expect("independent sibling remains pending");
    let response = peer.read().await;
    let batch = response
        .as_array()
        .expect("both responses share their original batch");
    assert_eq!(
        batch,
        &[
            json!({"jsonrpc":"2.0","id":91,"result":{"action":"decline"}}),
            json!({"jsonrpc":"2.0","id":92,"result":Callback::Permission.cancelled()}),
        ]
    );
    peer.respond(&other, json!({"stopReason":"end_turn"})).await;
    other_done
        .await
        .expect("sibling task")
        .expect("the sibling turn remains live");
    peer.sync().await;
    client.shutdown().await;
}
