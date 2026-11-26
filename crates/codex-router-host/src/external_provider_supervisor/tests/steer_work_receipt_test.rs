use super::*;
use crate::{provider_test_support::AcpFixtureScript, session_owner_calls::InputAcceptance};
use acp_client_runtime::handover_test_control::{HandoverPause, HandoverPausePoint};
use collaboration_protocol::MessageDelivery;
use provider_session_machines::{
    SessionGeneration,
    session_owner::{AuthorizationTarget, SessionOwnerCommand},
    session_turn_machine::{SessionTurn, TurnId},
};
use serde_json::json;
use session_event_model::{InputId, PromptContent, ProviderSubmission};
use std::{path::Path, time::Duration};
use tokio::sync::oneshot;

const FOREGROUND: &str = "hold this foreground turn";
const STEER: &str = "one ordinary steer";

#[derive(Clone, Copy)]
pub(super) enum SteerPeerAction {
    LostReply,
    PassedLimit,
    MalformedReply,
    PromptRequired,
}

pub(super) fn steer_script(
    script: AcpFixtureScript,
    receipt: &Path,
    action: SteerPeerAction,
) -> AcpFixtureScript {
    let script = script
        .expect_request(
            "foreground",
            "session/prompt",
            json!({"prompt": [{"type":"text","text":FOREGROUND}]}),
        )
        .expect_recorded_request(
            "steer",
            "_session/steering",
            json!({"prompt": [{"type":"text","text":STEER}]}),
            &receipt.with_extension("ordinary-steer"),
        )
        .respond("foreground", json!({"stopReason":"end_turn"}));
    match action {
        SteerPeerAction::LostReply | SteerPeerAction::PassedLimit => script
            .wait_for_socket_signal(&receipt.with_extension("steer.sock"))
            .exit(),
        SteerPeerAction::MalformedReply => {
            script.respond("steer", json!({"outcome":"invalidFixtureOutcome"}))
        }
        SteerPeerAction::PromptRequired => script
            .respond("steer", json!({"outcome":"promptRequired"}))
            .expect_recorded_request(
                "replacement",
                "session/prompt",
                json!({"prompt":[{"type":"text","text":STEER}]}),
                &receipt.with_extension("replacement"),
            )
            .respond("replacement", json!({"stopReason":"end_turn"})),
    }
}

fn request(rig: &SteeringSession, text: &str) -> ProviderPromptContentsRequest {
    ProviderPromptContentsRequest {
        operation_id: OperationId::generate(),
        input_id: InputId::generate(),
        target: rig.target.clone(),
        requested_by: requester().into(),
        approver: requester().into(),
        input: acp_client_runtime::ProviderTurnInput::Prompt(ProviderSubmission::plain(vec![
            PromptContent::text(text.into()).expect("valid fixture prompt"),
        ])),
        busy: if text == STEER {
            PromptBusyPolicy::Send(MessageDelivery::Steer)
        } else {
            PromptBusyPolicy::RefuseBusy
        },
    }
}

fn stop_before_offer(rig: &SteeringSession, input: InputId) -> HandoverPause {
    rig.supervisor
        .runtime_for(&rig.target.endpoint)
        .expect("provider runtime")
        .pause_work_handover_for_test(HandoverPausePoint::BeforeOffer, input)
}

async fn take_foreground(rig: &SteeringSession) -> TurnId {
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("session owner");
    assert_eq!(
        rig.supervisor
            .submit_owned_direct_delivery(request(rig, FOREGROUND))
            .await
            .expect("foreground"),
        InputAcceptance::Started
    );
    let snapshot = owner.snapshots().borrow().clone();
    let SessionTurn::Running { turn } = snapshot.turn else {
        panic!("foreground must own a running turn");
    };
    turn
}

fn assert_one_request(path: &Path, method: &str) {
    let text = std::fs::read_to_string(path).expect("peer request capture");
    let requests = text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("request JSON"))
        .collect::<Vec<_>>();
    let [one] = requests.as_slice() else {
        panic!("one peer request required: {requests:?}");
    };
    assert_eq!(one["method"], method);
    assert_eq!(one["params"]["prompt"][0]["text"], STEER);
}

async fn verify_unknown_steer(action: SteerPeerAction) {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        Arc::clone(&clock),
        NoticeScript::OrdinarySteer(action),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let listener = tokio::net::UnixListener::bind(rig.receipt.with_extension("steer.sock"))
        .expect("peer release socket");
    let old_turn = take_foreground(&rig).await;
    clock
        .advance(Duration::from_secs(1))
        .expect("different deadline from foreground");
    let deadline = clock.monotonic_now().after(Duration::from_secs(10));
    let request = request(&rig, STEER);
    let input = request.input_id.clone();
    let first = stop_before_offer(&rig, input.clone());
    let supervisor = Arc::clone(&rig.supervisor);
    let attempt = tokio::spawn(async move {
        supervisor
            .submit_owned_delivery(request, MessageDelivery::Steer)
            .await
    });
    let accepted = first
        .reached
        .await
        .expect("original authorization reached the offer");
    assert_eq!(accepted.input_id, input);
    assert_eq!(
        accepted.target,
        AuthorizationTarget::ExistingTurn { turn: old_turn }
    );
    let mut next = stop_before_offer(&rig, input);
    first.release.send(()).expect("send the one steer");
    let (peer, _) = listener.accept().await.expect("peer accepted that steer");
    assert_one_request(
        &rig.receipt.with_extension("ordinary-steer"),
        "_session/steering",
    );

    let mut keep_peer = Some(peer);
    if matches!(action, SteerPeerAction::LostReply) {
        drop(keep_peer.take());
    } else {
        clock.not_due_check_now(deadline).await;
        clock
            .advance(Duration::from_secs(10))
            .expect("steer result deadline");
    }
    tokio::select! {
        outcome = attempt => {
            assert_eq!(outcome.expect("attempt joined").expect("typed outcome"), InputAcceptance::Unknown);
        }
        repeated = &mut next.reached => {
            let _released = next.release.send(());
            panic!("an uncertain steer was offered again: {repeated:?}");
        }
    }

    assert!(matches!(
        next.reached.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(keep_peer);
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_one_request(
        &rig.receipt.with_extension("ordinary-steer"),
        "_session/steering",
    );
    assert!(!rig.receipt.exists(), "the peer saw unexpected traffic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_steer_reply_yields_unknown_and_no_fresh_offer() {
    verify_unknown_steer(SteerPeerAction::LostReply).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_expired_steer_reply_yields_unknown_and_no_fresh_offer() {
    verify_unknown_steer(SteerPeerAction::PassedLimit).await;
}

#[tokio::test]
async fn a_malformed_steer_reply_keeps_the_distinct_protocol_failure() {
    use collaboration_contracts::ProviderConversationBackend as _;
    use collaboration_protocol::{
        ConversationOperationFailureKind, ConversationOperationFailureStage,
        ConversationOperationWaitRequest, PositiveSeconds, ProviderOperationEffect,
    };

    let rig = managed_notice_fixture(
        Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        NoticeScript::OrdinarySteer(SteerPeerAction::MalformedReply),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    take_foreground(&rig).await;
    let request = request(&rig, STEER);
    let operation_id = request.operation_id.clone();
    rig.supervisor
        .prompt_contents(request)
        .await
        .expect("ordinary steer admitted");

    let failure = rig
        .supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id,
            timeout_seconds: PositiveSeconds::try_from(2).expect("fixture wait budget"),
        })
        .await
        .expect_err("malformed reply is not an ordinary uncertain result");

    assert_eq!(
        failure.kind,
        ConversationOperationFailureKind::ProtocolViolation
    );
    assert_eq!(failure.stage, ConversationOperationFailureStage::Dispatch);
    assert_eq!(failure.effect, ProviderOperationEffect::Unknown);
    assert_eq!(
        String::from(failure.message),
        "provider returned an invalid steering result"
    );
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_one_request(
        &rig.receipt.with_extension("ordinary-steer"),
        "_session/steering",
    );
    assert!(!rig.receipt.exists(), "the peer saw unexpected traffic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prompt_required_does_not_offer_the_replacement_until_the_undo_receipt() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock,
        NoticeScript::OrdinarySteer(SteerPeerAction::PromptRequired),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let old_turn = take_foreground(&rig).await;
    let request = request(&rig, STEER);
    let input = request.input_id.clone();
    let (undo_written, release_undo_receipt) = rig
        .supervisor
        .pause_undo_receipt_for_test(&rig.target, request.operation_id.clone());
    let first = stop_before_offer(&rig, input.clone());
    let supervisor = Arc::clone(&rig.supervisor);
    let attempt = tokio::spawn(async move {
        supervisor
            .submit_owned_delivery(request, MessageDelivery::Steer)
            .await
    });
    let original = first.reached.await.expect("original steer");
    assert_eq!(
        original.target,
        AuthorizationTarget::ExistingTurn { turn: old_turn }
    );
    let mut replacement = stop_before_offer(&rig, input.clone());
    first.release.send(()).expect("send steer");
    undo_written.await.expect("real undo stored");

    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("session owner");
    tokio::select! {
        barrier = owner.admit(SessionOwnerCommand::AcceptSettings { generation: SessionGeneration::UNTRACKED }) => {
            assert!(barrier.is_err(), "stale generation is a read-only owner barrier");
        }
        reached = &mut replacement.reached => {
            let _released = replacement.release.send(());
            panic!("replacement offered before undo was acknowledged: {reached:?}");
        }
    }
    assert!(!rig.receipt.with_extension("replacement").exists());
    release_undo_receipt
        .send(())
        .expect("deliver the real undo outcome");
    let new_authorization = replacement.reached.await.expect("replacement now offered");
    assert_eq!(new_authorization.input_id, input);
    assert_eq!(new_authorization.target, AuthorizationTarget::NewTurn);
    assert!(new_authorization.authorization_seq > original.authorization_seq);
    let runtime = rig
        .supervisor
        .runtime_for(&rig.target.endpoint)
        .expect("provider runtime");
    let replacement_turn = runtime
        .held_work_approval_for_test(&String::from(rig.target.session_id.clone()))
        .expect("replacement turn");
    assert_ne!(replacement_turn, old_turn);
    replacement
        .release
        .send(())
        .expect("publish positive-control replacement");
    assert_eq!(
        attempt
            .await
            .expect("attempt joined")
            .expect("typed outcome"),
        InputAcceptance::Queued
    );
    owner
        .snapshots()
        .wait_for(|snapshot| {
            snapshot
                .last_live_turn
                .as_ref()
                .is_some_and(|ended| ended.input == input && ended.turn == replacement_turn)
        })
        .await
        .expect("replacement settles under its own turn");
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_one_request(
        &rig.receipt.with_extension("ordinary-steer"),
        "_session/steering",
    );
    assert_one_request(&rig.receipt.with_extension("replacement"), "session/prompt");
    assert!(!rig.receipt.exists(), "the peer saw unexpected traffic");
}
