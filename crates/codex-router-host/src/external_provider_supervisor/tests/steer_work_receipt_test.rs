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
    PromptRequiredRefused,
    EndBeforeMarkReceipt,
    ReplaceRunBeforeMarkReceipt,
}

pub(super) fn steer_script(
    script: AcpFixtureScript,
    receipt: &Path,
    action: SteerPeerAction,
) -> AcpFixtureScript {
    let script = script.expect_request(
        "foreground",
        "session/prompt",
        json!({"prompt": [{"type":"text","text":FOREGROUND}]}),
    );
    let script = if matches!(
        action,
        SteerPeerAction::EndBeforeMarkReceipt | SteerPeerAction::ReplaceRunBeforeMarkReceipt
    ) {
        script.wait_for_socket_signal(&receipt.with_extension("steer.sock"))
    } else {
        script.expect_recorded_request(
            "steer",
            "_session/steering",
            json!({"prompt": [{"type":"text","text":STEER}]}),
            &receipt.with_extension("ordinary-steer"),
        )
    };
    let script = if matches!(action, SteerPeerAction::PromptRequiredRefused) {
        script
    } else {
        script.respond("foreground", json!({"stopReason":"end_turn"}))
    };
    match action {
        SteerPeerAction::LostReply | SteerPeerAction::PassedLimit => script
            .wait_for_socket_signal(&receipt.with_extension("steer.sock"))
            .exit(),
        SteerPeerAction::MalformedReply => {
            script.respond("steer", json!({"outcome":"invalidFixtureOutcome"}))
        }
        SteerPeerAction::EndBeforeMarkReceipt => script,
        SteerPeerAction::ReplaceRunBeforeMarkReceipt => script
            .expect_request(
                "reload",
                "session/load",
                json!({"sessionId":"saved-notice-session"}),
            )
            .respond("reload", json!({})),
        SteerPeerAction::PromptRequiredRefused => script
            .respond("steer", json!({"outcome":"promptRequired"}))
            .wait_for_socket_signal(&receipt.with_extension("steer.sock"))
            .respond("foreground", json!({"stopReason":"end_turn"})),
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
            .submit_owned_delivery(request, MessageDelivery::Auto)
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

async fn wait_for_fixture_operation(
    rig: &SteeringSession,
    operation_id: OperationId,
) -> Result<
    collaboration_protocol::ConversationOperationWaitResult,
    Box<collaboration_protocol::ConversationOperationFailure>,
> {
    use collaboration_contracts::ProviderConversationBackend as _;
    use collaboration_protocol::{ConversationOperationWaitRequest, PositiveSeconds};

    rig.supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id,
            timeout_seconds: PositiveSeconds::try_from(2).expect("fixture wait budget"),
        })
        .await
        .map_err(Box::new)
}

fn assert_named_steer_refusal(
    answer: Result<
        collaboration_protocol::ConversationOperationWaitResult,
        Box<collaboration_protocol::ConversationOperationFailure>,
    >,
) {
    use collaboration_protocol::{
        ConversationOperationFailureKind, ConversationOperationFailureStage,
        ProviderOperationEffect,
    };

    let failure = answer.expect_err("named steer refuses once the turn is gone");
    assert_eq!(failure.kind, ConversationOperationFailureKind::NoActiveTurn);
    assert_eq!(failure.stage, ConversationOperationFailureStage::Dispatch);
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    assert_eq!(
        String::from(failure.message),
        "no running turn; send a prompt, or use auto"
    );
}

#[tokio::test]
async fn named_steer_is_refused_without_replacement_when_the_harness_requires_a_prompt() {
    let rig = managed_notice_fixture(
        Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        NoticeScript::OrdinarySteer(SteerPeerAction::PromptRequiredRefused),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let listener = tokio::net::UnixListener::bind(rig.receipt.with_extension("steer.sock"))
        .expect("foreground release socket");
    let foreground = take_foreground(&rig).await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("session owner");
    let request = request(&rig, STEER);
    let operation = request.operation_id.clone();

    rig.supervisor
        .prompt_contents(request)
        .await
        .expect("named steer admitted while running");
    let answer = wait_for_fixture_operation(&rig, operation).await;
    let observed_turn = owner.snapshots().borrow().turn;
    let (peer, _) = listener
        .accept()
        .await
        .expect("peer awaiting foreground release");
    drop(peer);
    let ending = owner
        .snapshots()
        .wait_for(|snapshot| {
            snapshot.turn == SessionTurn::Idle
                && snapshot
                    .last_live_turn
                    .as_ref()
                    .is_some_and(|ended| ended.turn == foreground)
        })
        .await
        .expect("foreground ends after its release")
        .last_live_turn
        .clone();

    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_eq!(observed_turn, SessionTurn::Running { turn: foreground });
    assert!(
        matches!(ending, Some(ended) if matches!(ended.outcome, session_event_model::TurnOutcome::Ended {
            stop_reason: session_event_model::StopReason::EndTurn, ..
        }))
    );
    assert_named_steer_refusal(answer);
    assert_one_request(
        &rig.receipt.with_extension("ordinary-steer"),
        "_session/steering",
    );
    assert!(!rig.receipt.exists(), "the peer saw unexpected traffic");
}

#[tokio::test]
async fn named_steer_is_refused_when_the_turn_ends_before_its_mark_returns() {
    let rig = managed_notice_fixture(
        Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        NoticeScript::OrdinarySteer(SteerPeerAction::EndBeforeMarkReceipt),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let listener = tokio::net::UnixListener::bind(rig.receipt.with_extension("steer.sock"))
        .expect("foreground release socket");
    take_foreground(&rig).await;
    let request = request(&rig, STEER);
    let operation = request.operation_id.clone();
    let (saved, release) = rig
        .supervisor
        .pause_steer_mark_receipt_for_test(&rig.target, operation.clone());
    let supervisor = Arc::clone(&rig.supervisor);
    let admitted = tokio::spawn(async move { supervisor.prompt_contents(request).await });
    let (peer, _) = listener
        .accept()
        .await
        .expect("peer waiting to end the foreground");

    saved.await.expect("steer mark saved before release");
    drop(peer);
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("session owner");
    owner
        .snapshots()
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .expect("foreground ended before the mark returned");
    release.send(()).expect("return the saved mark");
    admitted
        .await
        .expect("submission task")
        .expect("started with a running turn");
    let answer = wait_for_fixture_operation(&rig, operation).await;

    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_named_steer_refusal(answer);
    assert!(!rig.receipt.exists(), "the peer saw unexpected traffic");
}

#[tokio::test]
async fn named_steer_reports_stale_generation_after_its_marked_run_is_replaced() {
    use collaboration_contracts::ProviderConversationBackend as _;
    use collaboration_protocol::{
        ConversationCloseRequest, ConversationOperationFailureKind,
        ConversationOperationSettlement, ConversationOperationWaitOutput, ProviderOperationEffect,
    };
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle,
        session_owner::{OwnerInput, RestoreDisposition, RevokeReason, SessionOwnerReply},
    };

    let rig = managed_notice_fixture(
        Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH)),
        NoticeScript::OrdinarySteer(SteerPeerAction::ReplaceRunBeforeMarkReceipt),
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let listener = tokio::net::UnixListener::bind(rig.receipt.with_extension("steer.sock"))
        .expect("foreground release socket");
    let foreground = take_foreground(&rig).await;
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("session owner");
    let SessionLifecycle::Live { generation: old } = owner.snapshots().borrow().lifecycle else {
        panic!("foreground starts on a live run");
    };
    let request = request(&rig, STEER);
    let operation = request.operation_id.clone();
    let (saved, release_mark) = rig
        .supervisor
        .pause_steer_mark_receipt_for_test(&rig.target, operation.clone());
    let supervisor = rig.supervisor.clone();
    let pending = tokio::spawn(async move { supervisor.prompt_contents(request).await });
    let (peer, _) = listener
        .accept()
        .await
        .expect("peer waits to end the foreground");
    saved.await.expect("old steer mark saved");
    drop(peer);
    owner
        .snapshots()
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .expect("foreground ended");

    let close = OperationId::generate();
    rig.supervisor
        .close(ConversationCloseRequest {
            operation_id: close.clone(),
            target: rig.target.clone(),
            generation: None,
            requested_by: requester().into(),
            approver: requester().into(),
        })
        .await
        .expect("close accepted");
    assert!(matches!(
        wait_for_fixture_operation(&rig, close)
            .await
            .expect("close settled")
            .output,
        ConversationOperationWaitOutput::Available {
            settlement: ConversationOperationSettlement::Closed { .. }
        }
    ));
    // The same operation-owned recovery used when the ACP delivery route
    // loads a stored session. A separate user load rightly refuses here.
    let restored = owner
        .restore_for_delivery(uuid::Uuid::now_v7())
        .await
        .expect("delivery recovery made a new run");
    assert!(matches!(
        restored.reply, SessionOwnerReply::RestoreFinished(result)
            if matches!(result.disposition, RestoreDisposition::Restored { .. })
    ));
    let SessionLifecycle::Live {
        generation: replacement,
    } = owner.snapshots().borrow().lifecycle
    else {
        panic!("reload must establish a live run");
    };
    assert_ne!(old, replacement);

    release_mark
        .send(())
        .expect("return the old mark after replacement");
    pending
        .await
        .expect("steer admission task")
        .expect("steer admitted on the old run");
    let answer = wait_for_fixture_operation(&rig, operation).await;
    let runtime = rig
        .supervisor
        .runtime_for(&rig.target.endpoint)
        .expect("fixture runtime");
    let legacy_id = OperationId::generate();
    let legacy = crate::external_provider_supervisor::provider_owner_prompt::retained_prompt(
        &runtime,
        legacy_id.clone(),
        rig.target.clone(),
        crate::session_owner_calls::OwnedSettlement {
            reply: SessionOwnerReply::InputUnsent {
                input: OwnerInput {
                    generation: old,
                    operation: uuid::Uuid::parse_str(legacy_id.as_str()).expect("operation UUID"),
                    input: InputId::generate(),
                    turn: foreground,
                },
                reason: RevokeReason::RunReplaced,
            },
            saved: true,
            turn_approver: None,
        },
    );
    rig.supervisor.shutdown().await.expect("fixture shutdown");

    let failure = answer.expect_err("old named steer refused");
    assert_eq!(
        failure.kind,
        ConversationOperationFailureKind::StaleGeneration
    );
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    assert!(
        matches!(legacy, crate::external_provider_supervisor::ProviderOperationCompletion::Owned { result: Err(failure), .. }
        if failure.kind == ConversationOperationFailureKind::Unavailable && failure.effect == ProviderOperationEffect::None)
    );
    assert!(
        !rig.receipt.exists(),
        "no steer or prompt escapes on the replaced run"
    );
}
