//! Local restore retirement and admission, through the real owning store.

#[path = "restore_recovery/admission_budget_test.rs"]
mod admission_budget_tests;

#[path = "restore_recovery/admission_race_test.rs"]
mod admission_race_tests;

#[path = "restore_recovery/dispatch_eligibility_test.rs"]
mod dispatch_eligibility_tests;

#[path = "restore_recovery/terminal_blocker_test.rs"]
mod terminal_blocker_tests;

#[path = "restore_recovery/prompt_custody_test.rs"]
mod prompt_custody_tests;

#[path = "restore_recovery/shutdown_drain_test.rs"]
mod shutdown_drain_tests;

#[path = "restore_recovery/recovered_admission_test.rs"]
mod recovered_admission_tests;

#[path = "restore_recovery/writer_admission_test.rs"]
mod writer_admission_tests;

#[path = "restore_recovery/restore_readback_test.rs"]
mod restore_readback_tests;

#[path = "restore_recovery/cancel_confirmation_test.rs"]
mod cancel_confirmation_tests;

#[path = "restore_recovery/refusal_answers_test.rs"]
mod refusal_answers_tests;

#[path = "restore_recovery/cursor_lost_session_test.rs"]
mod cursor_lost_session_tests;

#[path = "restore_recovery/fixture.rs"]
pub(crate) mod restore_recovery_fixture;

use super::*;
use collaboration_protocol::{
    ConversationCloseRequest, ConversationOperationShowRequest, ConversationPromptRequest,
    MessageContent, MessageDelivery,
};
use restore_recovery_fixture::{
    RestoreRig, admission_pause, policy, wait_for_queued_input, wait_request,
};
use router_clock::{ClockInstant, ManualClock, RuntimeClock as _};

#[tokio::test]
async fn host_close_without_a_wire_route_retires_the_exact_opening_as_not_enqueued() {
    use provider_session_machines::session_lifecycle_machine::SessionLifecycle;

    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host_with(&["advertise-close"]).await;
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("runtime");
    let (reached, release) = runtime.preparation_fixture.pause_restore();
    let restoring = OperationId::generate();
    host.load(rig.load_request(restoring.clone()))
        .await
        .expect("restore admitted");
    reached.await.expect("ACP prepared the exact opening");
    let owner = host.session_owner_for(&rig.target).expect("session owner");
    let mut snapshots = owner.snapshots();
    let SessionLifecycle::Loading {
        generation: opening,
        ..
    } = snapshots.borrow().lifecycle
    else {
        panic!("the original restore must still be logically loading");
    };
    let incarnation = snapshots.borrow().owner.incarnation;
    runtime.retire_provider_route_for_test(incarnation, opening);

    let closing = OperationId::generate();
    host.close(ConversationCloseRequest {
        operation_id: closing.clone(),
        target: rig.target.clone(),
        generation: Some(generation()),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
    })
    .await
    .expect("the Host accepts the logical close");
    let close_failure = host
        .wait(wait_request(closing))
        .await
        .expect_err("no physical route can send the close");
    assert_eq!(
        close_failure.kind,
        collaboration_protocol::ConversationOperationFailureKind::Unavailable
    );
    assert_eq!(
        close_failure.stage,
        collaboration_protocol::ConversationOperationFailureStage::Settlement
    );
    assert_eq!(close_failure.effect, ProviderOperationEffect::None);
    assert_eq!(
        String::from(close_failure.message),
        crate::session_close_evidence::classify_close(
            provider_session_machines::session_owner::CloseOutcome::NotEnqueued
        )
        .summary
    );
    snapshots
        .wait_for(|snapshot| snapshot.lifecycle == SessionLifecycle::NotLoaded { last: opening })
        .await
        .expect("the Host locally retires this exact generation");
    assert_eq!(rig.wire_receipt()["closes"], serde_json::json!([]));

    release
        .send(())
        .expect("release the stale Host preparation");
    let restore_failure = host
        .wait(wait_request(restoring))
        .await
        .expect_err("the original restore was closed before commit");
    assert_eq!(restore_failure.effect, ProviderOperationEffect::Applied);
    assert_eq!(
        host.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::NotLoaded
    );
    rig.assert_loads(&host).await;
    assert_eq!(rig.wire_receipt()["closes"], serde_json::json!([]));
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn a_refused_close_cannot_revive_a_restore_retired_before_commit() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig
        .open_host_with(&["restored-in-auto", "advertise-close", "hold-close-refusal"])
        .await;
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("runtime");
    let (reached, release) = runtime.preparation_fixture.pause_restore();
    let restoring = OperationId::generate();
    host.load(rig.load_request(restoring.clone()))
        .await
        .expect("restore admitted");
    reached.await.expect("ACP prepared its route");
    let closing = OperationId::generate();
    host.close(ConversationCloseRequest {
        operation_id: closing.clone(),
        target: rig.target.clone(),
        generation: Some(generation()),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
    })
    .await
    .expect("competing close admitted");
    rig.until_wire_request("closes").await;
    release
        .send(())
        .expect("release the stale Host preparation");
    let restore_failure = host
        .wait(wait_request(restoring))
        .await
        .expect_err("close retired the restore before Host commit");
    assert_eq!(restore_failure.effect, ProviderOperationEffect::Applied);
    {
        use provider_session_machines::{
            harness_lifecycle::OpeningFailure,
            session_lifecycle_machine::{LoadAttempt, RestoreVia, SessionLifecycle},
            session_owner::SessionOwnerCommand,
        };
        assert_eq!(
            OpeningFailure::Closed.valid_moves(),
            &[provider_session_machines::harness_lifecycle::LifecycleMove::WaitForClose]
        );
        let owner = host
            .session_owner_for(&rig.target)
            .expect("registered owner");
        assert!(matches!(
            owner.snapshots().borrow().lifecycle,
            SessionLifecycle::Closing { .. }
        ));
        assert!(matches!(
            owner
                .admit(SessionOwnerCommand::Restore {
                    attempt: LoadAttempt(uuid::Uuid::now_v7()),
                    via: RestoreVia::Load,
                })
                .await,
            Err(crate::session_owner_door::OwnerDoorError::Refused { .. })
        ));
        assert_eq!(
            rig.wire_receipt()["restores"]
                .as_array()
                .expect("restore calls")
                .len(),
            1,
            "the route cannot reopen until the exact close settles"
        );
    }
    node_test_fixture::release_fixture(&rig.root.path().join("wire.json"), ".release-close")
        .expect("let ACP refuse close");
    let close_failure = host
        .wait(wait_request(closing))
        .await
        .expect_err("close refused by ACP");
    assert_eq!(close_failure.effect, ProviderOperationEffect::None);
    assert!(
        matches!(
            host.owner_session_activity(&rig.target),
            crate::ProviderSessionActivity::NotLoaded
        ),
        "refused competing close reopened the failed restore"
    );
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));

    rig.assert_loads(&host).await;
    let input = OperationId::generate();
    host.prompt(rig.prompt_request(input.clone(), "one new prompt"))
        .await
        .expect("retry accepted new input");
    host.wait(wait_request(input))
        .await
        .expect("new input finished");
    assert_eq!(
        rig.wire_receipt()["prompts"],
        serde_json::json!([rig.target.session_id])
    );
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn a_public_prompt_stays_local_until_host_activation_settles() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("silent-prompts").await;
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("runtime");
    let (reached, release) = runtime.preparation_fixture.pause_restore();
    let restoring = OperationId::generate();
    host.load(rig.load_request(restoring.clone()))
        .await
        .expect("restore admitted");
    reached.await.expect("ACP prepared its route");
    let input = OperationId::generate();
    host.prompt(rig.prompt_request(input.clone(), "do not send this input"))
        .await
        .expect("public prompt admitted");
    wait_for_queued_input(&host, &rig.target, &input).await;
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
    runtime.preparation_fixture.fault_restore();
    release.send(()).expect("fail Host activation");
    let (restore_failure, refusal) = tokio::join!(
        host.wait(wait_request(restoring)),
        host.wait(wait_request(input.clone()))
    );
    assert_eq!(
        restore_failure.expect_err("restore failed").effect,
        ProviderOperationEffect::Applied
    );
    let refusal = refusal.expect_err("provisional input was refused locally");
    assert_eq!(refusal.effect, ProviderOperationEffect::None);
    assert_eq!(
        String::from(refusal.message),
        "the restore did not activate; load or resume this session and retry the input"
    );
    let stored = host
        .wait(wait_request(input))
        .await
        .expect_err("refusal was retained");
    assert_eq!(stored.effect, ProviderOperationEffect::None);
    assert_eq!(
        rig.clock.monotonic_now(),
        ClockInstant::ZERO,
        "retirement must not need a timeout"
    );
    assert!(matches!(
        host.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::NotLoaded
    ));
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn successful_host_activation_releases_the_waiting_public_prompt() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("restored-in-auto").await;
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("runtime");
    let (reached, release) = runtime.preparation_fixture.pause_restore();
    let restoring = OperationId::generate();
    host.load(rig.load_request(restoring.clone()))
        .await
        .expect("restore admitted");
    reached.await.expect("ACP prepared its route");
    let input = OperationId::generate();
    host.prompt(rig.prompt_request(input.clone(), "wait to send this input"))
        .await
        .expect("public prompt admitted");
    wait_for_queued_input(&host, &rig.target, &input).await;
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
    release.send(()).expect("publish Host activation");
    host.wait(wait_request(restoring))
        .await
        .expect("restore activated");
    let finished = host
        .wait(wait_request(input))
        .await
        .expect("prompt completed");
    assert!(matches!(
        finished.output,
        ConversationOperationWaitOutput::Available {
            settlement: ConversationOperationSettlement::PromptCompleted { .. }
        }
    ));
    assert_eq!(
        rig.wire_receipt()["prompts"],
        serde_json::json!([rig.target.session_id])
    );
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn explicit_restore_reports_recovery_guidance_and_keeps_its_stored_effect() {
    for resume in [false, true] {
        let rig = RestoreRig::new();
        rig.seed_session().await;
        let host = rig.open_host("restored-in-auto").await;
        let runtime =
            crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
                .expect("runtime");
        runtime.preparation_fixture.fault_restore();
        let id = OperationId::generate();
        let admitted = if resume {
            host.resume(rig.resume_request(id.clone())).await
        } else {
            host.load(rig.load_request(id.clone())).await
        };
        admitted.expect("restore operation admitted");
        let failure = host
            .wait(wait_request(id.clone()))
            .await
            .expect_err("restore Host preparation failed");
        assert_eq!(
            failure.kind,
            collaboration_protocol::ConversationOperationFailureKind::Unavailable
        );
        assert_eq!(failure.effect, ProviderOperationEffect::Applied);
        assert_eq!(
            String::from(failure.message),
            "retry the restore when the Router can prepare the session"
        );
        let stored = rig
            .store()
            .await
            .inspect(&id)
            .await
            .expect("read operation")
            .expect("stored restore");
        assert_eq!(stored.stage, ProviderOperationStage::Terminal);
        assert_eq!(stored.effect, ProviderOperationEffect::Applied);
        assert_eq!(
            stored.reconciliation_state,
            ProviderReconciliationState::Confirmed
        );
        assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
        host.shutdown().await.expect("stop Host");
    }
}

#[tokio::test]
async fn a_retired_load_allows_a_same_session_retry_before_and_after_store_reopen() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("silent-first-restore").await;
    let id = OperationId::generate();
    host.load(rig.load_request(id.clone()))
        .await
        .expect("first load admitted");
    let timeout = std::time::Duration::from_secs(30);
    rig.clock
        .not_due_check_now(ClockInstant::ZERO.after(timeout))
        .await;
    rig.clock
        .advance(timeout)
        .expect("reach the restore deadline");
    let failure = host
        .wait(wait_request(id.clone()))
        .await
        .expect_err("first load has no answer");
    assert_eq!(failure.effect, ProviderOperationEffect::Unknown);
    assert!(String::from(failure.message).contains("retry this session"));
    let snapshot = host
        .show(ConversationOperationShowRequest {
            operation_id: id.clone(),
        })
        .await
        .expect("first load record");
    assert_eq!(snapshot.stage, ProviderOperationStage::Terminal);
    assert_eq!(
        snapshot.reconciliation,
        ProviderReconciliationState::Unresolved
    );

    rig.assert_loads(&host).await;
    host.shutdown().await.expect("stop the first Host");
    drop(host);
    let reopened = rig.open_host("plain-prompts").await;
    rig.assert_loads(&reopened).await;
    let earlier = rig
        .store()
        .await
        .inspect(&id)
        .await
        .expect("read retired load")
        .expect("retired load retained");
    assert_eq!(earlier.effect, ProviderOperationEffect::Unknown);
    assert_eq!(
        earlier.reconciliation_state,
        ProviderReconciliationState::Unresolved
    );
    reopened.shutdown().await.expect("stop reopened Host");
}

#[tokio::test]
async fn a_disconnect_during_restore_keeps_unknown_effect_and_reconnect_guidance() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("disconnect-during-restore").await;
    let id = OperationId::generate();
    host.load(rig.load_request(id.clone()))
        .await
        .expect("load admitted");
    let failure = host
        .wait(wait_request(id.clone()))
        .await
        .expect_err("connection closed during restore");
    assert_eq!(
        provider_session_machines::harness_lifecycle::OpeningFailure::Disconnected.valid_moves(),
        &[provider_session_machines::harness_lifecycle::LifecycleMove::StopOwner]
    );
    assert_eq!(failure.effect, ProviderOperationEffect::Unknown);
    assert!(String::from(failure.message).contains("reconnect and retry this session"));
    let row = rig
        .store()
        .await
        .inspect(&id)
        .await
        .expect("operation read")
        .expect("load recorded");
    assert_eq!(row.stage, ProviderOperationStage::Terminal);
    assert_eq!(
        row.reconciliation_state,
        ProviderReconciliationState::Unresolved
    );
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn an_uncertain_prompt_still_blocks_restore_before_and_after_store_reopen() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("refuse-prompt").await;
    rig.assert_loads(&host).await;
    let id = OperationId::generate();
    host.prompt(ConversationPromptRequest {
        delivery: MessageDelivery::Auto,
        input_id: None,
        operation_id: id.clone(),
        target: rig.target.clone(),
        generation: Some(generation()),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
        prompt: MessageContent::HumanUser {
            text: MessageText::try_from("test input".to_owned()).expect("prompt text"),
        },
    })
    .await
    .expect("prompt admitted");
    let uncertainty = host
        .wait(wait_request(id.clone()))
        .await
        .expect_err("prompt effect is uncertain");
    assert_eq!(uncertainty.effect, ProviderOperationEffect::Unknown);
    assert_restore_blocked(&rig, &host, &id).await;
    host.shutdown().await.expect("stop first Host");
    drop(host);

    let reopened = rig.open_host("plain-prompts").await;
    assert_restore_blocked(&rig, &reopened, &id).await;
    reopened.shutdown().await.expect("stop reopened Host");
}

async fn assert_restore_blocked(
    rig: &RestoreRig,
    host: &ExternalProviderSupervisor,
    earlier: &OperationId,
) {
    let blocked = host
        .load(rig.load_request(OperationId::generate()))
        .await
        .expect_err("uncertain prompt must still guard admission");
    assert_eq!(blocked.kind, ConversationOperationFailureKind::Busy);
    assert!(String::from(blocked.message).contains(earlier.as_str()));
}
