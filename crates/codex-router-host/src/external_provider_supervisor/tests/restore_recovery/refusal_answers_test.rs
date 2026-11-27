use super::*;
use collaboration_contracts::SessionEventHub;
use collaboration_protocol::{ProviderSettingName, ProviderSettingsSetRequest};
use provider_session_machines::{
    session_lifecycle_answers::state_answer,
    session_lifecycle_machine::SessionLifecycle,
    session_owner::{
        OwnerEvent, PublicationStatus, RunAttempt, SessionOwnerCommand, SessionWireResult,
        WireOutcome,
    },
    session_refusal_answers::{session_busy_answer, settings_hold_answer},
    session_setting_machine::UnresolvedCause,
};

#[tokio::test]
async fn connection_loss_during_a_held_opening_refuses_queued_work_and_late_activation() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let (host, hub) = rig
        .open_host_with_fixture_arguments(
            &["plain-prompts", "silent-prompts"],
            Arc::new(acp_provider_adapters::StandardAcpBehavior),
        )
        .await;
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("provider runtime");
    let (held, release) = runtime.preparation_fixture.pause_restore();
    let restoring = OperationId::generate();
    host.load(rig.load_request(restoring.clone()))
        .await
        .expect("restore admitted");
    held.await
        .expect("provider load reached the held Host activation");
    let owner = host
        .session_owner_for(&rig.target)
        .expect("restoring owner");
    let SessionLifecycle::Loading { generation, .. } = owner.snapshots().borrow().lifecycle else {
        panic!("the old opening must still be held");
    };
    let queued = OperationId::generate();
    host.prompt(rig.prompt_request(queued.clone(), "queued during held opening"))
        .await
        .expect("the held opening admits queued work");
    wait_for_queued_input(&host, &rig.target, &queued).await;

    runtime.retirement().cancel();
    let (restore, queued) = tokio::join!(
        host.wait(wait_request(restoring)),
        host.wait(wait_request(queued))
    );
    assert_eq!(
        restore
            .expect_err("the lost opening did not activate")
            .effect,
        ProviderOperationEffect::Applied
    );
    assert_eq!(
        queued.expect_err("the queued prompt did not send").effect,
        ProviderOperationEffect::None
    );
    let queued_completion = runtime
        .preparation_fixture
        .observe_restore_completion_queued();
    release
        .send(())
        .expect("release the old activation completion");
    queued_completion
        .await
        .expect("the old HostReady completion was enqueued");
    // The owner consumes queued completions before admitting this command.
    let barrier = owner
        .admit(SessionOwnerCommand::AcceptSettings { generation })
        .await
        .expect_err("the old activation did not reopen the unloaded session");
    assert!(
        matches!(
            barrier,
            crate::session_owner_door::OwnerDoorError::Refused { .. },
        ),
        "the owner processed the command after the queued completion"
    );
    assert_lost_session_is_retained(&rig, &hub, &owner).await;
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn retired_acp_connection_refuses_host_ready_before_the_owner_consumes_loss() {
    use provider_session_machines::session_owner::{
        PreparationStage, RemoteEffect, RestoreDisposition, RestoreFailure,
    };
    use session_event_model::{SessionEvent, SessionState};
    use std::num::NonZeroUsize;

    let rig = RestoreRig::new();
    rig.seed_session().await;
    let (host, hub) = rig
        .open_host_with_fixture_arguments(
            &["plain-prompts"],
            Arc::new(acp_provider_adapters::StandardAcpBehavior),
        )
        .await;
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("provider runtime");
    let owner = host
        .session_owner_for(&rig.target)
        .expect("registered owner");
    let board = collaboration_protocol::ProviderIdentity::session_to_board(&rig.target)
        .expect("board session");
    let mut publication = hub
        .owner_inbox(board.clone(), NonZeroUsize::MIN)
        .0
        .capacity_returned();
    let (prepared, release_prepared) = runtime.preparation_fixture.pause_restore();
    let (loss_reached, release_loss) = runtime.preparation_fixture.pause_harness_loss();
    let restoring = OperationId::generate();
    host.load(rig.load_request(restoring.clone()))
        .await
        .expect("restore admitted");
    prepared
        .await
        .expect("real ACP opening and successful HostReady are held");
    publication
        .changed()
        .await
        .expect("initial rebase applied before attaching");
    let mut history = hub.attach(board).await.expect("pre-activation history");
    assert!(!history.snapshot.iter().any(|event| matches!(
        event.event,
        SessionEvent::StateChanged {
            state: SessionState::Idle
        }
    )));
    let SessionLifecycle::Loading {
        generation,
        attempt,
        ..
    } = owner.snapshots().borrow().lifecycle
    else {
        panic!("the exact held opening must still be loading");
    };
    let run = RunAttempt {
        generation,
        attempt,
    };
    assert!(matches!(
        owner
            .admit(SessionOwnerCommand::AcceptSettings { generation })
            .await,
        Err(crate::session_owner_door::OwnerDoorError::Refused { .. })
    ));
    let token = runtime.retirement();
    token.cancel();
    loss_reached
        .await
        .expect("the real connection-loss report is held before its mailbox send");
    assert!(token.is_cancelled());
    assert!(matches!(owner.snapshots().borrow().lifecycle,
        SessionLifecycle::Loading { generation: loading, attempt: pending, .. }
            if loading == generation && pending == attempt));
    let queued = runtime
        .preparation_fixture
        .observe_restore_completion_queued();
    release_prepared
        .send(())
        .expect("offer the held HostReady before loss");
    queued.await.expect("actual reserved HostReady queued");

    let failure = host.wait(wait_request(restoring)).await.expect_err(
        "known-retired ACP connection must not settle restore successfully before loss",
    );
    assert_eq!(failure.effect, ProviderOperationEffect::Applied);
    let snapshot = owner.snapshots().borrow().clone();
    let result = snapshot
        .last_restore
        .expect("the owner consumed activation and recorded its result");
    assert_eq!(result.attempt, run);
    assert_eq!(
        result.disposition,
        RestoreDisposition::Retired {
            failure: RestoreFailure::LocalPrerequisite {
                stage: PreparationStage::Route
            },
            remote: RemoteEffect::Applied,
        }
    );
    assert!(
        matches!(
            history.receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "known-retired activation must publish no transient Live or replay batch"
    );
    assert_lost_session_is_retained(&rig, &hub, &owner).await;

    release_loss
        .send(())
        .expect("now let the ordinary owner-wide loss proceed");
    host.shutdown().await.expect("stop Host");
}

#[tokio::test]
async fn connection_loss_after_close_is_offered_keeps_the_outcome_unknown() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let (host, hub) = rig
        .open_host_with_fixture_arguments(
            &["plain-prompts", "advertise-close", "hold-close-refusal"],
            Arc::new(acp_provider_adapters::StandardAcpBehavior),
        )
        .await;
    rig.assert_loads(&host).await;
    let owner = host.session_owner_for(&rig.target).expect("live owner");
    let runtime = crate::provider_test_support::fixture_runtime_for(&host, &rig.target.endpoint)
        .expect("provider runtime");
    let close = OperationId::generate();
    host.close(ConversationCloseRequest {
        operation_id: close.clone(),
        target: rig.target.clone(),
        generation: Some(generation()),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
    })
    .await
    .expect("close admitted");
    rig.until_wire_request("closes").await;
    let SessionLifecycle::Closing {
        generation,
        attempt,
        ..
    } = owner.snapshots().borrow().lifecycle
    else {
        panic!("the provider received a close but has not replied");
    };
    let run = RunAttempt {
        generation,
        attempt,
    };
    runtime.retirement().cancel();
    let failure = host
        .wait(wait_request(close.clone()))
        .await
        .expect_err("the taken close is unconfirmed");
    assert_eq!(
        failure.kind,
        ConversationOperationFailureKind::OutcomeUnknown
    );
    assert_eq!(failure.effect, ProviderOperationEffect::Unknown);

    owner
        .report_event(OwnerEvent::Wire(SessionWireResult::Close {
            run,
            outcome: WireOutcome::Confirmed(()),
        }))
        .expect("deliver the late confirmation to its original owner and attempt");
    owner
        .admit(SessionOwnerCommand::AcceptSettings { generation })
        .await
        .expect_err("the retired attempt is no longer live");
    assert_lost_session_is_retained(&rig, &hub, &owner).await;
    let durable = rig
        .store()
        .await
        .inspect(&close)
        .await
        .expect("inspect close")
        .expect("close record");
    assert_eq!(durable.effect, ProviderOperationEffect::Unknown);
    assert_eq!(
        rig.wire_receipt()["closes"],
        serde_json::json!([rig.target.session_id])
    );
    host.shutdown().await.expect("stop Host");
}

async fn assert_lost_session_is_retained(
    rig: &RestoreRig,
    hub: &switchboard_session_events::ProviderSessionEventHub,
    owner: &crate::session_owner_door::SessionOwnerAddress,
) {
    let snapshot = owner.snapshots().borrow().clone();
    assert!(matches!(
        snapshot.lifecycle,
        SessionLifecycle::NotLoaded { .. }
    ));
    assert_eq!(snapshot.publication, PublicationStatus::Current);
    assert_eq!(
        snapshot.projection.state,
        session_event_model::SessionState::Unloaded
    );
    let target = collaboration_protocol::ProviderIdentity::session_to_board(&rig.target)
        .expect("session identity for the hub");
    assert_eq!(
        hub.state(target).await.expect("existing session"),
        session_event_model::SessionState::Unloaded
    );
    assert!(
        rig.store()
            .await
            .session_record(&rig.target)
            .await
            .expect("read session record")
            .is_some()
    );
}

fn mode_request(rig: &RestoreRig) -> ProviderSettingsSetRequest {
    ProviderSettingsSetRequest {
        target: rig.target.clone(),
        actor: rig.target.clone().into(),
        setting: ProviderSettingName::Mode,
        value: "plan".into(),
    }
}

async fn lifecycle_setting_refusal(
    rig: &RestoreRig,
    host: &ExternalProviderSupervisor,
    exact: &str,
) {
    let snapshot = host
        .owner_session_snapshot(&rig.target)
        .expect("owner snapshot");
    let failure = host
        .settings_set(mode_request(rig))
        .await
        .expect_err("setting is refused by lifecycle");
    assert_eq!(failure.message, exact);
    assert_eq!(failure.message, state_answer(&snapshot.lifecycle));
}

#[tokio::test]
async fn not_loaded_and_loading_settings_use_the_owner_lifecycle_answer() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("silent-first-restore").await;
    // Discover the saved session's owner without sending anything to the agent.
    let _owner = host
        .session_owner_for(&rig.target)
        .expect("saved session owner");
    lifecycle_setting_refusal(&rig, &host, "session is not loaded; load it or resume it").await;
    let load = OperationId::generate();
    host.load(rig.load_request(load.clone()))
        .await
        .expect("load admitted");
    rig.until_wire_request("restores").await;
    lifecycle_setting_refusal(&rig, &host, "session is loading; wait for it or close it").await;
    let deadline = rig
        .clock
        .monotonic_now()
        .after(std::time::Duration::from_secs(30));
    rig.clock.not_due_check_now(deadline).await;
    rig.clock
        .advance(std::time::Duration::from_secs(30))
        .expect("retire the silent load");
    let _retired = host.wait(wait_request(load)).await;
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn closing_settings_use_the_owner_lifecycle_answer() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig
        .open_host_with(&["advertise-close", "hold-close-refusal"])
        .await;
    rig.assert_loads(&host).await;
    let close = OperationId::generate();
    host.close(ConversationCloseRequest {
        operation_id: close.clone(),
        target: rig.target.clone(),
        generation: Some(generation()),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
    })
    .await
    .expect("close admitted");
    rig.until_wire_request("closes").await;
    let snapshot = host
        .owner_session_snapshot(&rig.target)
        .expect("closing snapshot");
    assert!(matches!(
        snapshot.lifecycle,
        SessionLifecycle::Closing { .. }
    ));
    lifecycle_setting_refusal(&rig, &host, "session is closing; wait for it").await;
    node_test_fixture::release_fixture(&rig.root.path().join("wire.json"), ".release-close")
        .expect("release close");
    let _settled = host.wait(wait_request(close)).await;
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn busy_without_steering_and_explicit_steer_derive_the_running_answer() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("silent-prompts").await;
    rig.assert_loads(&host).await;
    host.prompt(rig.prompt_request(OperationId::generate(), "held foreground"))
        .await
        .expect("prompt admitted");
    rig.until_wire_request("prompts").await;
    let snapshot = host
        .owner_session_snapshot(&rig.target)
        .expect("running snapshot");
    let busy = session_busy_answer(&snapshot.lifecycle, snapshot.turn, false);

    let setting = host
        .settings_set(mode_request(&rig))
        .await
        .expect_err("prompt in progress blocks a setting");
    assert_eq!(
        setting.message,
        "a turn is running; queue the message or cancel the turn"
    );
    assert_eq!(setting.message, busy);

    let steer = OperationId::generate();
    let mut request = rig.prompt_request(steer.clone(), "explicit steer");
    request.delivery = MessageDelivery::Steer;
    let failure = match host.prompt(request).await {
        Ok(_) => host
            .wait(wait_request(steer))
            .await
            .expect_err("explicit steer is refused"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.kind,
        ConversationOperationFailureKind::UnsupportedCapability
    );
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    let answer = String::from(failure.message);
    assert_eq!(
        answer,
        "this harness can't take a steer mid-turn; a turn is running; queue the message or cancel the turn"
    );
    assert_eq!(
        answer.strip_prefix("this harness can't take a steer mid-turn; "),
        Some(busy.as_str())
    );
    host.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_prompt_on_a_saved_unloaded_session_keeps_its_kind_and_recovery_guidance() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("plain-prompts").await;
    let refusal = host
        .prompt(rig.prompt_request(OperationId::generate(), "prompt while unloaded"))
        .await
        .expect_err("the explicit prompt never loads the session");
    assert_eq!(refusal.kind, ConversationOperationFailureKind::Unavailable);
    assert_eq!(refusal.effect, ProviderOperationEffect::None);
    assert_eq!(
        String::from(refusal.message),
        "session is not loaded; load it or resume it"
    );
    assert!(!rig.root.path().join("wire.json").exists());
    host.shutdown().await.expect("fixture shutdown");
}

#[tokio::test]
async fn an_unknown_setting_uses_its_derived_prompt_hold_answer() {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("malformed-mode-reply").await;
    rig.assert_loads(&host).await;
    let setting = host
        .settings_set(mode_request(&rig))
        .await
        .expect_err("mode result is unknown");
    assert_eq!(
        setting.kind,
        collaboration_protocol::ProviderSettingsFailureKind::OutcomeUnknown
    );
    let snapshot = host
        .owner_session_snapshot(&rig.target)
        .expect("held snapshot");
    assert!(snapshot.settings_block_input);
    let prompt = OperationId::generate();
    let failure = match host
        .prompt(rig.prompt_request(prompt.clone(), "prompt while held"))
        .await
    {
        Ok(_) => host
            .wait(wait_request(prompt))
            .await
            .expect_err("setting holds input"),
        Err(failure) => failure,
    };
    assert_eq!(
        failure.kind,
        ConversationOperationFailureKind::SettingsUnresolved
    );
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    let answer = String::from(failure.message);
    assert_eq!(
        answer,
        "a setting change's outcome is unknown; set a value explicitly"
    );
    assert_eq!(
        answer,
        settings_hold_answer(UnresolvedCause::OutcomeUnknown)
    );
    host.shutdown().await.expect("shutdown");
}
