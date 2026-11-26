use super::*;

#[path = "notice_admission_test.rs"]
mod admission;
#[cfg(debug_assertions)]
#[path = "notice_handover_fence_test.rs"]
mod handover_fence;
#[cfg(debug_assertions)]
#[path = "setting_work_interest_test.rs"]
mod setting_work_interest;
#[path = "notice_started_turn_test.rs"]
mod started_turn;
#[cfg(debug_assertions)]
#[path = "steer_work_receipt_test.rs"]
mod steer_work_receipt;
#[cfg(debug_assertions)]
#[path = "work_offer_admission_test.rs"]
mod work_offer_admission;
use crate::ManagedAcpNoticeAdapter;
use router_clock::{ManualClock, RuntimeClock};
use switchboard_message_custody::{
    handover::NoticeWriteOutcome,
    handover_notice::{
        ClaimedNotice, MachineId, ManagedNoticePort, NoticeAttemptId, NoticeAuthority,
        SessionRef as MailSessionRef,
    },
};

const NOTICE_TEXT: &str = "You have 1 new message. Read them with message_inbox.";
const HUMAN_TEXT: &str = "human prompt";

#[derive(Clone, Copy)]
enum NoticeScript {
    NoWrite,
    SettingWork,
    HoldCancelTerminal,
    #[cfg(debug_assertions)]
    OrdinarySteer(steer_work_receipt::SteerPeerAction),
    Direct,
    DirectWithLoggedOutReport,
    DirectThenSteer,
    HumanThenNoticeSteer,
    HumanWithQueuedPrompt,
    DirectThenProviderDeclinesSteer,
    DirectThenProviderStartsTurn,
    DirectWithSteeringAvailable,
}

async fn managed_notice_fixture(
    clock: Arc<ManualClock>,
    notices: NoticeScript,
    provider: collaboration_protocol::ProviderKind,
) -> SteeringSession {
    use crate::provider_test_support::AcpFixtureScript;
    use collaboration_protocol::{ConversationBindingIdentity, ProviderOperationKind};
    use provider_session_machines::session_lifecycle_machine::RestoreVia;
    use serde_json::json;
    use switchboard_session_store::{ProviderOperationAdmission, ProviderSessionRecord};

    let root = tempfile::tempdir().unwrap();
    let receipt = root.path().join("wire.json");
    let mut identity = binding();
    identity.runtime.provider = provider;
    let endpoint_name = match provider {
        collaboration_protocol::ProviderKind::ClaudeCode => "claude-local",
        collaboration_protocol::ProviderKind::Cursor => "cursor-local",
    };
    identity.endpoint.endpoint_id =
        collaboration_protocol::EndpointId::try_from(endpoint_name.to_owned()).unwrap();
    let target = SessionRef {
        endpoint: identity.endpoint.clone(),
        session_id: SessionId::try_from("saved-notice-session".to_owned()).unwrap(),
    };
    let mut store = ProviderOperationStore::open(&root.path().join("operations.sqlite"))
        .await
        .unwrap();
    let record = ProviderSessionRecord {
        target: target.clone(),
        working_directory: ProviderWorkingDirectory::try_from(root.path().display().to_string())
            .unwrap(),
        requested_policy: ProviderRequestedPolicy {
            access: RouterAccess::WriteRestricted,
        },
        created_by: target.clone().into(),
        approver: target.clone().into(),
        updated_at_ms: 0,
        resume: Default::default(),
    };
    let operation = OperationId::generate();
    store.record_session(&record).await.unwrap();
    store
        .admit(ProviderOperationAdmission {
            operation_id: operation.clone(),
            operation_kind: ProviderOperationKind::ConversationLoad,
            binding: ConversationBindingIdentity::ExternalProvider {
                binding: identity.clone(),
            },
            admitted_at_ms: 0,
        })
        .await
        .unwrap();
    store.record_target(&operation, &target, 0).await.unwrap();
    store.mark_may_have_dispatched(&operation, 0).await.unwrap();
    let script =
        AcpFixtureScript::new().expect_request("init", "initialize", json!({"protocolVersion": 1}));
    let script = if matches!(notices, NoticeScript::DirectWithLoggedOutReport) {
        script.send(json!({
            "jsonrpc": "2.0",
            "method": "_auth/status_update",
            "params": {"authStatus": {"kind": "none", "label": "Signed out"}}
        }))
    } else {
        script
    };
    let setting_catalogue = |value: &str| {
        json!({"configOptions": [{
            "id": "model", "name": "Model", "category": "model", "type": "select",
            "currentValue": value,
            "options": [{"value":"old","name":"Old"},{"value":"new","name":"New"}]
        }]})
    };
    let loaded = if matches!(notices, NoticeScript::SettingWork) {
        setting_catalogue("old")
    } else {
        json!({})
    };
    let steering = matches!(
        notices,
        NoticeScript::DirectThenSteer
            | NoticeScript::HumanThenNoticeSteer
            | NoticeScript::HumanWithQueuedPrompt
            | NoticeScript::DirectThenProviderDeclinesSteer
            | NoticeScript::DirectThenProviderStartsTurn
            | NoticeScript::DirectWithSteeringAvailable
    );
    #[cfg(debug_assertions)]
    let steering = steering || matches!(notices, NoticeScript::OrdinarySteer(_));
    let script = script
        .respond(
            "init",
            json!({
                "protocolVersion": 1,
                "agentCapabilities": {"loadSession": true, "mcpCapabilities": {"http": true}},
                "agentInfo": {"name": "managed-notice-fixture", "version": "1"},
                "_meta": {"steering": {"supported": steering}}
            }),
        )
        .expect_request(
            "load",
            "session/load",
            json!({"sessionId": "saved-notice-session"}),
        )
        .respond("load", loaded)
        .record_diagnostics(receipt.clone());
    #[cfg(debug_assertions)]
    let (script, notices) = match notices {
        NoticeScript::OrdinarySteer(action) => (
            steer_work_receipt::steer_script(script, &receipt, action),
            NoticeScript::NoWrite,
        ),
        _ => (script, notices),
    };
    let script = if matches!(notices, NoticeScript::SettingWork) {
        script
            .expect_terminal_request(
                "setter",
                "session/set_config_option",
                json!({"sessionId": "saved-notice-session", "configId":"model", "value":"new"}),
            )
            .write_marker(&receipt.with_extension("setting"))
            .respond("setter", setting_catalogue("new"))
    } else if !matches!(notices, NoticeScript::NoWrite) {
        let foreground = match notices {
            NoticeScript::DirectThenProviderStartsTurn => "Hold the foreground turn",
            NoticeScript::HumanThenNoticeSteer | NoticeScript::HumanWithQueuedPrompt => HUMAN_TEXT,
            _ => NOTICE_TEXT,
        };
        let script = script
            .expect_request(
                "notice",
                "session/prompt",
                json!({
                    "sessionId": "saved-notice-session",
                    "prompt": [{"type": "text", "text": foreground}]
                }),
            )
            .write_marker(&receipt.with_extension("prompt"));
        if matches!(notices, NoticeScript::DirectThenProviderDeclinesSteer) {
            script
                .expect_request(
                    "steer",
                    "_session/steering",
                    json!({"sessionId": "saved-notice-session"}),
                )
                .write_marker(&receipt.with_extension("steer"))
                .respond("steer", json!({"outcome":"promptRequired"}))
                .respond("notice", json!({"stopReason": "end_turn"}))
                // A fallback build also completes this request. The test checks
                // its marker before advancing the clock and offering the retry.
                .expect_request(
                    "retry",
                    "session/prompt",
                    json!({
                        "sessionId": "saved-notice-session",
                        "prompt": [{"type": "text", "text": NOTICE_TEXT}]
                    }),
                )
                .write_marker(&receipt.with_extension("retry"))
                .respond("retry", json!({"stopReason": "end_turn"}))
        } else if matches!(notices, NoticeScript::DirectThenProviderStartsTurn) {
            script
                .expect_recorded_request(
                    "steer",
                    "_session/steering",
                    json!({
                        "sessionId": "saved-notice-session",
                        "prompt": [{"type": "text", "text": NOTICE_TEXT}]
                    }),
                    &receipt.with_extension("traffic"),
                )
                .respond("notice", json!({"stopReason": "end_turn"}))
                .wait_for_socket_signal(&receipt.with_extension("sock"))
                .respond("steer", json!({"outcome": "startedNewTurn"}))
        } else {
            let script = if matches!(
                notices,
                NoticeScript::DirectThenSteer
                    | NoticeScript::HumanThenNoticeSteer
                    | NoticeScript::HumanWithQueuedPrompt
            ) {
                script
                    .expect_request(
                        "steer",
                        "_session/steering",
                        json!({
                            "sessionId": "saved-notice-session",
                            "prompt": [{"type": "text", "text": NOTICE_TEXT}]
                        }),
                    )
                    .write_marker(&receipt.with_extension("steer"))
                    .respond("steer", json!({"outcome": "injected"}))
            } else {
                script
            };
            let script = script.expect_message(json!({
                "jsonrpc": "2.0", "method": "session/cancel",
                "params": {"sessionId": "saved-notice-session"}
            }));
            let script = if matches!(notices, NoticeScript::HoldCancelTerminal) {
                script.wait_for_socket_signal(&receipt.with_extension("cancel.sock"))
            } else {
                script
            };
            let script = script.respond("notice", json!({"stopReason": "cancelled"}));
            if matches!(notices, NoticeScript::HumanWithQueuedPrompt) {
                admission::expect_queued_prompt(script, &receipt)
            } else {
                script
            }
        }
    } else {
        script
    };
    let (supervisor, hub) = crate::provider_test_support::bind_scripted_provider(
        identity,
        script.launch(),
        Arc::new(Mutex::new(store)),
        Arc::new(acp_provider_adapters::StandardAcpBehavior),
        clock,
    )
    .await
    .unwrap();
    let supervisor = Arc::new(supervisor);
    let owner = supervisor.session_owner_for(&target).unwrap();
    let restored = owner
        .restore_explicit(
            operation.as_str().parse().unwrap(),
            RestoreVia::Load,
            Box::new(record),
        )
        .await
        .unwrap();
    assert_eq!(
        supervisor.owner_session_activity(&target),
        crate::ProviderSessionActivity::Idle,
        "{:?}; wire receipt {:?}",
        restored.reply,
        std::fs::read_to_string(&receipt)
    );
    let hub_target = serde_json::from_value(serde_json::to_value(&target).unwrap()).unwrap();
    SteeringSession {
        _root: root,
        receipt,
        target,
        hub,
        hub_target,
        supervisor,
    }
}

#[tokio::test]
async fn managed_notice_provider_prompt_required_leaves_direct_delivery_for_the_next_offer() {
    use provider_session_machines::session_turn_machine::SessionTurn;

    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectThenProviderDeclinesSteer,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, initial) = prepare_notice(&rig, &clock, authority.clone());
    assert!(matches!(
        adapter
            .ready(&recipient)
            .await
            .unwrap()
            .offer(initial)
            .await,
        NoticeWriteOutcome::Written { .. }
    ));
    let exact = adapter.ready(&recipient).await.unwrap();
    assert_eq!(
        exact
            .offer(second_notice(&recipient, &clock, authority.clone()))
            .await,
        NoticeWriteOutcome::SteerNotInjected
    );
    let owner = rig.supervisor.session_owner_for(&rig.target).unwrap();
    let mut snapshots = owner.snapshots();
    snapshots
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .unwrap();
    assert!(rig.receipt.with_extension("steer").is_file());
    assert!(!rig.receipt.with_extension("retry").exists());

    clock.advance(std::time::Duration::from_secs(60)).unwrap();
    let retry_attempt = NoticeAttemptId::mint(clock.utc_now());
    let retry = ClaimedNotice::new(
        recipient.clone(),
        retry_attempt,
        vec!["019f0000-0000-7000-8000-000000000018".parse().unwrap()],
        clock
            .monotonic_now()
            .after(std::time::Duration::from_secs(1)),
        clock.utc_now() + chrono::TimeDelta::seconds(1),
        authority,
    )
    .unwrap();
    let writer = adapter.ready(&recipient).await.unwrap();
    assert_eq!(
        writer.offer(retry).await,
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );
    let retry_input = session_event_model::InputId::new(String::from(retry_attempt)).unwrap();
    snapshots
        .wait_for(|snapshot| {
            snapshot
                .last_live_turn
                .as_ref()
                .is_some_and(|turn| turn.input == retry_input)
        })
        .await
        .unwrap();
    assert!(rig.receipt.with_extension("retry").is_file());
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, true);
}

fn prepare_notice(
    rig: &SteeringSession,
    clock: &ManualClock,
    authority: Arc<NoticeAuthority>,
) -> (ManagedAcpNoticeAdapter, MailSessionRef, ClaimedNotice) {
    let machine: MachineId = "019f0000-0000-7000-8000-000000000011".parse().unwrap();
    let recipient = MailSessionRef {
        machine_id: machine.clone(),
        session_id: String::from(rig.target.session_id.clone()).parse().unwrap(),
    };
    let notice = ClaimedNotice::new(
        recipient.clone(),
        NoticeAttemptId::try_from("019f0000-0000-7000-8000-000000000015".to_owned()).unwrap(),
        vec!["019f0000-0000-7000-8000-000000000017".parse().unwrap()],
        clock
            .monotonic_now()
            .after(std::time::Duration::from_secs(1)),
        clock.utc_now() + chrono::TimeDelta::seconds(1),
        authority,
    )
    .unwrap();
    (
        ManagedAcpNoticeAdapter::new(rig.supervisor.clone(), machine),
        recipient,
        notice,
    )
}

fn second_notice(
    recipient: &MailSessionRef,
    clock: &ManualClock,
    authority: Arc<NoticeAuthority>,
) -> ClaimedNotice {
    ClaimedNotice::new(
        recipient.clone(),
        "019f0000-0000-7000-8000-000000000016"
            .to_owned()
            .try_into()
            .unwrap(),
        vec!["019f0000-0000-7000-8000-000000000018".parse().unwrap()],
        clock
            .monotonic_now()
            .after(std::time::Duration::from_secs(1)),
        clock.utc_now() + chrono::TimeDelta::seconds(1),
        authority,
    )
    .unwrap()
}

#[tokio::test]
async fn managed_claude_notice_is_written_on_take_before_the_provider_turn_ends() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::Direct,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));

    let ready = adapter.ready(&recipient).await.unwrap();
    assert_eq!(
        ready.offer(notice).await,
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Running
    );
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, false);
}

#[tokio::test]
async fn idle_managed_claude_gets_notice_with_logged_out_connection_report() {
    use session_event_model::{ProviderAuthStatus, SessionState};

    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectWithLoggedOutReport,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let snapshot = rig.supervisor.owner_session_snapshot(&rig.target).unwrap();
    assert_eq!(snapshot.projection.state, SessionState::Idle);
    assert_eq!(
        snapshot.projection.capabilities.unwrap().auth_status,
        ProviderAuthStatus::LoggedOut
    );
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));

    let writer = adapter
        .ready(&recipient)
        .await
        .expect("an idle session is ready even when its connection reports LoggedOut");
    assert_eq!(
        writer.offer(notice).await,
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, false);
}

#[tokio::test]
async fn notice_for_running_claude_uses_the_exact_turn_and_preserves_durable_writer_take() {
    use provider_session_machines::session_turn_machine::SessionTurn;
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectThenSteer,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, notice) = prepare_notice(&rig, &clock, authority.clone());
    let first = adapter.ready(&recipient).await.unwrap().offer(notice).await;
    assert!(matches!(first, NoticeWriteOutcome::Written { .. }));
    let snapshot = rig.supervisor.owner_session_snapshot(&rig.target).unwrap();
    assert!(matches!(snapshot.turn, SessionTurn::Running { .. }));
    let next = second_notice(&recipient, &clock, authority);
    let writer = adapter
        .ready(&recipient)
        .await
        .expect("running Claude must take the notice by steering");
    assert_eq!(
        writer.offer(next).await,
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );
    assert_eq!(
        rig.supervisor
            .owner_session_snapshot(&rig.target)
            .unwrap()
            .turn,
        snapshot.turn
    );
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, true);
}

#[tokio::test]
async fn managed_notice_running_cursor_without_steering_is_definitely_unsent_and_never_queued() {
    use switchboard_message_custody::handover::NoticePreTakeRefusal;

    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::Direct,
        collaboration_protocol::ProviderKind::Cursor,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, first) = prepare_notice(&rig, &clock, authority.clone());
    assert!(matches!(
        adapter.ready(&recipient).await.unwrap().offer(first).await,
        NoticeWriteOutcome::Written { .. }
    ));

    let active = rig.supervisor.owner_session_snapshot(&rig.target).unwrap();
    let second = second_notice(&recipient, &clock, authority);
    assert_eq!(
        adapter.ready(&recipient).await.unwrap().offer(second).await,
        NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::CannotSteer)
    );
    let snapshot = rig.supervisor.owner_session_snapshot(&rig.target).unwrap();
    assert_eq!(snapshot.turn, active.turn);
    assert_eq!(snapshot.queue, active.queue);
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, false);
}

#[tokio::test]
async fn retired_custody_cannot_take_a_managed_claude_notice() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::NoWrite,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, notice) = prepare_notice(&rig, &clock, authority.clone());
    let ready = adapter.ready(&recipient).await.unwrap();
    authority.close();

    assert!(matches!(
        ready.offer(notice).await,
        NoticeWriteOutcome::DefinitelyNotWritten(_)
    ));
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Idle
    );
    rig.supervisor.shutdown().await.unwrap();
}

async fn finish_notice_turn(rig: &SteeringSession) {
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle, session_turn_machine::SessionTurn,
    };
    let owner = rig.supervisor.session_owner_for(&rig.target).unwrap();
    let mut snapshots = owner.snapshots();
    let snapshot = snapshots.borrow().clone();
    let (SessionLifecycle::Live { generation }, SessionTurn::Running { turn }) =
        (snapshot.lifecycle, snapshot.turn)
    else {
        panic!("expected a live foreground turn");
    };
    owner.cancel_turn(generation, turn).await.unwrap().unwrap();
    snapshots
        .wait_for(|state| state.turn == SessionTurn::Idle)
        .await
        .unwrap();
}

#[tokio::test]
async fn peer_cancel_does_not_end_the_turn_until_its_separate_terminal_fact() {
    use provider_session_machines::{
        session_lifecycle_machine::SessionLifecycle, session_owner::CancelDrainReport,
        session_turn_machine::SessionTurn,
    };
    use session_event_model::{StopReason, TurnOutcome};

    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        Arc::clone(&clock),
        NoticeScript::HoldCancelTerminal,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let terminal = tokio::net::UnixListener::bind(rig.receipt.with_extension("cancel.sock"))
        .expect("peer terminal release socket");
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    assert!(matches!(
        adapter
            .ready(&recipient)
            .await
            .expect("ready")
            .offer(notice)
            .await,
        NoticeWriteOutcome::Written { .. }
    ));
    let owner = rig
        .supervisor
        .session_owner_for(&rig.target)
        .expect("active owner");
    let mut snapshots = owner.snapshots();
    let running = snapshots.borrow().clone();
    let (SessionLifecycle::Live { generation }, SessionTurn::Running { turn }) =
        (running.lifecycle, running.turn)
    else {
        panic!("foreground was not running");
    };

    assert_eq!(
        owner
            .cancel_turn(generation, turn)
            .await
            .expect("cancel dispatched")
            .expect("cancel receipt"),
        CancelDrainReport::Drained
    );
    let (peer, _) = terminal.accept().await.expect("peer saw session/cancel");
    assert_eq!(snapshots.borrow().turn, SessionTurn::Cancelling { turn });
    assert!(snapshots.borrow().last_live_turn.is_none());
    drop(peer);

    let ended = snapshots
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .expect("peer terminal fact reduced");
    assert!(matches!(
        ended.last_live_turn.as_ref(),
        Some(observed)
            if observed.turn == turn
                && matches!(observed.outcome, TurnOutcome::Ended { stop_reason: StopReason::Cancelled, .. })
    ));
    drop(ended);
    rig.supervisor.shutdown().await.expect("fixture shutdown");
    assert_wire_script_completed(&rig, false);
}

fn assert_wire_script_completed(rig: &SteeringSession, expects_steer: bool) {
    assert!(rig.receipt.with_extension("prompt").is_file());
    assert_eq!(rig.receipt.with_extension("steer").is_file(), expects_steer);
    assert!(
        !rig.receipt.exists(),
        "unexpected wire traffic: {:?}",
        std::fs::read_to_string(&rig.receipt)
    );
}

#[tokio::test]
async fn managed_notice_cannot_replace_a_captured_turn_that_ends_before_take() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectWithSteeringAvailable,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, initial) = prepare_notice(&rig, &clock, authority.clone());
    assert!(matches!(
        adapter
            .ready(&recipient)
            .await
            .unwrap()
            .offer(initial)
            .await,
        NoticeWriteOutcome::Written { .. }
    ));
    let captured = adapter.ready(&recipient).await.unwrap();
    finish_notice_turn(&rig).await;

    let outcome = captured
        .offer(second_notice(&recipient, &clock, authority))
        .await;
    assert!(matches!(
        outcome,
        NoticeWriteOutcome::DefinitelyNotWritten(_)
    ));
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Idle
    );
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, false);
}

#[tokio::test]
async fn managed_notice_steer_respects_the_retired_custody_fence() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectWithSteeringAvailable,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let (adapter, recipient, initial) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    assert!(matches!(
        adapter
            .ready(&recipient)
            .await
            .unwrap()
            .offer(initial)
            .await,
        NoticeWriteOutcome::Written { .. }
    ));
    let captured = adapter.ready(&recipient).await.unwrap();
    let retired = Arc::new(NoticeAuthority::new());
    let notice = second_notice(&recipient, &clock, retired.clone());
    retired.close();

    assert!(matches!(
        captured.offer(notice).await,
        NoticeWriteOutcome::DefinitelyNotWritten(_)
    ));
    finish_notice_turn(&rig).await;
    rig.supervisor.shutdown().await.unwrap();
    assert_wire_script_completed(&rig, false);
}
