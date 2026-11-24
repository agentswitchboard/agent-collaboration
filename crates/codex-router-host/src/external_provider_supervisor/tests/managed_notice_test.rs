use super::*;
use crate::ManagedClaudeNoticeAdapter;
use router_clock::{ManualClock, RuntimeClock};
use switchboard_message_custody::{
    handover::NoticeWriteOutcome,
    handover_notice::{
        ClaimedNotice, MachineId, ManagedClaudeNoticePort, NoticeAttemptId, NoticeAuthority,
        SessionRef as MailSessionRef,
    },
};

async fn idle_claude_fixture(clock: Arc<ManualClock>, expects_notice: bool) -> SteeringSession {
    use crate::provider_test_support::AcpFixtureScript;
    use collaboration_protocol::{ConversationBindingIdentity, ProviderOperationKind};
    use provider_session_machines::session_lifecycle_machine::RestoreVia;
    use serde_json::json;
    use switchboard_session_store::{ProviderOperationAdmission, ProviderSessionRecord};

    let root = tempfile::tempdir().unwrap();
    let receipt = root.path().join("wire.json");
    let mut identity = binding();
    identity.runtime.provider = collaboration_protocol::ProviderKind::ClaudeCode;
    identity.endpoint.endpoint_id =
        collaboration_protocol::EndpointId::try_from("claude-local".to_owned()).unwrap();
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
    let script = AcpFixtureScript::new()
        .expect_request("init", "initialize", json!({"protocolVersion": 1}))
        .respond(
            "init",
            json!({
                "protocolVersion": 1,
                "agentCapabilities": {"loadSession": true, "mcpCapabilities": {"http": true}},
                "agentInfo": {"name": "managed-notice-fixture", "version": "1"}
            }),
        )
        .expect_request(
            "load",
            "session/load",
            json!({"sessionId": "saved-notice-session"}),
        )
        .respond("load", json!({}))
        .record_diagnostics(receipt.clone());
    let script = if expects_notice {
        script.expect_request("notice", "session/prompt", json!({
            "sessionId": "saved-notice-session",
            "prompt": [{"type": "text", "text": "You have 1 new message. Read them with message_inbox."}]
        })).expect_message(json!({
            "jsonrpc": "2.0", "method": "session/cancel",
            "params": {"sessionId": "saved-notice-session"}
        })).respond("notice", json!({"stopReason": "cancelled"}))
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

fn prepare_notice(
    rig: &SteeringSession,
    clock: &ManualClock,
    authority: Arc<NoticeAuthority>,
) -> (ManagedClaudeNoticeAdapter, MailSessionRef, ClaimedNotice) {
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
        ManagedClaudeNoticeAdapter::new(
            rig.supervisor.clone(),
            machine,
            rig.target.endpoint.clone(),
        ),
        recipient,
        notice,
    )
}

#[tokio::test]
async fn managed_claude_notice_is_written_on_take_before_the_provider_turn_ends() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone(), true).await;
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
    assert!(matches!(
        adapter.ready(&recipient).await,
        Err(switchboard_message_custody::handover_notice::NoticeDeferred::Busy)
    ));
    rig.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn retired_custody_cannot_take_a_managed_claude_notice() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone(), false).await;
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
