use super::*;
use crate::ManagedClaudeNoticeAdapter;
use crate::external_provider_supervisor::{AdmissionTestPause, PromptBusyPolicy};
use collaboration_protocol::MessageDelivery as ProviderMessageDelivery;
use router_clock::{ManualClock, RuntimeClock};
use runtime_primitives::Clock as _;
use state_machine_core::StateMachine as _;
use switchboard_machine_core_store::{
    DirectHandoff, DirectHandoffChange, HandoffCommit, InboxDecision, InboxOfferExpiry, InboxStore,
    MachineLocalStore,
};
use switchboard_message_custody::{
    flush_marks::next_mark_after,
    handover::{
        DirectHandoffMachine, Handoff, HandoffEffect, HandoffEvent, NoticePreTakeRefusal,
        NoticeWriteOutcome,
    },
    handover_notice::{
        ClaimedNotice, MachineId, ManagedClaudeNoticePort, NoticeAttemptId, NoticeAuthority,
        SessionRef as MailSessionRef,
    },
};

const NOTICE_TEXT: &str = "You have 1 new message. Read them with message_inbox.";
const HUMAN_TEXT: &str = "human prompt";
const NOTICE_MESSAGE_ID: &str = "019f0000-0000-7000-8000-000000000017";

async fn idle_claude_fixture(
    clock: Arc<ManualClock>,
    expected_prompt: Option<&str>,
) -> SteeringSession {
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
    let script = if let Some(expected_prompt) = expected_prompt {
        script
            .expect_request(
                "prompt",
                "session/prompt",
                json!({
                    "sessionId": "saved-notice-session",
                    "prompt": [{"type": "text", "text": expected_prompt}]
                }),
            )
            .expect_message(json!({
                "jsonrpc": "2.0", "method": "session/cancel",
                "params": {"sessionId": "saved-notice-session"}
            }))
            .respond("prompt", json!({"stopReason": "cancelled"}))
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
        vec![NOTICE_MESSAGE_ID.parse().unwrap()],
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
    let rig = idle_claude_fixture(clock.clone(), Some(NOTICE_TEXT)).await;
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
    let rig = idle_claude_fixture(clock.clone(), None).await;
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

#[tokio::test]
async fn pre_owner_notice_custody_yields_to_the_human_prompt() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone(), Some(HUMAN_TEXT)).await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    let attempt = notice.attempt();
    let notice_operation = OperationId::try_from(String::from(attempt)).unwrap();
    let (notice_held, release_notice) = pause_before_owner(&rig, notice_operation);
    let ready = adapter.ready(&recipient).await.unwrap();
    let offer = ready.offer(notice);
    tokio::pin!(offer);
    tokio::select! {
        reached = notice_held => reached.unwrap(),
        result = &mut offer => panic!("the notice passed its pre-owner pause: {result:?}"),
    }

    let operation = OperationId::generate();
    let started = rig
        .supervisor
        .prompt_contents(human_prompt(&rig, operation, PromptBusyPolicy::RefuseBusy))
        .await;
    let _ = release_notice.send(());
    assert!(
        started.is_ok(),
        "the held notice blocked the human: {started:?}"
    );
    assert_waiting_then_eligible(&rig, &clock, recipient, attempt, offer.await).await;
    rig.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_reserved_human_prompt_refuses_notice_even_before_the_owner_command() {
    for policy in [
        PromptBusyPolicy::RefuseBusy,
        PromptBusyPolicy::Send(ProviderMessageDelivery::Auto),
    ] {
        let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
        let rig = idle_claude_fixture(clock.clone(), Some(HUMAN_TEXT)).await;
        let (adapter, recipient, notice) =
            prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
        let attempt = notice.attempt();
        let notice_operation = OperationId::try_from(String::from(attempt)).unwrap();
        let operation = OperationId::generate();
        let (human_held, release_human) = pause_before_owner(&rig, operation.clone());
        let human = rig
            .supervisor
            .prompt_contents(human_prompt(&rig, operation, policy));
        tokio::pin!(human);
        tokio::select! {
            reached = human_held => reached.unwrap(),
            result = &mut human => panic!("the human passed its pre-owner pause: {result:?}"),
        }
        assert_eq!(
            rig.supervisor.owner_session_activity(&rig.target),
            crate::ProviderSessionActivity::Idle
        );

        let (notice_reached_owner, release_notice) = pause_before_owner(&rig, notice_operation);
        let offer = adapter.ready(&recipient).await.unwrap().offer(notice);
        tokio::pin!(offer);
        let outcome = tokio::select! {
            result = &mut offer => result,
            reached = notice_reached_owner => {
                reached.unwrap();
                drop(release_notice);
                panic!("a notice passed admission while a human reservation was held");
            }
        };
        release_human.send(()).unwrap();
        human.await.expect("the earlier human request runs");
        assert_waiting_then_eligible(&rig, &clock, recipient, attempt, outcome).await;
        rig.supervisor.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn a_human_prompt_revokes_the_exact_notice_offered_to_a_full_writer() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone(), Some(HUMAN_TEXT)).await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    let attempt = notice.attempt();
    let owner = rig.supervisor.session_owner_for(&rig.target).unwrap();
    let runtime = rig.supervisor.runtime_for(&rig.target.endpoint).unwrap();
    let offered = runtime.preparation_fixture.observe_untaken_notice_offer();
    let capacity = owner.hold_provider_writer_capacity();
    let offer = adapter.ready(&recipient).await.unwrap().offer(notice);
    tokio::pin!(offer);
    tokio::select! {
        offered = offered => offered.expect("owner holds the offered authorization after the real take refused capacity"),
        result = &mut offer => panic!("notice finished before the writer opened: {result:?}"),
    }
    drop(capacity);
    let operation = OperationId::generate();
    rig.supervisor
        .prompt_contents(human_prompt(&rig, operation, PromptBusyPolicy::RefuseBusy))
        .await
        .expect("the human prompt takes the slot after exact notice undo");
    assert_waiting_then_eligible(&rig, &clock, recipient, attempt, offer.await).await;
    rig.supervisor.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_taken_notice_turn_gets_the_ordinary_running_refusal_for_a_human_prompt() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = idle_claude_fixture(clock.clone(), Some(NOTICE_TEXT)).await;
    let (adapter, recipient, notice) =
        prepare_notice(&rig, &clock, Arc::new(NoticeAuthority::new()));
    assert!(matches!(
        adapter.ready(&recipient).await.unwrap().offer(notice).await,
        NoticeWriteOutcome::Written { .. }
    ));
    assert_eq!(
        rig.supervisor.owner_session_activity(&rig.target),
        crate::ProviderSessionActivity::Running
    );
    let refused = rig
        .supervisor
        .prompt_contents(human_prompt(
            &rig,
            OperationId::generate(),
            PromptBusyPolicy::RefuseBusy,
        ))
        .await
        .expect_err("the notice is an actual running turn");
    assert_eq!(refused.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(refused.effect, ProviderOperationEffect::None);
    rig.supervisor.shutdown().await.unwrap();
}

pub(super) fn pause_before_owner(
    rig: &SteeringSession,
    operation: OperationId,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered, wait_for_entry) = tokio::sync::oneshot::channel();
    let (release_owner, release) = tokio::sync::oneshot::channel();
    *rig.supervisor
        .inner
        .before_owner_prompt_test_pause
        .lock()
        .unwrap() = Some((operation, AdmissionTestPause { entered, release }));
    (wait_for_entry, release_owner)
}

fn human_prompt(
    rig: &SteeringSession,
    operation_id: OperationId,
    busy: PromptBusyPolicy,
) -> ProviderPromptContentsRequest {
    ProviderPromptContentsRequest {
        operation_id,
        input_id: session_event_model::InputId::generate(),
        target: rig.target.clone(),
        requested_by: rig.target.clone().into(),
        approver: rig.target.clone().into(),
        input: acp_client_runtime::ProviderTurnInput::Prompt(
            session_event_model::ProviderSubmission::plain(vec![
                session_event_model::PromptContent::text(HUMAN_TEXT.to_owned()).unwrap(),
            ]),
        ),
        busy,
    }
}

async fn assert_waiting_then_eligible(
    rig: &SteeringSession,
    clock: &ManualClock,
    recipient: MailSessionRef,
    attempt: NoticeAttemptId,
    outcome: NoticeWriteOutcome,
) {
    use workspace_model::{
        PostText,
        central_mail::{MessageBody, MessageDelivery, MessageRecord},
    };

    assert_eq!(
        outcome,
        NoticeWriteOutcome::DefinitelyNotWritten(NoticePreTakeRefusal::Busy)
    );
    let now = clock.utc_now();
    let expiry = now + chrono::TimeDelta::minutes(15);
    let message = MessageRecord::queued_central(
        NOTICE_MESSAGE_ID.parse().unwrap(),
        recipient.clone(),
        MessageBody::Direct {
            body: PostText::try_from("inbox fixture".to_owned()).unwrap(),
            delivery: MessageDelivery::Auto,
            reply_to: None,
        },
        now,
    );
    let mut inbox = InboxStore::new(
        MachineLocalStore::open(&rig._root.path().join("notice-inbox.sqlite"))
            .await
            .unwrap(),
    );
    inbox
        .record_delivery(
            &message,
            InboxDecision::Accepted,
            now.timestamp_millis(),
            InboxOfferExpiry::DirectWithExpiry(expiry),
        )
        .await
        .unwrap();
    let waiting = Handoff {
        expires_at: expiry,
        phase: DirectHandoff::Waiting,
    };
    let (writing, effects) =
        DirectHandoffMachine::transition(&waiting, HandoffEvent::EligibleAtMark { attempt, now })
            .unwrap();
    assert_eq!(effects, [HandoffEffect::OfferClaimedNotice]);
    commit_notice_state(&mut inbox, &recipient, waiting.phase, writing.phase, now).await;

    let (waiting, effects) = DirectHandoffMachine::transition(
        &writing,
        HandoffEvent::WriteFinished {
            attempt,
            outcome,
            now,
        },
    )
    .unwrap();
    assert!(effects.is_empty());
    commit_notice_state(&mut inbox, &recipient, writing.phase, waiting.phase, now).await;
    let row = inbox
        .scan_direct_handoffs()
        .await
        .unwrap()
        .healthy
        .remove(0);
    assert_eq!(row.handoff, DirectHandoff::Waiting);

    clock.advance_to(next_mark_after(clock.now())).unwrap();
    let now = clock.utc_now();
    let next_attempt = NoticeAttemptId::mint(now);
    let (retry, effects) = DirectHandoffMachine::transition(
        &Handoff::from(&row),
        HandoffEvent::EligibleAtMark {
            attempt: next_attempt,
            now,
        },
    )
    .unwrap();
    assert_eq!(effects, [HandoffEffect::OfferClaimedNotice]);
    assert_eq!(
        retry.phase,
        DirectHandoff::Writing {
            attempt: next_attempt
        }
    );
    commit_notice_state(&mut inbox, &recipient, row.handoff, retry.phase, now).await;
    assert_eq!(
        inbox.scan_direct_handoffs().await.unwrap().healthy[0].handoff,
        retry.phase
    );
}

async fn commit_notice_state(
    inbox: &mut InboxStore,
    recipient: &MailSessionRef,
    expected: DirectHandoff,
    next: DirectHandoff,
    now: workspace_model::Timestamp,
) {
    assert_eq!(
        inbox
            .compare_and_set_direct_handoffs(
                recipient,
                &[DirectHandoffChange {
                    message_id: NOTICE_MESSAGE_ID.parse().unwrap(),
                    expected,
                    next
                }],
                now,
            )
            .await
            .unwrap(),
        HandoffCommit::Committed,
    );
}
