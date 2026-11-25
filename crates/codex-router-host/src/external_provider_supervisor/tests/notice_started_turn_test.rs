use super::*;
use provider_session_machines::session_turn_machine::SessionTurn;

#[tokio::test]
async fn managed_notice_started_new_turn_after_the_old_turn_ends_is_seen_once() {
    use state_machine_core::StateMachine;
    use switchboard_machine_core_store::{DirectHandoff, SeenReceiptState};
    use switchboard_message_custody::handover::{
        DirectHandoffMachine, Handoff, HandoffEffect, HandoffEvent,
    };

    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectThenProviderStartsTurn,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let socket = tokio::net::UnixListener::bind(rig.receipt.with_extension("sock")).unwrap();
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, _) = prepare_notice(&rig, &clock, authority.clone());
    let actor = requester().into();
    let first_prompt = rig
        .supervisor
        .submit_owned_direct_delivery(ProviderPromptContentsRequest {
            operation_id: OperationId::generate(),
            input_id: session_event_model::InputId::generate(),
            target: rig.target.clone(),
            requested_by: actor,
            approver: requester().into(),
            input: acp_client_runtime::ProviderTurnInput::Prompt(
                session_event_model::ProviderSubmission::plain(vec![
                    session_event_model::PromptContent::text("Hold the foreground turn".to_owned())
                        .unwrap(),
                ]),
            ),
            busy: PromptBusyPolicy::RefuseBusy,
        })
        .await
        .unwrap();
    assert_eq!(
        first_prompt,
        crate::session_owner_calls::InputAcceptance::Started
    );
    let owner = rig.supervisor.session_owner_for(&rig.target).unwrap();
    let mut snapshots = owner.snapshots();
    let active = snapshots.borrow().clone();
    let SessionTurn::Running { turn: old_turn } = active.turn else {
        panic!("the first prompt must hold the current turn");
    };
    let writer = adapter.ready(&recipient).await.unwrap();
    let notice = second_notice(&recipient, &clock, authority);
    let attempt = notice.attempt();
    let pending_reply = tokio::spawn(writer.offer(notice));
    let (release, _) = socket.accept().await.unwrap();
    snapshots
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .unwrap();
    drop(release);
    let outcome = pending_reply.await.unwrap();
    assert_eq!(
        outcome,
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );

    let current = snapshots.borrow().clone();
    let SessionTurn::Running { turn: new_turn } = current.turn else {
        panic!("the owner must record the provider-started turn");
    };
    assert_ne!(new_turn, old_turn);
    assert!(current.queue.is_empty());
    let (settled, effects) = DirectHandoffMachine::transition(
        &Handoff {
            expires_at: clock.utc_now() + chrono::TimeDelta::seconds(1),
            phase: DirectHandoff::Writing { attempt },
        },
        HandoffEvent::WriteFinished {
            attempt,
            outcome,
            now: clock.utc_now(),
        },
    )
    .unwrap();
    assert_eq!(
        settled.phase,
        DirectHandoff::Seen {
            completed_at: clock.utc_now(),
            receipt: SeenReceiptState::Owed,
        }
    );
    assert_eq!(effects, [HandoffEffect::WakeReceiptPump]);

    rig.supervisor.shutdown().await.unwrap();
    assert_only_wire_steer(&rig);
}

#[tokio::test]
async fn held_notice_steer_does_not_refuse_human_admission_after_turn_ends() {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = managed_notice_fixture(
        clock.clone(),
        NoticeScript::DirectThenProviderStartsTurn,
        collaboration_protocol::ProviderKind::ClaudeCode,
    )
    .await;
    let socket = tokio::net::UnixListener::bind(rig.receipt.with_extension("sock")).unwrap();
    let authority = Arc::new(NoticeAuthority::new());
    let (adapter, recipient, _) = prepare_notice(&rig, &clock, authority.clone());
    let foreground = rig
        .supervisor
        .prompt_contents(refuse_busy_prompt(&rig, "Hold the foreground turn"))
        .await
        .expect("the foreground prompt starts");
    let owner = rig.supervisor.session_owner_for(&rig.target).unwrap();
    let mut snapshots = owner.snapshots();
    let SessionTurn::Running { turn: old_turn } = snapshots.borrow().turn else {
        panic!("the foreground turn must be running");
    };
    let writer = adapter.ready(&recipient).await.unwrap();
    let notice = second_notice(&recipient, &clock, authority);
    let notice_operation = String::from(notice.attempt());
    let pending_notice = tokio::spawn(writer.offer(notice));
    let (release, _) = socket.accept().await.unwrap();
    snapshots
        .wait_for(|snapshot| snapshot.turn == SessionTurn::Idle)
        .await
        .unwrap();
    let settled = rig
        .supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: foreground.operation.operation_id,
            timeout_seconds: PositiveSeconds::try_from(1).unwrap(),
        })
        .await
        .unwrap();
    assert!(matches!(
        settled.output,
        ConversationOperationWaitOutput::Available {
            settlement: ConversationOperationSettlement::PromptCompleted { .. }
        }
    ));
    assert!(!pending_notice.is_finished());

    let human = rig
        .supervisor
        .prompt_contents(refuse_busy_prompt(&rig, "Do not start a second turn"))
        .await
        .expect("the held notice reply must not refuse the human at Host admission");
    assert_eq!(
        human.admission,
        collaboration_protocol::ConversationAdmissionState::Admitted
    );
    let unsent = rig
        .supervisor
        .wait(ConversationOperationWaitRequest {
            operation_id: human.operation.operation_id,
            timeout_seconds: PositiveSeconds::try_from(1).unwrap(),
        })
        .await
        .expect_err("the owner dispatch fence must keep the human prompt off the wire");
    assert_eq!(unsent.stage, ConversationOperationFailureStage::Dispatch);
    assert_eq!(unsent.kind, ConversationOperationFailureKind::Busy);
    assert_eq!(unsent.effect, ProviderOperationEffect::None);
    assert!(String::from(unsent.message).contains(&notice_operation));
    assert_eq!(snapshots.borrow().turn, SessionTurn::Idle);

    drop(release);
    assert_eq!(
        pending_notice.await.unwrap(),
        NoticeWriteOutcome::Written {
            taken_at: clock.utc_now()
        }
    );
    let SessionTurn::Running { turn: new_turn } = snapshots.borrow().turn else {
        panic!("the provider-started turn must be recorded");
    };
    assert_ne!(new_turn, old_turn);
    rig.supervisor.shutdown().await.unwrap();
    assert_only_wire_steer(&rig);
}

fn refuse_busy_prompt(rig: &SteeringSession, text: &str) -> ProviderPromptContentsRequest {
    ProviderPromptContentsRequest {
        operation_id: OperationId::generate(),
        input_id: session_event_model::InputId::generate(),
        target: rig.target.clone(),
        requested_by: requester().into(),
        approver: requester().into(),
        input: acp_client_runtime::ProviderTurnInput::Prompt(
            session_event_model::ProviderSubmission::plain(vec![
                session_event_model::PromptContent::text(text.to_owned()).unwrap(),
            ]),
        ),
        busy: PromptBusyPolicy::RefuseBusy,
    }
}

fn assert_only_wire_steer(rig: &SteeringSession) {
    let traffic = std::fs::read_to_string(rig.receipt.with_extension("traffic")).unwrap();
    let requests: Vec<_> = traffic
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|message| {
            matches!(
                message["method"].as_str(),
                Some("session/prompt" | "_session/steering")
            )
        })
        .collect();
    assert_eq!(requests.len(), 1, "{traffic}");
    assert_eq!(requests[0]["method"], "_session/steering");
    assert!(
        !rig.receipt.exists(),
        "{:?}",
        std::fs::read_to_string(&rig.receipt)
    );
}
