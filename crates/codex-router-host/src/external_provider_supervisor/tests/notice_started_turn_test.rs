use super::*;

#[tokio::test]
async fn managed_notice_started_new_turn_after_the_old_turn_ends_is_seen_once() {
    use provider_session_machines::session_turn_machine::SessionTurn;
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
