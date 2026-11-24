use super::*;
use crate::{ProviderPromptContentsRequest, external_provider_supervisor::PromptBusyPolicy};
use collaboration_protocol::{MessageContent, MessageDelivery};
use provider_session_machines::session_turn_machine::SessionTurn;
use router_clock::{ManualClock, RuntimeClock as _};

#[tokio::test]
async fn a_start_that_first_saw_running_waits_for_the_turn_seen_by_its_command() {
    start_observed_before_settling(false).await;
}

#[tokio::test]
async fn a_start_that_first_saw_running_gets_the_exact_fence_at_the_deadline() {
    start_observed_before_settling(true).await;
}

async fn start_observed_before_settling(advance_deadline: bool) {
    let clock = Arc::new(ManualClock::new(chrono::DateTime::UNIX_EPOCH));
    let rig = steering_session_on_clock(
        "provider-steering-agent.ts",
        "fixture-session",
        |receipt| send_race_arguments(receipt, "hold-each-prompt"),
        clock.clone(),
    )
    .await;
    let actor = human("settling-admission");
    let first = OperationId::generate();
    let next = OperationId::generate();
    let (writer_held, release_writer) = rig.supervisor.pause_terminal_write_of(&rig.target, &first);
    let (reached_command, send_command) =
        super::managed_notice::pause_before_owner(&rig, next.clone());
    rig.supervisor
        .prompt(ConversationPromptRequest {
            operation_id: first.clone(),
            input_id: None,
            target: rig.target.clone(),
            generation: None,
            requested_by: actor.clone(),
            approver: actor.clone(),
            delivery: MessageDelivery::Auto,
            prompt: MessageContent::HumanUser {
                text: MessageText::try_from("first".to_owned()).unwrap(),
            },
        })
        .await
        .expect("first prompt taken");
    assert!(matches!(
        rig.supervisor
            .session_owner_for(&rig.target)
            .unwrap()
            .snapshots()
            .borrow()
            .turn,
        SessionTurn::Running { .. }
    ));
    let start_held = rig.supervisor.observe_prompt_start_held(&rig.target);
    let content = session_event_model::PromptContent::text("second".to_owned()).unwrap();
    let second = rig
        .supervisor
        .prompt_contents(ProviderPromptContentsRequest {
            operation_id: next,
            input_id: session_event_model::InputId::generate(),
            target: rig.target.clone(),
            requested_by: actor.clone(),
            approver: actor,
            input: acp_client_runtime::ProviderTurnInput::Prompt(
                session_event_model::ProviderSubmission::plain(vec![content]),
            ),
            busy: PromptBusyPolicy::RefuseBusy,
        });
    tokio::pin!(second);
    tokio::select! {
        reached = reached_command => reached.expect("second was reserved while Running"),
        reply = &mut second => panic!("start returned before its owner command: {reply:?}"),
    }
    node_test_fixture::release_fixture(&rig.receipt, ".release-1")
        .expect("end the first provider turn");
    writer_held.await.expect("terminal writer held in Settling");
    send_command
        .send(())
        .expect("admit the delayed start command");
    tokio::select! {
        held = start_held => held.expect("the reduced command was held by Settling"),
        reply = &mut second => panic!("owner returned early while Settling: {reply:?}"),
    }
    assert_eq!(recorded_requests(&rig.receipt, "session/prompt"), 1);
    if advance_deadline {
        let budget = std::time::Duration::from_secs(10);
        clock
            .not_due_check_now(clock.monotonic_now().after(budget))
            .await;
        clock.advance(budget).unwrap();
        let failure = second
            .await
            .expect_err("deadline keeps the successor unsent");
        assert_eq!(
            failure.kind,
            ConversationOperationFailureKind::PreviousTurnSettlementUnresolved
        );
        assert_eq!(failure.operation_id, Some(first));
        assert_eq!(recorded_requests(&rig.receipt, "session/prompt"), 1);
        release_writer.send(()).unwrap();
    } else {
        release_writer.send(()).unwrap();
        second.await.expect("clear settlement allows the start");
        while recorded_requests(&rig.receipt, "session/prompt") < 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(recorded_requests(&rig.receipt, "session/prompt"), 2);
        node_test_fixture::release_fixture(&rig.receipt, ".release-2")
            .expect("end the successor turn");
    }
    rig.supervisor.shutdown().await.expect("stop fixture");
}
