//! Causal changes to a dispatch decision after it first had to wait.

use super::*;

#[tokio::test]
async fn mcp_auto_refuses_a_new_binding_wide_holder_while_waiting() {
    use crate::binding_store_fixture::BindingFixtureWait;
    let rig = follow_up_rig().await;
    let (prior, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let blocked = rig.supervisor.observe_turn_start_fixture(
        &target,
        BindingFixtureWait::DirectDispatchBlocked(operation.clone()),
    );
    let reply = {
        let call = server.call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        );
        tokio::pin!(call);
        tokio::select! {
            reached = waiting => reached.expect("initially waits on the completed turn"),
            result = &mut call => panic!("answered before waiting: {result:?}"),
        }
        let competing = rig
            .supervisor
            .hold_binding_wide_dispatch_for_test(&provider_binding(&target));
        assert!(rig.supervisor.live_operation_ids().contains(&prior));
        rig.supervisor.wake_eligibility_for_test(&target);
        blocked
            .await
            .expect("reject while both the prior and the newly overlapping holder remain live");
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        release
            .send(())
            .expect("release prior custody after refusal was decided");
        let reply = call.await.expect("MCP refusal");
        assert!(rig.supervisor.publish_held_result(&competing).await);
        reply
    };
    assert_eq!(reply["error"]["kind"], "busy", "{reply}");
    assert_eq!(reply["error"]["stage"], "dispatch", "{reply}");
    assert_eq!(reply["error"]["effect"], "none", "{reply}");
    assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Terminal).await;
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
}

#[tokio::test]
async fn mcp_waiting_mark_cannot_cross_an_owner_handover() {
    use crate::binding_store_fixture::BindingFixtureWait;
    use provider_session_machines::{
        session_lifecycle_machine::CloseAttempt,
        session_owner::{CloseRequest, RunAttempt},
    };

    let rig = follow_up_rig().await;
    let (prior, release) = begin_held_terminal(&rig).await;
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let mark = rig.supervisor.observe_turn_start_fixture(
        &target,
        BindingFixtureWait::DirectDispatchAccepted(operation.clone()),
    );
    let reply = {
        let call = server.call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        );
        tokio::pin!(call);
        tokio::select! {
            reached = waiting => reached.expect("the old owner is waiting"),
            result = &mut call => panic!("answered before waiting: {result:?}"),
        }
        let old = rig
            .supervisor
            .session_owner_for(&target)
            .expect("old owner");
        let run = live_run(&rig);
        let runtime = provider_runtime(&rig);
        let replacement = runtime
            .preparation_fixture
            .observe_replacement_installation();
        // Exhaust the old owner's completion capacity, then drive a real effect failure.
        let capacity = old.hold_completion_capacity();
        let failure = old
            .admit(SessionOwnerCommand::CloseWithCapability(CloseRequest {
                run: RunAttempt {
                    generation: run.generation,
                    attempt: CloseAttempt(uuid::Uuid::now_v7()),
                },
                provider_supports_close: false,
            }))
            .await
            .expect_err("old owner cannot complete the close effect");
        assert!(matches!(failure, OwnerDoorError::OwnerStopped));
        drop(capacity);
        replacement
            .await
            .expect("registry installed a successor over the same lane");
        let current = rig
            .supervisor
            .owner_session_snapshot(&target)
            .expect("new owner");
        assert_ne!(current.owner.incarnation, run.incarnation);
        assert!(rig.supervisor.live_operation_ids().contains(&prior));
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        release
            .send(())
            .expect("release previous custody after handover");
        tokio::select! {
            reached = mark => panic!("stale owner obtained a mark: {reached:?}"),
            result = &mut call => result.expect("old caller receives its refusal"),
        }
    };
    assert_eq!(reply["error"]["kind"], "unavailable", "{reply}");
    assert_eq!(reply["error"]["effect"], "none", "{reply}");
    assert!(!rig.receipt.with_extension("successor").exists());
    let row = rig
        .port
        .store
        .lock()
        .await
        .inspect(&operation)
        .await
        .expect("read op")
        .expect("old admission");
    assert!(row.dispatched_at_ms.is_none());
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
    assert!(!rig.receipt.with_extension("successor").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn actor_cutoff_prevents_a_late_offer_before_terminal_sql() {
    use crate::binding_store_fixture::BindingFixtureWait;

    let rig = follow_up_rig().await;
    let (prior, release) = begin_held_terminal(&rig).await;
    let prior_deadline = rig
        .port
        .clock
        .monotonic_now()
        .after(crate::session_store_lane::STORE_WAIT);
    rig.clock
        .advance(Duration::from_secs(1))
        .expect("give the earlier terminal reply its own cutoff");
    let target = protocol_target("fixture-session");
    let server = serve_prompt_tool(&rig, &target);
    let operation = OperationId::generate();
    let waiting = rig
        .supervisor
        .observe_direct_dispatch_wait_for_test(&target, operation.clone());
    let expired = rig.supervisor.observe_turn_start_fixture(
        &target,
        BindingFixtureWait::DispatchWaitExpired(operation.clone()),
    );
    let marked = rig.supervisor.observe_turn_start_fixture(
        &target,
        BindingFixtureWait::DirectDispatchAccepted(operation.clone()),
    );
    let runtime = provider_runtime(&rig);
    let (cutoff, offer) = runtime.preparation_fixture.pause_dispatch_mark();
    let (retiring, write_terminal) = runtime.preparation_fixture.pause_dispatch_expiry();
    let reply = {
        let call = server.call_structured(
            "conversation_prompt",
            prompt_tool_arguments(&target, &operation),
        );
        tokio::pin!(call);
        let actor_deadline = tokio::select! {
            reached = cutoff => reached.expect("the actor supplies its absolute cutoff before the store effect"),
            result = &mut call => panic!("MCP answered before its mark decision: {result:?}"),
        };
        rig.clock
            .advance(Duration::from_secs(1))
            .expect("delay the first store offer after actor commit");
        let legacy_deadline = rig
            .port
            .clock
            .monotonic_now()
            .after(crate::session_store_lane::STORE_WAIT);
        assert!(legacy_deadline > actor_deadline);
        offer
            .send(())
            .expect("let the first lane decision see prior custody");
        waiting.await.expect("initial offer has no ticket");
        wait_for_tool_input(&rig, &target, &operation).await;
        eprintln!("dispatch boundary: initial offer is parked");
        rig.clock
            .advance(prior_deadline.saturating_duration_since(rig.port.clock.monotonic_now()))
            .expect("reach the previous terminal reply deadline first");
        let earlier = rig
            .supervisor
            .wait(ConversationOperationWaitRequest {
                operation_id: prior.clone(),
                timeout_seconds: PositiveSeconds::try_from(5).expect("fixture wait"),
            })
            .await
            .expect_err("earlier reply is available while its terminal write is still held");
        assert_eq!(earlier.effect, ProviderOperationEffect::Applied);
        assert!(rig.supervisor.live_operation_ids().contains(&prior));
        assert!(rig.port.clock.monotonic_now() < actor_deadline);
        let boundary = actor_deadline.after(Duration::from_millis(500));
        assert!(boundary < legacy_deadline);
        rig.clock
            .advance(boundary.saturating_duration_since(rig.port.clock.monotonic_now()))
            .expect("cross the actor cutoff while the old lane budget would still be open");
        retiring
            .await
            .expect("actor expired the input, before offering terminal SQL");
        eprintln!("dispatch boundary: actor expiry effect is held");
        let mut completions = rig.supervisor.subscribe_completions();
        release
            .send(())
            .expect("release previous terminal custody inside the old gap");
        while rig.supervisor.live_operation_ids().contains(&prior) {
            completions
                .changed()
                .await
                .expect("prior operation custody release");
        }
        eprintln!("dispatch boundary: prior custody released");
        tokio::select! {
            reached = marked => panic!("an expired input obtained a late ticket: {reached:?}"),
            reached = expired => reached.expect("the retry used the actor cutoff"),
        }
        assert_eq!(rig.port.clock.monotonic_now(), boundary);
        assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Admitted).await;
        write_terminal
            .send(())
            .expect("release new operation terminal SQL");
        call.await.expect("MCP expiry")
    };
    assert_eq!(reply["error"]["kind"], "unavailable", "{reply}");
    assert_eq!(reply["error"]["stage"], "dispatch", "{reply}");
    assert_eq!(reply["error"]["effect"], "none", "{reply}");
    assert_eq!(
        reply["error"]["message"], "provider input was not submitted",
        "{reply}"
    );
    assert_tool_has_no_dispatch(&rig, &operation, ProviderOperationStage::Terminal).await;
    server.stop().await.expect("MCP shutdown");
    rig.supervisor.shutdown().await.expect("drain");
}
