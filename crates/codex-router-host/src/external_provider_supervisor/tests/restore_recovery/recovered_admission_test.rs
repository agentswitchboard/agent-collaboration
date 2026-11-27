use super::*;
use crate::session_owner_door::{OwnerDoorError, SessionOwnerAddress};
use collaboration_contracts::SessionEventHub;
use collaboration_protocol::{ProviderIdentity, ProviderReconciliationState};
use provider_session_machines::{
    session_lifecycle_machine::{CloseAttempt, SessionLifecycle},
    session_owner::SessionOwnerCommand,
};

async fn recovered_prompt_failure(taken: bool, recovery_first: bool) {
    let rig = RestoreRig::new();
    rig.seed_session().await;
    let (host, hub) = rig
        .open_host_with_fixture_arguments(
            &["plain-prompts", "silent-prompts"],
            Arc::new(acp_provider_adapters::StandardAcpBehavior),
        )
        .await;
    rig.assert_loads(&host).await;
    let old_owner = host.session_owner_for(&rig.target).expect("session owner");
    let runtime = host.runtime_for(&rig.target.endpoint).expect("runtime");
    let acknowledgement = runtime.preparation_fixture.pause_delivery_ack(taken);
    let (recovery_reached, release_recovery) = runtime.preparation_fixture.pause_recovery();
    // Saturation fails the owner when it tries to reserve the mark receipt,
    // before it can create a transport authorization.
    let capacity = (!taken).then(|| old_owner.hold_completion_capacity());
    let operation = OperationId::generate();
    let input = session_event_model::InputId::generate();
    let mut request = rig.prompt_request(operation.clone(), "in-flight admission");
    request.input_id = Some(input.clone());
    let prompt = host.prompt(request);
    tokio::pin!(prompt);
    tokio::select! {
        result = acknowledgement.reached => { let _observed = result.expect("delivery acknowledgement edge"); },
        result = &mut prompt => panic!("prompt answered before its acknowledgement edge: {result:?}"),
    }
    drop(capacity);

    if taken {
        // Discard the acknowledgement while keeping a real writer take. A
        // different command then fails the old owner before a prompt result.
        rig.until_wire_request("prompts").await;
        fail_live_owner(&old_owner).await;
    }

    recovery_reached
        .await
        .expect("failed owner recovery has its evidence");
    let successor = host
        .session_owner_for(&rig.target)
        .expect("replacement installed");
    assert_ne!(
        successor.snapshots().borrow().owner,
        old_owner.snapshots().borrow().owner,
    );
    if recovery_first {
        release_recovery.send(()).expect("release recovery");
        old_owner.wait_for_recovery_replies().await;
        acknowledgement
            .release
            .send(())
            .expect("release acknowledgement");
        acknowledgement
            .finished
            .await
            .expect("caller handles acknowledgement");
    } else {
        acknowledgement
            .release
            .send(())
            .expect("release acknowledgement");
        acknowledgement
            .finished
            .await
            .expect("caller handles acknowledgement first");
        release_recovery.send(()).expect("release recovery");
    }
    prompt
        .await
        .expect("admitted operation exposes the recovered result");
    let failure = host
        .wait(wait_request(operation.clone()))
        .await
        .expect_err("prompt did not settle successfully");
    if taken {
        assert_eq!(failure.effect, ProviderOperationEffect::Unknown);
        assert_eq!(
            failure.kind,
            ConversationOperationFailureKind::OutcomeUnknown
        );
        assert_eq!(
            rig.wire_receipt()["prompts"],
            serde_json::json!([rig.target.session_id])
        );
        assert_uncertain_dispatch_mark(&rig, &operation).await;
    } else {
        assert_eq!(failure.effect, ProviderOperationEffect::None);
        assert_eq!(
            String::from(failure.message),
            "the provider connection ended; reconnect and send again; the saved result isn't available and may differ",
        );
        assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
        assert_unsent_row(&host, &operation).await;
        rig.assert_loads(&host).await;
        assert_replay_has_no_input(&rig, &hub, &input).await;
    }
    host.shutdown().await.expect("shut down fixture");
    if taken {
        assert_uncertain_dispatch_mark(&rig, &operation).await;
        let wire_receipt = rig.root.path().join("wire.json");
        std::fs::rename(
            &wire_receipt,
            rig.root.path().join("first-provider-wire.json"),
        )
        .expect("retain the first provider's wire receipt before reopening");
        let (reopened, reopened_hub) = rig
            .open_host_with_fixture_arguments(
                &["plain-prompts", "restored-in-auto"],
                Arc::new(acp_provider_adapters::StandardAcpBehavior),
            )
            .await;
        let refusal = reopened
            .load(rig.load_request(OperationId::generate()))
            .await
            .expect_err("the old unknown dispatch blocks a fresh load and replay");
        assert_eq!(refusal.kind, ConversationOperationFailureKind::Busy);
        assert!(String::from(refusal.message).contains(operation.as_str()));
        let target = ProviderIdentity::session_to_board(&rig.target).expect("hub target");
        let history = reopened_hub
            .attach(target.clone())
            .await
            .expect("saved session");
        assert_eq!(history.epoch, 0, "the refused load did not start replay");
        assert!(history.snapshot.is_empty());
        assert_eq!(
            reopened_hub
                .state(target)
                .await
                .expect("saved session state"),
            session_event_model::SessionState::Unloaded,
        );
        if wire_receipt
            .try_exists()
            .expect("reopened provider receipt path")
        {
            assert!(
                rig.wire_receipt()["prompts"]
                    .as_array()
                    .expect("wire prompts")
                    .is_empty(),
                "restore did not resend the old taken input"
            );
        }
        reopened
            .shutdown()
            .await
            .expect("shut down reopened fixture");
    }
    if !taken {
        let reopened = rig.open_host("restored-in-auto").await;
        assert_unsent_row(&reopened, &operation).await;
        let wait = reopened
            .wait(wait_request(operation))
            .await
            .expect("after restart only the original admission row remains");
        assert!(matches!(
            wait.output,
            ConversationOperationWaitOutput::Pending
        ));
        assert_eq!(wait.operation.stage, ProviderOperationStage::Admitted);
        assert_eq!(wait.operation.effect, ProviderOperationEffect::None);

        rig.assert_loads(&reopened).await;
        let later = OperationId::generate();
        reopened
            .prompt(rig.prompt_request(later.clone(), "a new operation"))
            .await
            .expect("the unmarked row does not hold new input");
        let settled = reopened
            .wait(wait_request(later))
            .await
            .expect("new prompt settles");
        assert!(matches!(
            settled.output,
            ConversationOperationWaitOutput::Available {
                settlement: ConversationOperationSettlement::PromptCompleted { .. }
            }
        ));
        reopened
            .shutdown()
            .await
            .expect("shut down reopened fixture");
    }
}

async fn fail_live_owner(owner: &SessionOwnerAddress) {
    let snapshot = owner.snapshots().borrow().clone();
    let SessionLifecycle::Live { generation } = snapshot.lifecycle else {
        panic!("failed prompt must have a live owner");
    };
    let capacity = owner.hold_completion_capacity();
    let failure = owner
        .admit(SessionOwnerCommand::Close {
            generation,
            attempt: CloseAttempt(uuid::Uuid::now_v7()),
        })
        .await
        .expect_err("owner stops before acknowledging close");
    assert!(matches!(failure, OwnerDoorError::OwnerStopped));
    drop(capacity);
}

#[cfg(debug_assertions)]
async fn fail_live_owner_on_delivery(owner: &SessionOwnerAddress) {
    use provider_session_machines::{
        session_owner::{DeliveryInput, OwnerInput, PromptAdmission},
        session_turn_machine::TurnId,
    };
    let SessionLifecycle::Live { generation } = owner.snapshots().borrow().lifecycle else {
        panic!("the offered input still belongs to a live owner");
    };
    let capacity = owner.hold_completion_capacity();
    let failure = owner
        .admit(SessionOwnerCommand::PlaceDelivery {
            input: OwnerInput {
                generation,
                input: session_event_model::InputId::generate(),
                operation: uuid::Uuid::now_v7(),
                turn: TurnId(uuid::Uuid::now_v7()),
            },
            delivery: DeliveryInput::Prompt {
                admission: PromptAdmission::Queue,
                selected: Vec::new(),
            },
        })
        .await
        .expect_err("the owner stops when it cannot reserve the second input's placement reply");
    assert!(matches!(failure, OwnerDoorError::OwnerStopped));
    drop(capacity);
}

async fn assert_uncertain_dispatch_mark(rig: &RestoreRig, operation: &OperationId) {
    let record = rig
        .store()
        .await
        .inspect(operation)
        .await
        .expect("inspect operation")
        .expect("operation retains its durable dispatch mark");
    assert_eq!(record.stage, ProviderOperationStage::MayHaveDispatched);
    assert_eq!(record.effect, ProviderOperationEffect::Unknown);
    assert_eq!(
        record.reconciliation_state,
        ProviderReconciliationState::Unresolved
    );
}

async fn assert_replay_has_no_input(
    rig: &RestoreRig,
    hub: &switchboard_session_events::ProviderSessionEventHub,
    input: &session_event_model::InputId,
) {
    let target = ProviderIdentity::session_to_board(&rig.target).expect("hub target");
    let replay = hub.attach(target).await.expect("restored session replay");
    assert_ne!(replay.epoch, 0, "load published a new replay epoch");
    assert!(
        replay.snapshot.iter().all(|observed| {
            !matches!(&observed.event,
            session_event_model::SessionEvent::TurnStarted { input_id, .. }
                | session_event_model::SessionEvent::InputAccepted { input_id, .. }
                if input_id == input)
        }),
        "the failed input did not appear as a fresh submission in replay"
    );
}

async fn assert_unsent_row(host: &ExternalProviderSupervisor, operation: &OperationId) {
    let shown = host
        .show(ConversationOperationShowRequest {
            operation_id: operation.clone(),
        })
        .await
        .expect("show the retained admission row");
    assert_eq!(shown.stage, ProviderOperationStage::Admitted);
    assert_eq!(shown.effect, ProviderOperationEffect::None);
    assert!(shown.queue_state.is_none());
    assert!(
        !host
            .inner
            .store
            .lock()
            .await
            .blocking_uncertain_operation_ids(&binding(), shown.target.as_ref())
            .await
            .expect("query uncertain operations")
            .contains(operation)
    );
}

#[cfg(debug_assertions)]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn failed_owner_withdraws_an_offered_prompt_and_fences_both_old_writes() {
    use crate::session_store_lane::{MarkedInput, SessionStoreOfferError};
    use acp_client_runtime::handover_test_control::HandoverPausePoint;
    use provider_session_machines::session_owner::{AuthorizationTarget, DispatchResult};

    let rig = RestoreRig::new();
    rig.seed_session().await;
    let (host, hub) = rig
        .open_host_with_fixture_arguments(
            &["plain-prompts", "silent-prompts"],
            Arc::new(acp_provider_adapters::StandardAcpBehavior),
        )
        .await;
    rig.assert_loads(&host).await;
    let previous = host.session_owner_for(&rig.target).expect("original owner");
    let runtime = host.runtime_for(&rig.target.endpoint).expect("ACP runtime");
    let (recovery_reached, release_recovery) = runtime.preparation_fixture.pause_recovery();
    let acknowledgement = runtime.preparation_fixture.pause_delivery_ack(true);
    let operation = OperationId::generate();
    let input = session_event_model::InputId::generate();
    let paused =
        runtime.pause_work_handover_for_test(HandoverPausePoint::AfterOffer, input.clone());
    let mut request = rig.prompt_request(operation.clone(), "offered before owner failure");
    request.input_id = Some(input.clone());
    let prompt = host.prompt(request);
    tokio::pin!(prompt);

    let authorization = tokio::select! {
        offered = paused.reached => offered.expect("the exact ACP offer was recorded"),
        result = &mut prompt => panic!("prompt answered before the writer offer: {result:?}"),
    };
    assert_eq!(authorization.target, AuthorizationTarget::NewTurn);
    assert_eq!(
        authorization.incarnation,
        previous.snapshots().borrow().owner.incarnation
    );
    assert_eq!(authorization.input_id, input);
    assert_uncertain_dispatch_mark(&rig, &operation).await;
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));

    let writer_capacity = previous
        .hold_provider_writer_capacity()
        .expect("the real ACP writer capacity fixture is installed");
    paused
        .release
        .send(())
        .expect("allow hand-over with the writer full");
    tokio::select! {
        result = acknowledgement.reached => { let _observed = result.expect("prompt admission acknowledgement"); },
        result = &mut prompt => panic!("prompt answered before its acknowledgement edge: {result:?}"),
    }
    fail_live_owner_on_delivery(&previous).await;
    recovery_reached
        .await
        .expect("the production door handed the lane to its successor");
    let successor = host
        .session_owner_for(&rig.target)
        .expect("replacement owner");
    assert_ne!(
        successor.snapshots().borrow().owner.incarnation,
        authorization.incarnation
    );

    drop(writer_capacity);
    assert_eq!(
        runtime.try_recovered_handover_for_test(&authorization),
        Ok(DispatchResult::DefinitelyUnsent(authorization.clone())),
        "a late take of the withdrawn slot cannot enqueue",
    );
    assert!(
        matches!(
            previous
                .recovery_storage_for_test()
                .try_recovered_mark_for_test(
                    authorization.incarnation,
                    MarkedInput {
                        input: input.clone(),
                        operation: operation.clone()
                    },
                ),
            Err(SessionStoreOfferError::Replaced),
        ),
        "the replaced controller cannot append a store mark"
    );

    release_recovery
        .send(())
        .expect("release the recovery reply");
    previous.wait_for_recovery_replies().await;
    acknowledgement
        .release
        .send(())
        .expect("release the displaced caller");
    prompt
        .await
        .expect("the admitted operation exposes the recovered result");
    acknowledgement
        .finished
        .await
        .expect("caller handled its acknowledgement");
    let failure = host
        .wait(wait_request(operation.clone()))
        .await
        .expect_err("withdrawn prompt was never sent");
    assert_eq!(failure.effect, ProviderOperationEffect::None);
    assert_confirmed_withdrawal(&rig, &operation).await;
    rig.assert_loads(&host).await;
    assert_replay_has_no_input(&rig, &hub, &input).await;
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
    host.shutdown().await.expect("shut down fixture");
    assert_confirmed_withdrawal(&rig, &operation).await;
}

#[cfg(debug_assertions)]
async fn assert_confirmed_withdrawal(rig: &RestoreRig, operation: &OperationId) {
    let record = rig
        .store()
        .await
        .inspect(operation)
        .await
        .expect("inspect operation")
        .expect("withdrawn operation retains its durable settlement");
    assert_eq!(record.stage, ProviderOperationStage::Terminal);
    assert_eq!(record.effect, ProviderOperationEffect::None);
    assert_eq!(
        record.reconciliation_state,
        ProviderReconciliationState::Confirmed
    );
}

#[tokio::test]
async fn a_lost_admission_ack_keeps_unsent_recovery_when_the_caller_is_first() {
    recovered_prompt_failure(false, false).await;
}

#[tokio::test]
async fn a_lost_admission_ack_keeps_unsent_recovery_when_recovery_is_first() {
    recovered_prompt_failure(false, true).await;
}

#[tokio::test]
async fn a_lost_admission_ack_keeps_taken_recovery_when_the_caller_is_first() {
    recovered_prompt_failure(true, false).await;
}

#[tokio::test]
async fn a_lost_admission_ack_keeps_taken_recovery_when_recovery_is_first() {
    recovered_prompt_failure(true, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn staged_queue_receipt_is_reclassified_after_owner_fails_before_publish() {
    queued_receipt_after_owner_failure(true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn staged_queue_waiters_close_when_no_owner_can_replace_the_failed_owner() {
    queued_receipt_after_owner_failure(false).await;
}

async fn queued_receipt_after_owner_failure(replace_owner: bool) {
    use crate::session_owner_calls::InputAcceptance;
    use crate::session_owner_preparation_fixture::DeliveryAcknowledgement;
    use provider_session_machines::session_owner::{
        OwnerInput, PromptAdmission, RevokeReason, SessionOwnerReply,
    };
    use provider_session_machines::session_turn_machine::TurnId;
    use tokio::sync::oneshot::error::TryRecvError;

    let rig = RestoreRig::new();
    rig.seed_session().await;
    let host = rig.open_host("silent-prompts").await;
    rig.assert_loads(&host).await;
    let owner = host.session_owner_for(&rig.target).expect("live owner");
    let mut snapshots = owner.snapshots();
    let before = snapshots.borrow_and_update().clone();
    let SessionLifecycle::Live { generation } = before.lifecycle else {
        panic!("the queue requires a live owner");
    };
    assert!(before.queue.is_empty());

    let runtime = host.runtime_for(&rig.target.endpoint).expect("runtime");
    let acknowledgement = runtime.preparation_fixture.pause_delivery_ack(false);
    let recovery = if replace_owner {
        Some(runtime.preparation_fixture.pause_recovery())
    } else {
        runtime.preparation_fixture.refuse_next_replacement();
        None
    };
    let capacity = owner.hold_completion_capacity();
    assert!(!capacity.is_empty());
    let input = OwnerInput {
        input: session_event_model::InputId::generate(),
        operation: uuid::Uuid::now_v7(),
        generation,
        turn: TurnId(uuid::Uuid::now_v7()),
    };
    let content = session_event_model::PreparedPrompt::plain(vec![
        session_event_model::PromptContent::text("awaiting a dispatch mark".to_owned())
            .expect("text"),
    ]);
    let delivery = owner.start_delivery(
        input.clone(),
        content,
        SessionOwnerCommand::Prompt {
            input: input.clone(),
            admission: PromptAdmission::Queue,
        },
    );
    tokio::pin!(delivery);
    let ack = tokio::select! {
        reached = acknowledgement.reached => reached.expect("observe the real acknowledgement"),
        _ = &mut delivery => panic!("delivery bypassed its acknowledgement probe"),
    };
    assert_eq!(ack, DeliveryAcknowledgement::OwnerStopped);
    assert_eq!(
        *snapshots.borrow(),
        before,
        "failed reduction published its queue"
    );
    drop(capacity);
    acknowledgement
        .release
        .send(())
        .expect("release the acknowledgement probe");
    let mut delivery = delivery.await.expect("failed-owner result receivers");
    acknowledgement
        .finished
        .await
        .expect("door returned the real caller handles");

    if let Some((reached, release)) = recovery {
        reached
            .await
            .expect("replacement has settled its writer evidence");
        assert_ne!(
            host.session_owner_for(&rig.target)
                .expect("replacement")
                .snapshots()
                .borrow()
                .owner,
            before.owner
        );
        assert_eq!(
            delivery.accepted.try_recv(),
            Err(TryRecvError::Empty),
            "the failed reduction must retain the acceptance waiter for recovery"
        );
        assert!(matches!(
            delivery.terminal.try_recv(),
            Err(TryRecvError::Empty)
        ));
        release
            .send(())
            .expect("let the real replacement settle the caller");
        assert_eq!(
            delivery.accepted.await.expect("recovered acceptance"),
            InputAcceptance::Unsent {
                reason: Some(RevokeReason::ConnectionLost)
            }
        );
        let terminal = delivery.terminal.await.expect("recovered terminal");
        assert!(
            !terminal.saved,
            "completion reservation failed before the lane took the mark"
        );
        assert!(
            matches!(terminal.reply, SessionOwnerReply::InputUnsent { input: actual, reason: RevokeReason::ConnectionLost } if actual == input)
        );
    } else {
        assert!(
            delivery.accepted.await.is_err(),
            "no replacement must close the prepared acceptance"
        );
        assert!(
            delivery.terminal.await.is_err(),
            "no replacement must close the terminal waiter"
        );
    }
    assert_eq!(rig.wire_receipt()["prompts"], serde_json::json!([]));
    host.shutdown().await.expect("drain the fixture");
}

#[tokio::test(flavor = "current_thread")]
async fn real_create_result_wakes_with_the_adopted_owner_already_live() {
    use provider_session_machines::session_owner::SessionSnapshot;
    use std::{
        future::Future as _,
        sync::{Arc, Mutex},
        task::{Context, Wake, Waker},
    };
    use tokio::sync::{oneshot, watch};

    struct ObserveCreateWake {
        snapshot: watch::Receiver<SessionSnapshot>,
        observed: Mutex<Option<oneshot::Sender<SessionLifecycle>>>,
    }

    impl Wake for ObserveCreateWake {
        fn wake(self: Arc<Self>) {
            if let Some(observed) = self.observed.lock().expect("wake observer").take() {
                let _observer_gone = observed.send(self.snapshot.borrow().lifecycle);
            }
        }
    }

    let rig = RestoreRig::new();
    let host = rig.open_host("restored-in-auto").await;
    let runtime = host
        .runtime_for(&rig.target.endpoint)
        .expect("provider runtime");
    let (before_adoption, release_create) = runtime.preparation_fixture.pause_create();
    let door = host
        .owner_door(&rig.target.endpoint)
        .expect("real owner door");
    let operation = OperationId::generate();
    let create = door.create_with_harness(
        rig.target.endpoint.clone(),
        operation.clone(),
        provider_session_machines::harness_lifecycle::OpenSetup {
            cwd: rig.root.path().to_owned(),
            scratch_folder: None,
            work_roots: Default::default(),
        },
        Default::default(),
        Default::default(),
    );
    tokio::pin!(create);
    tokio::select! {
        reached = before_adoption => reached.expect("hold create after provider ID"),
        _ = &mut create => panic!("create settled before the provider ID hold"),
    };
    let snapshot = door
        .provisional_snapshots_for_test(&operation)
        .expect("provisional owner watch");
    assert!(matches!(
        snapshot.borrow().lifecycle,
        SessionLifecycle::NotLoaded { .. }
    ));
    let (observed, at_wake) = oneshot::channel();
    {
        let waker = Waker::from(Arc::new(ObserveCreateWake {
            snapshot,
            observed: Mutex::new(Some(observed)),
        }));
        assert!(
            create
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending(),
            "register at the actual create-result receiver while adoption is held"
        );
    }
    release_create
        .send(())
        .expect("allow adoption and create settlement");
    let at_wake = at_wake
        .await
        .expect("the real result sender wakes its caller");
    let Ok(created) = create.await else {
        panic!("create did not complete");
    };
    assert!(!created.session.0.is_empty());
    assert!(
        matches!(at_wake, SessionLifecycle::Live { .. }),
        "Host create result woke its caller before the adopted snapshot was live: {at_wake:?}"
    );
    host.shutdown().await.expect("drain the fixture");
}
