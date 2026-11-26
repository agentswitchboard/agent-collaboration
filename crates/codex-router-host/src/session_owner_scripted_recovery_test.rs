//! Replacement, turn, and loss witnesses through the production owner door.

use super::*;
use crate::{
    ExternalProviderApprovalContext,
    session_owner_calls::InputAcceptance,
    session_owner_door::{OwnerDeliveryHandle, OwnerDoorError, SessionOwnerAddress},
};
use collaboration_protocol::{
    ProviderOperationEffect, ProviderOperationStage, ProviderReconciliationState,
};
use provider_session_machines::{
    harness_lifecycle::{ExactTurn, SessionLifecycleFact, TransportRun},
    session_owner::{
        DeliveryInput, OwnerInput, OwnerInputEvidence, PromptAdmission, RemoteEffect, SteerOrigin,
    },
    session_turn_machine::{SessionTurn, TurnId},
};
use script::{ScriptControls, ScriptedWork};
use session_event_model::{InputId, PreparedPrompt, PromptContent, StopReason, TurnOutcome};
use std::collections::{BTreeMap, BTreeSet};
use switchboard_session_store::ProviderOperationStore;
use tokio::sync::{mpsc, oneshot};

#[path = "session_owner_scripted_activation_test.rs"]
mod activation;
#[path = "session_owner_scripted_competing_loss_test.rs"]
mod loss;

struct ScriptedSession {
    fixture: StoreFixture,
    door: Arc<SessionOwnerDoor>,
    script: Arc<ScriptedHarness>,
    observations: mpsc::UnboundedReceiver<ScriptObservation>,
    target: SessionRef,
    hub: Arc<ProviderSessionEventHub>,
    preparation: Arc<crate::session_owner_preparation_fixture::PreparationFixture>,
}

impl ScriptedSession {
    async fn with_saved_session(controls: ScriptControls) -> Self {
        let fixture = StoreFixture::new().await;
        let target = fixture.record_session(SCRIPTED_SESSION).await;
        let hub = Arc::new(ProviderSessionEventHub::new(
            fixture.store.clone(),
            fixture.clock.clone(),
        ));
        let mut harness = None;
        let preparation =
            Arc::new(crate::session_owner_preparation_fixture::PreparationFixture::default());
        let door = Arc::new(SessionOwnerDoor::new(
            None,
            |tasks| {
                let (script, observations) =
                    ScriptedHarness::with_controls(tasks, ReplayCapture::Complete, controls);
                harness = Some((script.clone(), observations));
                script
            },
            fixture.clock.clone(),
            fixture.store.clone(),
            hub.clone(),
            CancellationToken::new(),
            preparation.clone(),
        ));
        let (script, observations) = harness.expect("the production door constructed its harness");
        Self {
            fixture,
            door,
            script,
            observations,
            target,
            hub,
            preparation,
        }
    }

    async fn load_saved(controls: ScriptControls) -> Self {
        let mut setup = Self::with_saved_session(controls).await;
        let owner = setup
            .door
            .open(setup.target.clone(), Default::default())
            .expect("bind the saved session through the production door");
        setup.restore(&owner).await;
        setup
            .observe(|event| matches!(event, ScriptObservation::LiveReported))
            .await;
        setup
    }

    fn owner(&self) -> Arc<SessionOwnerAddress> {
        self.door
            .existing(&self.target)
            .expect("the registered owner")
    }

    async fn observe(&mut self, select: impl Fn(&ScriptObservation) -> bool) -> ScriptObservation {
        loop {
            let event = self.observations.recv().await.expect("script observation");
            if select(&event) {
                return event;
            }
        }
    }

    async fn restore(&self, owner: &SessionOwnerAddress) {
        let restored = self.restore_settlement(owner).await;
        assert!(
            matches!(&restored.reply, SessionOwnerReply::RestoreFinished(result) if matches!(result.disposition, RestoreDisposition::Restored { .. })),
            "exact restore reply was {:?}",
            restored.reply
        );
    }

    async fn restore_settlement(&self, owner: &SessionOwnerAddress) -> OwnedSettlement {
        let load = record_operation(
            &self.fixture,
            &self.target,
            ProviderOperationKind::ConversationLoad,
        )
        .await;
        let persisted = self
            .fixture
            .store
            .lock()
            .await
            .session_record(&self.target)
            .await
            .expect("read the retained session")
            .expect("saved configuration");
        owner
            .restore_explicit(load, RestoreVia::Load, Box::new(persisted))
            .await
            .expect("the owner returns a restore result")
    }

    async fn input(&self, owner: &SessionOwnerAddress, turn: TurnId) -> OwnerInput {
        let SessionLifecycle::Live { generation } = owner.snapshots().borrow().lifecycle else {
            panic!("test input needs a live owner");
        };
        OwnerInput {
            generation,
            input: InputId::generate(),
            operation: record_operation(
                &self.fixture,
                &self.target,
                ProviderOperationKind::ConversationPrompt,
            )
            .await,
            turn,
        }
    }

    fn provide_approval(&self, owner: &SessionOwnerAddress, input: &OwnerInput) -> OperationId {
        let operation_id =
            OperationId::try_from(input.operation.to_string()).expect("operation ID");
        let actor = ProviderIdentity::from(self.target.clone());
        owner.remember_approval(
            input.input.clone(),
            ExternalProviderApprovalContext {
                requester: actor.clone(),
                approver: actor,
                target: self.target.clone(),
                operation_id: operation_id.clone(),
                binding_generation: self.fixture.binding.generation.clone(),
                binding_retirement: CancellationToken::new(),
                owner_scope: None,
            },
        );
        operation_id
    }
}

async fn start_delivery(
    owner: &SessionOwnerAddress,
    input: OwnerInput,
    command: SessionOwnerCommand,
) -> OwnerDeliveryHandle {
    let content = PreparedPrompt::plain(vec![
        PromptContent::text("scripted input".into()).expect("fixture prompt"),
    ]);
    owner
        .start_delivery(input, content, command)
        .await
        .expect("owner admitted work")
}

async fn force_owner_failure(
    owner: &SessionOwnerAddress,
    generation: provider_session_machines::SessionGeneration,
) {
    let capacity = owner.hold_completion_capacity();
    let failure = owner
        .admit(SessionOwnerCommand::PlaceDelivery {
            input: OwnerInput {
                generation,
                input: InputId::generate(),
                operation: Uuid::now_v7(),
                turn: TurnId(Uuid::now_v7()),
            },
            delivery: DeliveryInput::Prompt {
                admission: PromptAdmission::Queue,
                selected: Vec::new(),
            },
        })
        .await
        .expect_err("the real owner cannot reserve a placement completion");
    assert!(matches!(failure, OwnerDoorError::OwnerStopped));
    drop(capacity);
}

#[tokio::test]
async fn bound_owner_receives_harness_loss_before_its_first_opening() {
    use provider_session_machines::session_owner::{
        EffectInProgress, SessionOwnerRefusal, SessionOwnerRejection,
    };

    let mut setup = ScriptedSession::with_saved_session(ScriptControls::default()).await;
    let owner = setup
        .door
        .open(setup.target.clone(), Default::default())
        .expect("bind the saved session's owner before any work");
    let address = owner.snapshots().borrow().owner;
    assert_eq!(
        setup.observations.recv().await,
        Some(ScriptObservation::Bound(address))
    );
    let events = setup
        .script
        .owner_events(address.incarnation)
        .expect("owner-wide event port at bind");
    events
        .report_harness_gone()
        .await
        .expect("enqueue loss before open");
    let refused = owner
        .admit(SessionOwnerCommand::Restore {
            attempt: LoadAttempt(Uuid::now_v7()),
            via: RestoreVia::Load,
        })
        .await
        .expect_err("the first opening cannot use a lost harness");
    assert!(matches!(
        refused,
        OwnerDoorError::Refused {
            rejection: SessionOwnerRejection::Refused(SessionOwnerRefusal::EffectInProgress(
                EffectInProgress::ProviderDisconnected,
            )),
            ..
        }
    ));
    assert!(matches!(
        setup.observations.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let target = ProviderIdentity::session_to_board(&setup.target).expect("board target");
    assert_eq!(
        setup.hub.state(target).await.expect("retained session"),
        SessionState::Unloaded
    );
    setup.door.shutdown().await;
}

#[tokio::test]
async fn scripted_owner_replacement_keeps_no_evidence_unknown_at_both_callers_and_on_disk() {
    let mut setup = ScriptedSession::load_saved(ScriptControls {
        work: ScriptedWork::UnavailableRecovery,
        ..Default::default()
    })
    .await;
    let old = setup.owner();
    let address = old.snapshots().borrow().owner;
    let input = setup.input(&old, TurnId(Uuid::now_v7())).await;
    let operation = OperationId::try_from(input.operation.to_string()).expect("operation ID");
    let mut caller = start_delivery(
        &old,
        input.clone(),
        SessionOwnerCommand::Prompt {
            input: input.clone(),
            admission: PromptAdmission::Direct,
        },
    )
    .await;
    let ScriptObservation::PromptHeld(authorization) = setup
        .observe(|event| matches!(event, ScriptObservation::PromptHeld(_)))
        .await
    else {
        panic!("the script did not observe the exact held prompt")
    };
    assert_eq!(authorization.input_id, input.input);
    assert_eq!(authorization.incarnation, address.incarnation);
    assert!(matches!(
        caller.accepted.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let before = setup
        .fixture
        .store
        .lock()
        .await
        .inspect(&operation)
        .await
        .expect("read mark")
        .expect("accepted mark");
    assert_eq!(before.stage, ProviderOperationStage::MayHaveDispatched);
    assert_eq!(before.effect, ProviderOperationEffect::Unknown);

    force_owner_failure(&old, input.generation).await;
    let recovered = setup
        .observe(|event| matches!(event, ScriptObservation::OwnerGivenUp { .. }))
        .await;
    assert_eq!(
        recovered,
        ScriptObservation::OwnerGivenUp {
            incarnation: address.incarnation,
            asked: BTreeSet::from([input.input.clone()]),
            evidence: BTreeMap::from([(
                input.input.clone(),
                OwnerInputEvidence::NoEvidence { slots: Vec::new() }
            )]),
        }
    );
    let replacement = setup
        .observe(|event| matches!(event, ScriptObservation::Bound(_)))
        .await;
    assert!(matches!(replacement, ScriptObservation::Bound(owner) if owner != address));
    assert_eq!(
        caller.accepted.await.expect("acceptance after recovery"),
        InputAcceptance::Unknown
    );
    let terminal = caller
        .terminal
        .await
        .expect("terminal reply after recovery");
    assert!(!terminal.saved);
    assert_eq!(
        terminal.reply,
        SessionOwnerReply::InputFinished {
            input,
            remote: RemoteEffect::Unknown
        }
    );
    let mut reopened = ProviderOperationStore::open(&setup.fixture.path)
        .await
        .expect("reopen SQLite");
    let after = reopened
        .inspect(&operation)
        .await
        .expect("read reopened row")
        .expect("mark survives");
    assert_eq!(after.stage, before.stage);
    assert_eq!(after.effect, before.effect);
    assert_eq!(
        after.reconciliation_state,
        ProviderReconciliationState::Unresolved
    );
    setup.door.shutdown().await;
}

#[tokio::test]
async fn stale_harness_loss_leaves_the_replacement_live_with_its_prompt_waiting() {
    let mut setup = ScriptedSession::load_saved(ScriptControls {
        work: ScriptedWork::HoldPromptReply,
        ..Default::default()
    })
    .await;
    let previous = setup.owner();
    let old = previous.snapshots().borrow().clone();
    let SessionLifecycle::Live { generation } = old.lifecycle else {
        panic!("loaded owner")
    };
    let stale = setup
        .script
        .owner_events(old.owner.incarnation)
        .expect("old bound event port");
    force_owner_failure(&previous, generation).await;
    setup
        .observe(|event| matches!(event, ScriptObservation::Bound(_)))
        .await;
    let replacement = setup.owner();
    let new = replacement.snapshots().borrow().owner;
    assert_ne!(new, old.owner);
    setup.restore(&replacement).await;
    setup
        .observe(|event| matches!(event, ScriptObservation::LiveReported))
        .await;
    let input = setup.input(&replacement, TurnId(Uuid::now_v7())).await;
    let caller = start_delivery(
        &replacement,
        input.clone(),
        SessionOwnerCommand::Prompt {
            input: input.clone(),
            admission: PromptAdmission::Direct,
        },
    )
    .await;
    assert_eq!(
        caller.accepted.await.expect("replacement took its prompt"),
        InputAcceptance::Started
    );
    let mut terminal = caller.terminal;

    assert!(
        stale.report_harness_gone().await.is_err(),
        "the old exact port has ended"
    );
    assert_eq!(
        replacement.snapshots().borrow().turn,
        SessionTurn::Running { turn: input.turn }
    );
    assert!(matches!(
        terminal.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let finished = setup
        .script
        .finish_prompt(new.incarnation)
        .expect("replacement still owns the reply");
    assert_eq!(finished.input_id, input.input);
    let reply = terminal
        .await
        .expect("replacement caller receives its own terminal");
    assert!(reply.saved);
    assert!(
        matches!(reply.reply, SessionOwnerReply::PromptCompleted { input: completed, .. } if completed == input)
    );
    setup.door.shutdown().await;
}

#[tokio::test]
async fn refused_observed_turn_lease_keeps_the_provider_started_turn_without_a_new_offer() {
    let mut setup = ScriptedSession::load_saved(ScriptControls {
        work: ScriptedWork::RefuseObservedLease,
        ..Default::default()
    })
    .await;
    let owner = setup.owner();
    let address = owner.snapshots().borrow().owner;
    let original = setup.input(&owner, TurnId(Uuid::now_v7())).await;
    let original_caller = start_delivery(
        &owner,
        original.clone(),
        SessionOwnerCommand::Prompt {
            input: original.clone(),
            admission: PromptAdmission::Direct,
        },
    )
    .await;
    assert_eq!(
        original_caller.accepted.await.expect("foreground taken"),
        InputAcceptance::Started
    );
    let steer = setup.input(&owner, original.turn).await;
    let operation = setup.provide_approval(&owner, &steer);
    let caller = start_delivery(
        &owner,
        steer.clone(),
        SessionOwnerCommand::Steer {
            input: steer.clone(),
            supported: true,
            origin: SteerOrigin::Named,
        },
    )
    .await;

    let ScriptObservation::SteerTaken(authorization, observed_turn) = setup
        .observe(|event| matches!(event, ScriptObservation::SteerTaken(..)))
        .await
    else {
        panic!("the script did not observe the exact steer take")
    };
    assert_eq!(authorization.input_id, steer.input);
    assert_ne!(observed_turn, original.turn);
    assert_eq!(
        setup
            .observations
            .recv()
            .await
            .expect("observed lease entry"),
        ScriptObservation::ObservedLeaseRefused {
            turn: ExactTurn {
                run: TransportRun {
                    incarnation: address.incarnation,
                    generation: steer.generation
                },
                turn: observed_turn,
            },
            operation,
        }
    );
    assert_eq!(
        caller.accepted.await.expect("steer accepted"),
        InputAcceptance::ProviderStarted
    );
    let terminal = caller.terminal.await.expect("steer terminal");
    assert!(
        matches!(terminal.reply, SessionOwnerReply::SteerStarted { turn, .. } if turn == observed_turn)
    );
    assert_eq!(
        owner.snapshots().borrow().turn,
        SessionTurn::Running {
            turn: observed_turn
        }
    );
    let target = ProviderIdentity::session_to_board(&setup.target).expect("board session");
    let mut replay = setup.hub.attach(target).await.expect("session replay");
    let mut events = std::mem::take(&mut replay.snapshot).into_iter();
    loop {
        let event = match events.next() {
            Some(event) => event,
            None => replay
                .receiver
                .recv()
                .await
                .expect("owner turn publication"),
        };
        if matches!(event.event, SessionEvent::TurnStarted { ref input_id, ref turn_id }
            if input_id == &steer.input && turn_id == &observed_turn.0.to_string())
        {
            break;
        }
    }
    assert!(
        matches!(
            setup.observations.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ),
        "no second work offer followed the observed lease refusal"
    );
    assert!(setup.script.finish_prompt(address.incarnation).is_some());
    original_caller
        .terminal
        .await
        .expect("release the previous prompt receipt");
    let run_events = setup
        .script
        .run_events(address.incarnation)
        .expect("bound run events");
    run_events
        .send(SessionLifecycleFact::ObservedTurnEnded {
            run: steer.generation,
            turn: observed_turn,
            outcome: TurnOutcome::Ended {
                stop_reason: StopReason::EndTurn,
                local_cause: None,
            },
        })
        .await
        .expect("end the provider-started turn");
    setup.door.shutdown().await;
}
