//! A pending policy decision across concurrent owner, work, and reader loss.

use super::*;
use crate::{
    external_provider_approval_context::OwnerApprovalScope,
    host_request_policy::HostRequestPayloads,
};
use provider_session_machines::session_owner::{
    ApprovalContext, ApprovalPortOutcome, Decision, ExactScope, RequestCancellation,
    RequestCancellationSignal, RequestFuture, RequestPolicy, StopRequestsCause,
};
use script::PolicySignal;
use session_event_model::{
    ApprovalRequest, QuestionRequest, QuestionResponse,
    managed_interaction_refusal::ManagedInteractionRefusal,
};

#[derive(Debug, Eq, PartialEq)]
enum RequestWitness {
    Pending(ExactTurn, OperationId),
    Stopped(ExactTurn, OperationId),
    Answered,
}

struct HeldPolicy(mpsc::UnboundedSender<RequestWitness>);

impl RequestPolicy<HostRequestPayloads> for HeldPolicy {
    fn decide_approval(
        &self,
        context: ApprovalContext<ExternalProviderApprovalContext>,
        request: ApprovalRequest,
        cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<ApprovalPortOutcome>> {
        Box::pin(async move {
            let ExactScope::Turn(turn) = context.scope else {
                panic!("approval has a turn lease")
            };
            assert_eq!(request.request_id, "pending-approval");
            assert!(!cancellation.turn.is_cancelled());
            assert!(!cancellation.request.is_cancelled());
            self.0
                .send(RequestWitness::Pending(turn, context.context.operation_id))
                .expect("observe a policy request suspended on its cancellation signal");
            cancellation.turn.cancelled().await;
            assert!(
                !cancellation.request.is_cancelled(),
                "the agent did not withdraw the request"
            );
            self.0
                .send(RequestWitness::Answered)
                .expect("observe decision returned");
            Decision::Answer(ApprovalPortOutcome::Cancelled)
        })
    }

    fn decide_question(
        &self,
        _context: ApprovalContext<ExternalProviderApprovalContext>,
        _request: QuestionRequest,
        _cancellation: RequestCancellation,
    ) -> RequestFuture<'_, Decision<QuestionResponse>> {
        Box::pin(async { Decision::Answer(QuestionResponse::Cancelled) })
    }

    fn operation_id(&self, context: &ExternalProviderApprovalContext) -> OperationId {
        context.operation_id.clone()
    }

    fn binding_retirement(
        &self,
        context: &ExternalProviderApprovalContext,
    ) -> Arc<dyn RequestCancellationSignal> {
        Arc::new(PolicySignal(context.binding_retirement.clone()))
    }

    fn bind_owner_scope(&self, context: &mut ExternalProviderApprovalContext, turn: ExactTurn) {
        context.owner_scope = Some(OwnerApprovalScope {
            run: turn.run.generation,
            turn: turn.turn,
        });
    }

    fn managed_refusal(&self) -> Option<ManagedInteractionRefusal> {
        None
    }

    fn cancel_after_managed_refusal(
        &self,
        _context: ApprovalContext<ExternalProviderApprovalContext>,
    ) -> RequestFuture<'_, ()> {
        Box::pin(async {})
    }

    fn stop_all(
        &self,
        context: ExternalProviderApprovalContext,
        turn: ExactTurn,
        cause: StopRequestsCause,
    ) -> RequestFuture<'_, ()> {
        Box::pin(async move {
            assert_eq!(cause, StopRequestsCause::TurnCancelled);
            self.0
                .send(RequestWitness::Stopped(turn, context.operation_id))
                .expect("observe request stop");
        })
    }
}

#[tokio::test]
async fn competing_harness_loss_reports_settle_once_and_drain_pending_neutral_policy() {
    let (policy_events, mut witnessed) = mpsc::unbounded_channel();
    let setup = ScriptedSession::load_saved(ScriptControls {
        work: ScriptedWork::HoldPromptReply,
        policy: Some(Arc::new(HeldPolicy(policy_events))),
        ..Default::default()
    })
    .await;
    let owner = setup.owner();
    let address = owner.snapshots().borrow().owner;
    let foreground = setup.input(&owner, TurnId(Uuid::now_v7())).await;
    let operation = setup.provide_approval(&owner, &foreground);
    let expected_turn = ExactTurn {
        run: TransportRun {
            incarnation: address.incarnation,
            generation: foreground.generation,
        },
        turn: foreground.turn,
    };
    let active = start_delivery(
        &owner,
        foreground.clone(),
        SessionOwnerCommand::Prompt {
            input: foreground.clone(),
            admission: PromptAdmission::Direct,
        },
    )
    .await;
    assert_eq!(
        active.accepted.await.expect("foreground was taken"),
        InputAcceptance::Started
    );
    assert_eq!(
        witnessed.recv().await,
        Some(RequestWitness::Pending(expected_turn, operation.clone()))
    );
    let mut foreground_terminal = active.terminal;
    assert!(matches!(
        foreground_terminal.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    let queued = setup.input(&owner, TurnId(Uuid::now_v7())).await;
    let waiting = start_delivery(
        &owner,
        queued.clone(),
        SessionOwnerCommand::Prompt {
            input: queued.clone(),
            admission: PromptAdmission::Queue,
        },
    )
    .await;
    assert_eq!(
        waiting.accepted.await.expect("queue admission"),
        InputAcceptance::Queued
    );
    let target = ProviderIdentity::session_to_board(&setup.target).expect("board target");
    owner
        .snapshots()
        .wait_for(|snapshot| {
            snapshot.turn
                == (SessionTurn::Running {
                    turn: foreground.turn,
                })
                && snapshot.publication == PublicationStatus::Current
        })
        .await
        .expect("publish the running turn before loss");
    let before_loss = setup
        .hub
        .attach(target.clone())
        .await
        .expect("attach before loss");

    let loss = setup
        .script
        .emit_competing_loss(address.incarnation, foreground.generation)
        .await
        .expect("owner report, taken prompt reply, reader observation and request drain");
    assert_eq!(loss.owner_admission, Ok(()));
    assert_eq!(loss.reader_admission, Ok(()));
    assert!(matches!(
        loss.drained,
        Decision::Answer(ApprovalPortOutcome::Cancelled)
    ));
    // All three sends have enqueued. The owner drains its completion queue
    // before answering this command, even when the arrivals are already late.
    let barrier = owner
        .admit(SessionOwnerCommand::AcceptSettings {
            generation: foreground.generation,
        })
        .await
        .expect_err("the mailbox processed all three loss inputs before the command");
    assert!(matches!(barrier, OwnerDoorError::Refused { .. }));
    assert_eq!(
        witnessed.recv().await,
        Some(RequestWitness::Stopped(expected_turn, operation.clone()))
    );
    assert_eq!(witnessed.recv().await, Some(RequestWitness::Answered));
    assert!(matches!(
        witnessed.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let foreground_reply = foreground_terminal.await.expect("taken work settles");
    assert!(matches!(foreground_reply.reply,
        SessionOwnerReply::PromptFailed { input, remote: RemoteEffect::Unknown, .. }
        | SessionOwnerReply::InputFinished { input, remote: RemoteEffect::Unknown }
        if input == foreground));
    let waiting_reply = waiting.terminal.await.expect("queued work settles");
    assert!(
        matches!(waiting_reply.reply, SessionOwnerReply::InputUnsent { input, .. } if input == queued)
    );
    owner
        .snapshots()
        .wait_for(|snapshot| {
            snapshot.projection.state == SessionState::Unloaded
                && snapshot.publication == PublicationStatus::Current
        })
        .await
        .expect("publish the one loss");
    let history = setup
        .hub
        .attach(target.clone())
        .await
        .expect("retained session");
    assert_eq!(
        setup
            .hub
            .state(target)
            .await
            .expect("retained session state"),
        SessionState::Unloaded
    );
    assert_eq!(
        history
            .snapshot
            .iter()
            .filter(|entry| entry.sequence > before_loss.latest_issued_sequence)
            .filter(|entry| matches!(
                entry.event,
                SessionEvent::StateChanged {
                    state: SessionState::Unloaded
                }
            ))
            .count(),
        1
    );
    assert_eq!(
        history
            .snapshot
            .iter()
            .filter(|entry| entry.sequence > before_loss.latest_issued_sequence)
            .filter(|entry| matches!(entry.event,
        SessionEvent::TurnEnded { ref turn_id, .. } if turn_id == &foreground.turn.0.to_string()))
            .count(),
        1
    );
    let mut store = setup.fixture.store.lock().await;
    assert!(
        store
            .session_record(&setup.target)
            .await
            .expect("persisted session identity")
            .is_some()
    );
    let saved = store
        .inspect(&operation)
        .await
        .expect("taken row")
        .expect("taken operation remains");
    assert_eq!(saved.effect, ProviderOperationEffect::Unknown);
    drop(store);
    setup.door.shutdown().await;
}
