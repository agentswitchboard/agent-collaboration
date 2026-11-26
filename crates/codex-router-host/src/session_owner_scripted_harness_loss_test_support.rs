//! Pending policy request and three real completion ports for a scripted loss.

use super::{ScriptedHarness, lock};
use crate::{ExternalProviderApprovalContext, host_request_policy::HostRequestPayloads};
use provider_session_machines::{
    SessionGeneration,
    harness_lifecycle::{EventRefusal, SessionLifecycleFact, WorkReply},
    session_owner::{
        ApprovalContext, ApprovalPortOutcome, Decision, ExactScope, ExactTurn, OwnerIncarnation,
        RequestCancellation, RequestCancellationSignal, RequestFuture, RequestPolicy,
        StopRequestsCause,
    },
};
use session_event_model::{
    ApprovalChoice, ApprovalEffect, ApprovalRequest, ApprovalScope, OfferedOption, OfferedOptionId,
    OfferedOptions, OptionsOrigin,
};
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub(in crate::session_owner_scripted_harness_test) struct PolicySignal(
    pub(in crate::session_owner_scripted_harness_test) CancellationToken,
);

impl RequestCancellationSignal for PolicySignal {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }

    fn cancelled(&self) -> RequestFuture<'_, ()> {
        Box::pin(self.0.cancelled())
    }
}

pub(super) struct PendingRequest {
    policy: Arc<dyn RequestPolicy<HostRequestPayloads>>,
    context: ExternalProviderApprovalContext,
    turn: ExactTurn,
    cancellation: CancellationToken,
    response: oneshot::Receiver<Decision<ApprovalPortOutcome>>,
}

impl PendingRequest {
    pub(super) fn start(
        tasks: &TaskTracker,
        policy: Arc<dyn RequestPolicy<HostRequestPayloads>>,
        turn: ExactTurn,
        context: ExternalProviderApprovalContext,
    ) -> Self {
        let (respond, response) = oneshot::channel();
        let decide = policy.clone();
        let cancellation = CancellationToken::new();
        let observed = RequestCancellation {
            turn: Arc::new(PolicySignal(cancellation.clone())),
            request: Arc::new(PolicySignal(CancellationToken::new())),
        };
        let request_context = ApprovalContext {
            scope: ExactScope::Turn(turn),
            context: context.clone(),
        };
        tasks.spawn(async move {
            let decision = decide
                .decide_approval(
                    request_context,
                    ApprovalRequest {
                        request_id: "pending-approval".into(),
                        title: "Attempt held in the scripted harness".into(),
                        description: None,
                        subject: None,
                        options_origin: OptionsOrigin::AgentOffered,
                        options: OfferedOptions::new(vec![OfferedOption {
                            option_id: OfferedOptionId::new("allow".to_owned())
                                .expect("fixture option ID"),
                            label: "Allow once".into(),
                            choice: ApprovalChoice::new(ApprovalEffect::Allow, ApprovalScope::Once),
                        }])
                        .expect("fixture carries one valid choice"),
                    },
                    observed,
                )
                .await;
            let _harness_stopped_waiting = respond.send(decision);
        });
        Self {
            policy,
            context,
            turn,
            cancellation,
            response,
        }
    }

    async fn stop_and_drain(
        self,
    ) -> Result<Decision<ApprovalPortOutcome>, oneshot::error::RecvError> {
        self.cancellation.cancel();
        self.policy
            .stop_all(self.context, self.turn, StopRequestsCause::TurnCancelled)
            .await;
        self.response.await
    }
}

pub(in crate::session_owner_scripted_harness_test) struct LossReceipt {
    pub(in crate::session_owner_scripted_harness_test) owner_admission: Result<(), EventRefusal>,
    pub(in crate::session_owner_scripted_harness_test) reader_admission: Result<(), EventRefusal>,
    pub(in crate::session_owner_scripted_harness_test) drained: Decision<ApprovalPortOutcome>,
}

impl ScriptedHarness {
    pub(in crate::session_owner_scripted_harness_test) async fn emit_competing_loss(
        &self,
        incarnation: OwnerIncarnation,
        run: SessionGeneration,
    ) -> Option<LossReceipt> {
        let owner = self.owner_events(incarnation)?;
        let reader = self.run_events(incarnation)?;
        let (taken, reply) = self.take_prompt_reply(incarnation)?;
        assert_eq!(run, taken.run);
        let state = lock(&self.owners).get(&incarnation)?.clone();
        let pending = lock(&state).pending_request.take()?;
        let (owner_admission, reader_admission, drained) = tokio::join!(
            owner.report_harness_gone(),
            reader.send(SessionLifecycleFact::RunObservationFailed { run }),
            async {
                reply.send(WorkReply::ConnectionLost);
                pending.stop_and_drain().await
            },
        );
        Some(LossReceipt {
            owner_admission,
            reader_admission,
            drained: drained.ok()?,
        })
    }
}
