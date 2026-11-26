//! What a running turn holds for its cancellation: the turn's approval
//! context, its cancellation token and the barrier its permission responses
//! drain through. The client's cancel and the session actor both use it.

use crate::{
    AcpRequestPayloads, AcpRequestPolicy, ApprovalPortOutcome, ExternalProviderRuntimeError,
    InteractionFuture, acp_transport::TransportRun, interaction_port::observe_cancellation,
    owner_wire::OwnerAcpWire,
};
use provider_session_machines::session_owner::{
    CancelDrainReport, DispatchAuthorization, DispatchResult, PendingTurnResponse,
};
use session_event_model::{ApprovalRequest, QuestionRequest, QuestionResponse};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};
use switchboard_harness_contracts::{
    ApprovalContext, ApprovalLease, ApprovalLeaseRefusalReason, CancelDrainStart,
    CancelReservationRefusal, Decision, ExactScope, ExactTurn, ReplyAdmission, RequestCancellation,
    RunRetirement, StopRequestsCause, WorkReply, WriterState,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[cfg(test)]
#[path = "approval_turn_cancellation_test.rs"]
mod tests;

#[derive(Default)]
pub(crate) struct PermissionResponseBarrier {
    pending: Mutex<BTreeMap<PendingTurnResponse, usize>>,
    drained: tokio::sync::Notify,
}

impl PermissionResponseBarrier {
    fn pending(&self) -> MutexGuard<'_, BTreeMap<PendingTurnResponse, usize>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn track(
        self: &Arc<Self>,
        method: &'static str,
        id: &impl serde::Serialize,
    ) -> PermissionResponseGuard {
        let key = PendingTurnResponse {
            method,
            request_id: serde_json::to_string(id).unwrap_or_default(),
        };
        *self.pending().entry(key.clone()).or_default() += 1;
        PermissionResponseGuard {
            barrier: Arc::clone(self),
            key,
        }
    }

    async fn wait_drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.pending().is_empty() {
                return;
            }
            notified.await;
        }
    }
}

pub(crate) struct PermissionResponseGuard {
    barrier: Arc<PermissionResponseBarrier>,
    key: PendingTurnResponse,
}

impl Drop for PermissionResponseGuard {
    fn drop(&mut self) {
        let mut pending = self.barrier.pending();
        if let Some(count) = pending.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                pending.remove(&self.key);
            }
        }
        if pending.is_empty() {
            self.barrier.drained.notify_waiters();
        }
    }
}

pub(crate) struct ActiveApprovalContext<P: AcpRequestPayloads> {
    pub(crate) approval: P::Context,
    pub(crate) operation_id: P::OperationId,
    pub(crate) generation: provider_session_machines::SessionGeneration,
    pub(crate) incarnation: provider_session_machines::session_owner::OwnerIncarnation,
    pub(crate) turn: Option<provider_session_machines::session_turn_machine::TurnId>,
    pub(crate) cancelling: CancellationToken,
    pub(crate) responses: Arc<PermissionResponseBarrier>,
}

impl<P: AcpRequestPayloads> Clone for ActiveApprovalContext<P> {
    fn clone(&self) -> Self {
        Self {
            approval: self.approval.clone(),
            operation_id: self.operation_id.clone(),
            generation: self.generation,
            incarnation: self.incarnation,
            turn: self.turn,
            cancelling: self.cancelling.clone(),
            responses: Arc::clone(&self.responses),
        }
    }
}

impl<P: AcpRequestPayloads> ActiveApprovalContext<P> {
    fn policy_context(&self) -> ApprovalContext<P::Context> {
        let run = TransportRun {
            incarnation: self.incarnation,
            generation: self.generation,
        };
        ApprovalContext {
            scope: match self.turn {
                Some(turn) => ExactScope::Turn(ExactTurn { run, turn }),
                None => ExactScope::Session { run },
            },
            context: self.approval.clone(),
        }
    }

    fn policy_cancellation(&self, request: CancellationToken) -> RequestCancellation {
        RequestCancellation {
            turn: observe_cancellation(self.cancelling.clone()),
            request: observe_cancellation(request),
        }
    }

    pub(crate) fn decide_approval<'policy>(
        &self,
        policy: &'policy AcpRequestPolicy<P>,
        request: ApprovalRequest,
        cancellation: CancellationToken,
    ) -> InteractionFuture<'policy, ApprovalPortOutcome> {
        let decision = policy.decide_approval(
            self.policy_context(),
            request,
            self.policy_cancellation(cancellation),
        );
        Box::pin(async move {
            match decision.await {
                Decision::Answer(answer) => answer,
                Decision::Refused(_) => ApprovalPortOutcome::Cancelled,
            }
        })
    }

    pub(crate) fn decide_question<'policy>(
        &self,
        policy: &'policy AcpRequestPolicy<P>,
        request: QuestionRequest,
        cancellation: CancellationToken,
    ) -> InteractionFuture<'policy, QuestionResponse> {
        let decision = policy.decide_question(
            self.policy_context(),
            request,
            self.policy_cancellation(cancellation),
        );
        Box::pin(async move {
            match decision.await {
                Decision::Answer(answer) => answer,
                Decision::Refused(_) => QuestionResponse::Cancelled,
            }
        })
    }

    pub(crate) async fn cancel_after_refusal(
        self,
        request_policy: Arc<AcpRequestPolicy<P>>,
        refusal: session_event_model::managed_interaction_refusal::ManagedInteractionRefusal,
    ) {
        tracing::warn!(
            reason_code = refusal.code(),
            reason = refusal.reason(),
            "managed provider interaction refused"
        );
        request_policy
            .cancel_after_managed_refusal(self.policy_context())
            .await;
    }
}

struct ApprovalContextGuard<P: AcpRequestPayloads> {
    contexts: Arc<std::sync::Mutex<HashMap<String, ActiveApprovalContext<P>>>>,
    provider_session_id: String,
    operation_id: P::OperationId,
}

impl<P: AcpRequestPayloads> Drop for ApprovalContextGuard<P> {
    fn drop(&mut self) {
        if let Ok(mut contexts) = self.contexts.lock()
            && contexts
                .get(&self.provider_session_id)
                .is_some_and(|context| context.operation_id == self.operation_id)
        {
            contexts.remove(&self.provider_session_id);
        }
    }
}

pub struct OwnerApprovalBindings<P: AcpRequestPayloads> {
    active: Arc<Mutex<HashMap<String, ActiveApprovalContext<P>>>>,
    request_policy: Arc<AcpRequestPolicy<P>>,
}

impl<P: AcpRequestPayloads> Clone for OwnerApprovalBindings<P> {
    fn clone(&self) -> Self {
        Self {
            active: Arc::clone(&self.active),
            request_policy: Arc::clone(&self.request_policy),
        }
    }
}

pub struct OwnerApprovalLease<P: AcpRequestPayloads> {
    _lease: ApprovalContextGuard<P>,
}

impl<P: AcpRequestPayloads> ApprovalLease for OwnerApprovalLease<P> {}

impl<P: AcpRequestPayloads> OwnerApprovalBindings<P> {
    #[cfg(all(debug_assertions, feature = "test-observation"))]
    pub fn held_turn_for_test(
        &self,
        provider_session_id: &str,
    ) -> Option<provider_session_machines::session_turn_machine::TurnId> {
        self.active.lock().ok()?.get(provider_session_id)?.turn
    }

    pub(crate) fn new(
        active: Arc<Mutex<HashMap<String, ActiveApprovalContext<P>>>>,
        request_policy: Arc<AcpRequestPolicy<P>>,
    ) -> Self {
        Self {
            active,
            request_policy,
        }
    }

    pub fn install(
        &self,
        provider_session_id: String,
        run: TransportRun,
        turn: provider_session_machines::session_turn_machine::TurnId,
        context: P::Context,
    ) -> Result<OwnerApprovalLease<P>, ExternalProviderRuntimeError> {
        self.install_lease(provider_session_id, run, turn, context)
            .map_err(|reason| {
                ExternalProviderRuntimeError::Operation(
                    match reason {
                        ApprovalLeaseRefusalReason::ContextUnavailable => {
                            "provider approval context unavailable"
                        }
                        ApprovalLeaseRefusalReason::AlreadyActive => {
                            "a provider approval context is already active"
                        }
                    }
                    .to_owned(),
                )
            })
    }

    pub(crate) fn install_lease(
        &self,
        provider_session_id: String,
        run: TransportRun,
        turn: provider_session_machines::session_turn_machine::TurnId,
        mut context: P::Context,
    ) -> Result<OwnerApprovalLease<P>, ApprovalLeaseRefusalReason> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| ApprovalLeaseRefusalReason::ContextUnavailable)?;
        if active.contains_key(&provider_session_id) {
            return Err(ApprovalLeaseRefusalReason::AlreadyActive);
        }
        let operation_id = self.request_policy.operation_id(&context);
        self.request_policy
            .bind_owner_scope(&mut context, ExactTurn { run, turn });
        active.insert(
            provider_session_id.clone(),
            ActiveApprovalContext {
                approval: context,
                operation_id: operation_id.clone(),
                generation: run.generation,
                incarnation: run.incarnation,
                turn: Some(turn),
                cancelling: tokio_util::sync::CancellationToken::new(),
                responses: Arc::new(PermissionResponseBarrier::default()),
            },
        );
        Ok(OwnerApprovalLease {
            _lease: ApprovalContextGuard {
                contexts: Arc::clone(&self.active),
                provider_session_id,
                operation_id,
            },
        })
    }

    pub fn begin_cancel(
        &self,
        provider_session_id: &str,
        authorization: &DispatchAuthorization,
    ) -> Option<OwnerApprovalDrain<P>> {
        let drain = active_turn_cancellation(
            &self.active,
            &self.request_policy,
            provider_session_id,
            authorization,
        )?;
        drain.mark_cancelling();
        Some(drain)
    }
}

fn active_turn_cancellation<P: AcpRequestPayloads>(
    contexts: &Arc<Mutex<HashMap<String, ActiveApprovalContext<P>>>>,
    request_policy: &Arc<AcpRequestPolicy<P>>,
    provider_session_id: &str,
    authorization: &provider_session_machines::session_owner::DispatchAuthorization,
) -> Option<OwnerApprovalDrain<P>> {
    let provider_session_machines::session_owner::AuthorizationTarget::ExistingTurn { turn } =
        authorization.target
    else {
        return None;
    };
    contexts.lock().ok().and_then(|contexts| {
        contexts.get(provider_session_id).and_then(|context| {
            (context.incarnation == authorization.incarnation
                && context.generation == authorization.run
                && context.turn == Some(turn))
            .then(|| OwnerApprovalDrain {
                turn: ExactTurn {
                    run: TransportRun {
                        incarnation: authorization.incarnation,
                        generation: authorization.run,
                    },
                    turn,
                },
                cancelling: context.cancelling.clone(),
                approval: context.approval.clone(),
                request_policy: Arc::clone(request_policy),
                responses: Arc::clone(&context.responses),
            })
        })
    })
}

pub struct OwnerApprovalDrain<P: AcpRequestPayloads> {
    turn: ExactTurn,
    pub(crate) cancelling: CancellationToken,
    pub(crate) approval: P::Context,
    pub(crate) request_policy: Arc<AcpRequestPolicy<P>>,
    pub(crate) responses: Arc<PermissionResponseBarrier>,
}

impl<P: AcpRequestPayloads> OwnerApprovalDrain<P> {
    pub(crate) fn mark_cancelling(&self) {
        self.cancelling.cancel();
    }

    pub async fn settle_pending_approvals(&self) {
        self.request_policy
            .stop_all(
                self.approval.clone(),
                self.turn,
                StopRequestsCause::TurnCancelled,
            )
            .await;
        self.responses.wait_drained().await;
    }

    #[must_use]
    pub fn pending_responses(&self) -> Vec<PendingTurnResponse> {
        self.responses.pending().keys().cloned().collect()
    }
}

/// Per-owner executor for the exact turn's outstanding ACP responses.
pub(crate) struct OwnerCancelDrainExecutor<P: AcpRequestPayloads> {
    approvals: OwnerApprovalBindings<P>,
    pending: BTreeMap<DispatchAuthorization, Arc<OwnerApprovalDrain<P>>>,
}

impl<P: AcpRequestPayloads> OwnerCancelDrainExecutor<P> {
    pub fn new(approvals: OwnerApprovalBindings<P>) -> Self {
        Self {
            approvals,
            pending: BTreeMap::new(),
        }
    }

    pub(crate) fn begin_work(
        &mut self,
        session: &str,
        authorization: DispatchAuthorization,
        reserve_reply: &mut dyn FnMut() -> ReplyAdmission<WorkReply>,
        tasks: &TaskTracker,
        retired: RunRetirement,
    ) -> CancelDrainStart {
        let Some(drain) = self.approvals.begin_cancel(session, &authorization) else {
            return CancelDrainStart::AlreadyDrained;
        };
        let completion = match reserve_reply() {
            ReplyAdmission::Reserved(completion) => completion,
            ReplyAdmission::Refused(WriterState::Full) => {
                return CancelDrainStart::Refused(CancelReservationRefusal::Full);
            }
            ReplyAdmission::Refused(WriterState::Stopped) => {
                return CancelDrainStart::Refused(CancelReservationRefusal::Stopped);
            }
        };
        let drain = Arc::new(drain);
        self.pending.insert(authorization.clone(), drain.clone());
        tasks.spawn(async move {
            tokio::select! {
                () = retired => {}
                () = drain.settle_pending_approvals() => completion.send(
                    WorkReply::Dispatch(DispatchResult::CancelDrainFinished(authorization))
                ),
            }
        });
        CancelDrainStart::Waiting
    }

    pub fn take(
        &mut self,
        wire: &OwnerAcpWire,
        session: String,
        authorization: DispatchAuthorization,
        expired: bool,
    ) -> DispatchResult {
        let waiting = self.pending.remove(&authorization);
        let drain = if expired {
            CancelDrainReport::Expired {
                pending: waiting.map_or_else(Vec::new, |waiting| waiting.pending_responses()),
            }
        } else {
            CancelDrainReport::Drained
        };
        if wire.offer_cancel(authorization.clone(), session).is_err() {
            return DispatchResult::DefinitelyUnsent(authorization);
        }
        match wire.take(&authorization) {
            Ok(
                DispatchResult::Taken(authorization) | DispatchResult::AlreadyTaken(authorization),
            ) => DispatchResult::CancelTaken {
                authorization,
                drain,
            },
            Ok(other) => other,
            Err(_) => {
                let _rejected = wire.revoke(&authorization);
                DispatchResult::DefinitelyUnsent(authorization)
            }
        }
    }

    pub fn discard(&mut self, authorization: &DispatchAuthorization) {
        self.pending.remove(authorization);
    }
}
