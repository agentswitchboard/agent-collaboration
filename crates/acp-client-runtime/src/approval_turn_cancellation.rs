//! What a running turn holds for its cancellation: the turn's approval
//! context, its cancellation token and the barrier its permission responses
//! drain through. The client's cancel and the session actor both use it.

use crate::{
    AcpRequestPayloads, AcpRequestPolicy, ApprovalPortOutcome, ExternalProviderRuntimeError,
    acp_transport::TransportRun, interaction_port::observe_cancellation, owner_wire::OwnerAcpWire,
};
use provider_session_machines::session_owner::{
    CancelDrainReport, DispatchAuthorization, DispatchResult, PendingTurnResponse,
};
use session_event_model::{ApprovalRequest, QuestionRequest, QuestionResponse};
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use switchboard_harness_contracts::{
    ApprovalContext, ApprovalLease, ApprovalLeaseRefusalReason, CancelDrainStart,
    CancelReservationRefusal, Decision, ExactScope, ExactTurn, ReplyAdmission, RequestCancellation,
    RequestDecisionRefusal, RunRetirement, StopRequestsCause, TurnMembership, WorkReply,
    WriterState,
};
use tokio_util::{
    sync::CancellationToken,
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};

#[cfg(test)]
#[path = "approval_turn_cancellation_test.rs"]
mod tests;

#[path = "approval_response_barrier.rs"]
mod response_barrier;
use response_barrier::{PermissionResponseBarrier, PermissionResponseGuard};

#[derive(Default)]
struct PolicyStop {
    started: AtomicBool,
    finished: CancellationToken,
}

pub(crate) struct ActiveApprovalContext<P: AcpRequestPayloads> {
    pub(crate) approval: Arc<P::Context>,
    pub(crate) operation_id: Arc<P::OperationId>,
    pub(crate) generation: provider_session_machines::SessionGeneration,
    pub(crate) incarnation: provider_session_machines::session_owner::OwnerIncarnation,
    pub(crate) turn: Option<provider_session_machines::session_turn_machine::TurnId>,
    pub(crate) cancelling: CancellationToken,
    responses: Arc<PermissionResponseBarrier>,
    stop: Arc<PolicyStop>,
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
            stop: Arc::clone(&self.stop),
        }
    }
}

impl<P: AcpRequestPayloads> ActiveApprovalContext<P> {
    fn exact_turn(&self) -> Option<ExactTurn> {
        Some(ExactTurn {
            run: TransportRun {
                incarnation: self.incarnation,
                generation: self.generation,
            },
            turn: self.turn?,
        })
    }

    pub(crate) fn track_response(
        &self,
        method: &'static str,
        id: &impl serde::Serialize,
        peer: agent_client_protocol::RequestCancellation,
    ) -> Option<PermissionResponseGuard> {
        self.responses.track(method, id, peer)
    }

    pub(crate) fn managed_refusal(
        &self,
        policy: &AcpRequestPolicy<P>,
    ) -> Option<session_event_model::managed_interaction_refusal::ManagedInteractionRefusal> {
        self.responses
            .is_open()
            .then(|| policy.managed_refusal())
            .flatten()
    }

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
            context: self.approval.as_ref().clone(),
        }
    }

    fn policy_cancellation(&self, request: CancellationToken) -> RequestCancellation {
        RequestCancellation {
            turn: observe_cancellation(self.cancelling.clone()),
            request: observe_cancellation(request),
        }
    }

    pub(crate) async fn decide_approval(
        &self,
        policy: &AcpRequestPolicy<P>,
        request: ApprovalRequest,
        peer: agent_client_protocol::RequestCancellation,
    ) -> ApprovalPortOutcome {
        let cancellation = CancellationToken::new();
        if !self.responses.is_open() {
            return ApprovalPortOutcome::Cancelled;
        }
        let decision = policy.decide_approval(
            self.policy_context(),
            request,
            self.policy_cancellation(cancellation.clone()),
        );
        self.wait_for_decision(decision, peer, cancellation)
            .await
            .unwrap_or(ApprovalPortOutcome::Cancelled)
    }

    pub(crate) async fn decide_question(
        &self,
        policy: &AcpRequestPolicy<P>,
        request: QuestionRequest,
        peer: agent_client_protocol::RequestCancellation,
    ) -> QuestionResponse {
        let cancellation = CancellationToken::new();
        if !self.responses.is_open() {
            return QuestionResponse::Cancelled;
        }
        let decision = policy.decide_question(
            self.policy_context(),
            request,
            self.policy_cancellation(cancellation.clone()),
        );
        self.wait_for_decision(decision, peer, cancellation)
            .await
            .unwrap_or(QuestionResponse::Cancelled)
    }

    async fn wait_for_decision<Answer>(
        &self,
        decision: impl Future<Output = Decision<Answer>>,
        peer: agent_client_protocol::RequestCancellation,
        request: CancellationToken,
    ) -> Option<Answer> {
        let scope_stopped = async {
            self.responses.ended().await;
            self.stop.finished.cancelled().await;
        };
        tokio::pin!(decision, scope_stopped);
        let decision = tokio::select! {
            biased;
            () = peer.cancelled() => {
                request.cancel();
                tokio::select! {
                    biased;
                    _ = &mut decision => {}
                    () = &mut scope_stopped => {}
                }
                return None;
            }
            decision = &mut decision => decision,
            () = &mut scope_stopped => return None,
        };
        match decision {
            Decision::Answer(answer) => Some(answer),
            // Even a standing policy answer leaves ACP owing this new wire
            // request a response. Decline it without touching that answer.
            Decision::Refused(
                RequestDecisionRefusal::AlreadyAnswered
                | RequestDecisionRefusal::AnsweredElsewhere
                | RequestDecisionRefusal::NoOpenScope
                | RequestDecisionRefusal::ScopeEnded,
            ) => None,
        }
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
    bindings: OwnerApprovalBindings<P>,
    provider_session_id: String,
    responses: Arc<PermissionResponseBarrier>,
}

impl<P: AcpRequestPayloads> Drop for ApprovalContextGuard<P> {
    fn drop(&mut self) {
        self.bindings.end_matching(
            &self.provider_session_id,
            |context| Arc::ptr_eq(&context.responses, &self.responses),
            StopRequestsCause::TurnEnded,
        );
    }
}

pub struct OwnerApprovalBindings<P: AcpRequestPayloads> {
    pub(crate) active: Arc<Mutex<HashMap<String, ActiveApprovalContext<P>>>>,
    request_policy: Arc<AcpRequestPolicy<P>>,
    retirement: CancellationToken,
    closing: Arc<AtomicBool>,
    tasks: TaskTracker,
}

impl<P: AcpRequestPayloads> Clone for OwnerApprovalBindings<P> {
    fn clone(&self) -> Self {
        Self {
            active: Arc::clone(&self.active),
            request_policy: Arc::clone(&self.request_policy),
            retirement: self.retirement.clone(),
            closing: Arc::clone(&self.closing),
            tasks: self.tasks.clone(),
        }
    }
}

pub struct OwnerApprovalLease<P: AcpRequestPayloads> {
    _lease: ApprovalContextGuard<P>,
}

impl<P: AcpRequestPayloads> ApprovalLease for OwnerApprovalLease<P> {}

impl<P: AcpRequestPayloads> OwnerApprovalBindings<P> {
    #[cfg(feature = "test-support")]
    pub(crate) fn policy_operation_id(&self, context: &P::Context) -> P::OperationId {
        self.request_policy.operation_id(context)
    }

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
        retirement: CancellationToken,
    ) -> Self {
        Self {
            active,
            request_policy,
            retirement,
            closing: Arc::default(),
            tasks: TaskTracker::new(),
        }
    }

    /// The SDK can stop polling its tasks on EOF. Keep the admitted callback
    /// alive until ACP has stopped its policy and settled the responder.
    pub(crate) fn spawn_callback(
        &self,
        connection: &agent_client_protocol::ConnectionTo<agent_client_protocol::Agent>,
        callback: impl Future<Output = Result<(), agent_client_protocol::Error>> + Send + 'static,
    ) -> Result<(), agent_client_protocol::Error> {
        let tracked = self.tasks.spawn(callback);
        let retirement = self.retirement.clone();
        connection.spawn(async move {
            let result = match tracked.await {
                Ok(result) => result,
                Err(_) => Err(agent_client_protocol::Error::internal_error()),
            };
            result.inspect_err(|_| retirement.cancel())
        })
    }

    pub(crate) fn stop_connection(&self) {
        let closed: Vec<_> = {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.closing.store(true, Ordering::Relaxed);
            active
                .drain()
                .map(|(_, context)| self.close_context(context, StopRequestsCause::ConnectionLost))
                .collect()
        };
        for scope in closed {
            scope.finish();
        }
    }

    pub(crate) async fn drain_connection(&self) {
        self.tasks.close();
        self.tasks.wait().await;
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn observe_scope_drain(
        &self,
        session: &str,
    ) -> Option<(Vec<PendingTurnResponse>, impl Future<Output = ()> + use<P>)> {
        let responses = Arc::clone(&self.active.lock().ok()?.get(session)?.responses);
        let pending = responses.pending_responses();
        Some((pending, async move { responses.wait_drained().await }))
    }

    #[cfg(feature = "test-observation")]
    pub(crate) fn scope_lock_probe(
        &self,
        session: &str,
    ) -> Option<impl Fn() -> bool + Send + Sync + use<P>> {
        let scopes = self.active.clone();
        let responses = Arc::clone(&scopes.lock().ok()?.get(session)?.responses);
        Some(move || scopes.try_lock().is_ok() && responses.lock_available_for_test())
    }

    fn end_turn(&self, session: &str, turn: ExactTurn) {
        self.end_matching(
            session,
            |context| context.exact_turn() == Some(turn),
            StopRequestsCause::TurnEnded,
        );
    }

    pub(crate) fn apply_membership(
        &self,
        session: &str,
        incarnation: provider_session_machines::session_owner::OwnerIncarnation,
        change: TurnMembership,
    ) {
        match change {
            TurnMembership::End { run, turn } | TurnMembership::RemoveOnRevoke { run, turn } => {
                self.end_turn(
                    session,
                    ExactTurn {
                        run: TransportRun {
                            incarnation,
                            generation: run,
                        },
                        turn,
                    },
                );
            }
            TurnMembership::Retire(generation) => {
                self.retire_run(
                    session,
                    TransportRun {
                        incarnation,
                        generation,
                    },
                );
            }
            TurnMembership::Provisional { .. } | TurnMembership::ObservedEligible { .. } => {}
        }
    }

    pub(crate) fn retire_run(&self, session: &str, run: TransportRun) {
        self.end_matching(
            session,
            |context| {
                context.incarnation == run.incarnation && context.generation == run.generation
            },
            StopRequestsCause::GenerationRetired,
        );
    }

    fn end_matching(
        &self,
        session: &str,
        matches_scope: impl FnOnce(&ActiveApprovalContext<P>) -> bool,
        cause: StopRequestsCause,
    ) {
        let closed = {
            let mut active = self
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !active.get(session).is_some_and(matches_scope) {
                return;
            }
            let Some(context) = active.remove(session) else {
                return;
            };
            let cause = if self.retirement.is_cancelled() {
                StopRequestsCause::ConnectionLost
            } else {
                cause
            };
            self.close_context(context, cause)
        };
        closed.finish();
    }

    fn close_context(
        &self,
        context: ActiveApprovalContext<P>,
        cause: StopRequestsCause,
    ) -> ClosingApprovalScope<P> {
        let had_pending = context.responses.close();
        // Reserve the join before releasing the active map. Connection
        // teardown must see a retirement's work even before it can be woken.
        let stop = if had_pending {
            self.drain(&context).and_then(|drain| {
                let reservation = drain.reserve_policy_stop(true)?;
                Some((drain, reservation, cause))
            })
        } else {
            None
        };
        ClosingApprovalScope { context, stop }
    }

    fn drain(&self, context: &ActiveApprovalContext<P>) -> Option<OwnerApprovalDrain<P>> {
        Some(OwnerApprovalDrain {
            turn: context.exact_turn()?,
            cancelling: context.cancelling.clone(),
            approval: Arc::clone(&context.approval),
            request_policy: Arc::clone(&self.request_policy),
            responses: Arc::clone(&context.responses),
            stop: Arc::clone(&context.stop),
            tasks: self.tasks.clone(),
        })
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
        {
            let active = self
                .active
                .lock()
                .map_err(|_| ApprovalLeaseRefusalReason::ContextUnavailable)?;
            self.check_install(&active, &provider_session_id)?;
        }
        let operation_id = Arc::new(self.request_policy.operation_id(&context));
        self.request_policy
            .bind_owner_scope(&mut context, ExactTurn { run, turn });
        let approval = Arc::new(context);
        let responses = Arc::new(PermissionResponseBarrier::default());
        let mut active = self
            .active
            .lock()
            .map_err(|_| ApprovalLeaseRefusalReason::ContextUnavailable)?;
        self.check_install(&active, &provider_session_id)?;
        let displaced = active.insert(
            provider_session_id.clone(),
            ActiveApprovalContext {
                approval,
                operation_id,
                generation: run.generation,
                incarnation: run.incarnation,
                turn: Some(turn),
                cancelling: tokio_util::sync::CancellationToken::new(),
                responses: Arc::clone(&responses),
                stop: Arc::default(),
            },
        );
        drop(active);
        drop(displaced);
        Ok(OwnerApprovalLease {
            _lease: ApprovalContextGuard {
                bindings: self.clone(),
                provider_session_id,
                responses,
            },
        })
    }

    fn check_install(
        &self,
        active: &HashMap<String, ActiveApprovalContext<P>>,
        session: &str,
    ) -> Result<(), ApprovalLeaseRefusalReason> {
        if self.closing.load(Ordering::Relaxed) || self.retirement.is_cancelled() {
            return Err(ApprovalLeaseRefusalReason::ContextUnavailable);
        }
        if active.contains_key(session) {
            return Err(ApprovalLeaseRefusalReason::AlreadyActive);
        }
        Ok(())
    }

    pub fn begin_cancel(
        &self,
        provider_session_id: &str,
        authorization: &DispatchAuthorization,
    ) -> Option<OwnerApprovalDrain<P>> {
        let provider_session_machines::session_owner::AuthorizationTarget::ExistingTurn { turn } =
            authorization.target
        else {
            return None;
        };
        let exact = ExactTurn {
            run: TransportRun {
                incarnation: authorization.incarnation,
                generation: authorization.run,
            },
            turn,
        };
        let drain = {
            let contexts = self.active.lock().ok()?;
            let context = contexts.get(provider_session_id)?;
            (context.exact_turn() == Some(exact))
                .then(|| self.drain(context))
                .flatten()?
        };
        drain.mark_cancelling();
        Some(drain)
    }
}

struct ClosingApprovalScope<P: AcpRequestPayloads> {
    context: ActiveApprovalContext<P>,
    stop: Option<(OwnerApprovalDrain<P>, TaskTrackerToken, StopRequestsCause)>,
}

impl<P: AcpRequestPayloads> ClosingApprovalScope<P> {
    fn finish(self) {
        self.context.cancelling.cancel();
        self.context.responses.signal_end();
        if let Some((drain, reservation, cause)) = self.stop {
            drain.spawn_policy_stop(reservation, cause);
        }
    }
}

pub struct OwnerApprovalDrain<P: AcpRequestPayloads> {
    turn: ExactTurn,
    pub(crate) cancelling: CancellationToken,
    pub(crate) approval: Arc<P::Context>,
    pub(crate) request_policy: Arc<AcpRequestPolicy<P>>,
    responses: Arc<PermissionResponseBarrier>,
    stop: Arc<PolicyStop>,
    tasks: TaskTracker,
}

impl<P: AcpRequestPayloads> OwnerApprovalDrain<P> {
    pub(crate) fn mark_cancelling(&self) {
        self.cancelling.cancel();
    }

    pub async fn settle_pending_approvals(&self) {
        if let Some(reservation) = self.reserve_policy_stop(false) {
            self.spawn_policy_stop(reservation, StopRequestsCause::TurnCancelled);
        }
        tokio::select! {
            () = self.stop.finished.cancelled() => {}
            () = self.responses.ended() => {}
        }
        self.responses.wait_drained().await;
    }

    fn reserve_policy_stop(&self, had_pending_at_close: bool) -> Option<TaskTrackerToken> {
        self.responses
            .reserve_stop(had_pending_at_close, || {
                (!self.stop.started.swap(true, Ordering::Relaxed)).then(|| self.tasks.token())
            })
            .flatten()
    }

    fn spawn_policy_stop(&self, reservation: TaskTrackerToken, cause: StopRequestsCause) {
        let policy = Arc::clone(&self.request_policy);
        let context = Arc::clone(&self.approval);
        let turn = self.turn;
        let finished = self.stop.finished.clone().drop_guard();
        let responses = Arc::clone(&self.responses);
        tokio::spawn(async move {
            let _tracked = reservation;
            policy.stop_all(context.as_ref().clone(), turn, cause).await;
            drop(finished);
            responses.wait_drained().await;
        });
    }

    #[must_use]
    pub fn pending_responses(&self) -> Vec<PendingTurnResponse> {
        self.responses.pending_responses()
    }
}

/// Per-owner executor for the exact turn's outstanding ACP responses.
pub(crate) struct OwnerCancelDrainExecutor<P: AcpRequestPayloads> {
    approvals: OwnerApprovalBindings<P>,
    pending: BTreeMap<DispatchAuthorization, Arc<OwnerApprovalDrain<P>>>,
}

enum CancelDrainExit {
    Finished,
    Relinquished,
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
            let exit = tokio::select! {
                biased;
                () = drain.settle_pending_approvals() => CancelDrainExit::Finished,
                () = retired => CancelDrainExit::Relinquished,
            };
            match exit {
                CancelDrainExit::Finished => completion.send(WorkReply::Dispatch(
                    DispatchResult::CancelDrainFinished(authorization),
                )),
                CancelDrainExit::Relinquished => drop(completion),
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
