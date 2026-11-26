//! Connection-task ownership and lifecycle for provider permission requests.

#[cfg(any(test, feature = "test-observation"))]
use super::ExternalProviderApprovalRefusalWarning;
use super::{ActiveApprovalContext, ExternalProviderApprovalRefusalReason};
use crate::{
    AcpRequestPayloads, AcpRequestPolicy, ApprovalPortOutcome, OwnerApprovalBindings,
    ProviderPersistenceTarget, map_permission_options, reviewed_approval_fields,
};
use agent_client_protocol::schema::v1::{
    RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome,
};
use agent_client_protocol::{Agent, ConnectionTo, Error, Responder};
use session_event_model::ApprovalRequest;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::RwLock;

#[cfg(any(test, feature = "test-support"))]
pub(crate) type MalformedApprovalCallback<Context> = Arc<
    dyn Fn(Context, crate::RefusedApprovalOffer) -> crate::InteractionFuture<'static, ()>
        + Send
        + Sync,
>;

#[cfg(any(test, feature = "test-support"))]
pub(crate) type MalformedApprovalObserver<P> = Arc<
    Mutex<
        Option<
            MalformedApprovalCallback<
                <P as switchboard_harness_contracts::RequestPolicyPayloads>::Context,
            >,
        >,
    >,
>;

pub(super) struct PermissionDispatchState<P: AcpRequestPayloads> {
    pub(super) request_policy: Arc<AcpRequestPolicy<P>>,
    pub(super) approval_bindings: OwnerApprovalBindings<P>,
    #[cfg(feature = "test-observation")]
    pub(super) frame_observation: Arc<crate::provider_frame_observation::ProviderFrameObservation>,
    #[cfg(any(test, feature = "test-support"))]
    pub(super) malformed_approval_observer: MalformedApprovalObserver<P>,
    pub(super) refusal_reasons:
        Arc<Mutex<HashMap<P::OperationId, ExternalProviderApprovalRefusalReason>>>,
    pub(super) endpoint_id: Arc<RwLock<Option<String>>>,
    pub(super) persistence_target: ProviderPersistenceTarget,
    #[cfg(any(test, feature = "test-observation"))]
    pub(super) refusal_warnings: Arc<Mutex<Vec<ExternalProviderApprovalRefusalWarning>>>,
    #[cfg(any(test, feature = "test-observation"))]
    pub(super) permission_outcome: Arc<std::sync::atomic::AtomicU8>,
}

pub(super) async fn record_permission_refusal<P: AcpRequestPayloads>(
    state: &PermissionDispatchState<P>,
    provider_session_id: &str,
    operation_id: Option<P::OperationId>,
    reason: ExternalProviderApprovalRefusalReason,
) {
    report_permission_refusal(state, provider_session_id, reason).await;
    if let Some(operation_id) = operation_id
        && let Ok(mut reasons) = state.refusal_reasons.lock()
    {
        reasons.insert(operation_id, reason);
    }
}

async fn report_permission_refusal<P: AcpRequestPayloads>(
    state: &PermissionDispatchState<P>,
    provider_session_id: &str,
    reason: ExternalProviderApprovalRefusalReason,
) {
    let endpoint = state
        .endpoint_id
        .read()
        .await
        .clone()
        .unwrap_or_else(|| "unknown".to_owned());
    tracing::warn!(
        endpoint = %endpoint,
        provider_session_id,
        method = "session/request_permission",
        reason_code = reason.code(),
        "provider permission request refused",
    );
    #[cfg(any(test, feature = "test-observation"))]
    if let Ok(mut warnings) = state.refusal_warnings.lock() {
        warnings.push(ExternalProviderApprovalRefusalWarning {
            endpoint,
            provider_session_id: provider_session_id.to_owned(),
            method: "session/request_permission",
            reason_code: reason,
        });
    }
}

pub(super) fn spawn_external_approval_dispatch<P: AcpRequestPayloads>(
    request: RequestPermissionRequest,
    responder: Responder<RequestPermissionResponse>,
    connection: ConnectionTo<Agent>,
    context: Option<ActiveApprovalContext<P>>,
    state: PermissionDispatchState<P>,
) -> Result<(), Error> {
    let request_id = serde_json::to_string(&(request.session_id.0.as_ref(), responder.id()))
        .map_err(|_| Error::internal_error())?;
    let Some((context, response_guard)) = context.and_then(|context| {
        let guard = context.track_response(
            "session/request_permission",
            responder.id(),
            responder.cancellation(),
        )?;
        Some((context, guard))
    }) else {
        #[cfg(any(test, feature = "test-observation"))]
        state
            .permission_outcome
            .store(1, std::sync::atomic::Ordering::Relaxed);
        return responder.respond(RequestPermissionResponse::new(
            RequestPermissionOutcome::Cancelled,
        ));
    };
    let request_cancellation = responder.cancellation();
    let bindings = state.approval_bindings.clone();
    bindings.spawn_callback(&connection, async move {
        if let Some(refusal) = context.managed_refusal(state.request_policy.as_ref()) {
            #[cfg(feature = "test-observation")]
            let responder_id = responder.id().clone();
            let provider_session_id = request.session_id.0.to_string();
            let response: RequestPermissionResponse =
                serde_json::from_value(refusal.acp_permission())
                    .map_err(|_| Error::internal_error())?;
            let reason = ExternalProviderApprovalRefusalReason::Managed(refusal);
            let operation_id = context.operation_id.as_ref().clone();
            #[cfg(any(test, feature = "test-observation"))]
            state
                .permission_outcome
                .store(1, std::sync::atomic::Ordering::Relaxed);
            #[cfg(feature = "test-observation")]
            let queue_witness = state
                .frame_observation
                .before_permission_reply(responder.id(), request_cancellation.clone())
                .await;
            let declined = RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled);
            let (committed, queued) = {
                // Lock order is refusal reasons, then the response scope.
                // The peer can finish the prompt as soon as its reply is
                // queued. Keep the reader fenced through the record write,
                // with hashing outside the scope lock.
                let mut reasons = state.refusal_reasons.lock().ok();
                let (open, queued) = response_guard.commit(|open| {
                    (
                        open,
                        responder.respond(if open { response } else { declined }),
                    )
                });
                let committed = open && queued.is_ok();
                if committed && let Some(reasons) = reasons.as_mut() {
                    reasons.insert(operation_id, reason);
                }
                (committed, queued)
            };
            drop(response_guard);
            #[cfg(feature = "test-observation")]
            if queued.is_ok()
                && let Some(witness) = queue_witness
            {
                let _fixture = witness.send(());
            }
            if committed {
                report_permission_refusal(&state, &provider_session_id, reason).await;
                #[cfg(feature = "test-observation")]
                state
                    .frame_observation
                    .record_managed_permission_enqueue(responder_id);
                if !request_cancellation.is_cancelled() {
                    context
                        .cancel_after_refusal(Arc::clone(&state.request_policy), refusal)
                        .await;
                }
            }
            return queued;
        }
        let outcome = handle_contextual_permission_request(
            context,
            request,
            request_id,
            request_cancellation.clone(),
            &state,
        )
        .await;
        #[cfg(feature = "test-observation")]
        let queue_witness = state
            .frame_observation
            .before_permission_reply(responder.id(), request_cancellation)
            .await;
        let selected = matches!(outcome, RequestPermissionOutcome::Selected(_));
        let response = RequestPermissionResponse::new(outcome);
        let declined = RequestPermissionResponse::new(RequestPermissionOutcome::Cancelled);
        let (open, queued) = response_guard.commit(|open| {
            (
                open,
                responder.respond(if open { response } else { declined }),
            )
        });
        #[cfg(any(test, feature = "test-observation"))]
        state.permission_outcome.store(
            if open && selected { 2 } else { 1 },
            std::sync::atomic::Ordering::Relaxed,
        );
        #[cfg(not(any(test, feature = "test-observation")))]
        let _observation = (open, selected);
        #[cfg(feature = "test-observation")]
        if queued.is_ok()
            && let Some(witness) = queue_witness
        {
            let _fixture = witness.send(());
        }
        queued
    })
}

async fn handle_contextual_permission_request<P: AcpRequestPayloads>(
    context: ActiveApprovalContext<P>,
    request: RequestPermissionRequest,
    request_id: String,
    request_cancellation: agent_client_protocol::RequestCancellation,
    state: &PermissionDispatchState<P>,
) -> RequestPermissionOutcome {
    let provider_session_id = request.session_id.0.to_string();
    let reviewed = reviewed_approval_fields(&request.tool_call);
    let options = match map_permission_options(request.options, state.persistence_target) {
        Ok(options) => options,
        Err(refused) => {
            tracing::warn!(
                reason_code = refused.reason,
                option_count = refused.options.len(),
                "malformed ACP permission offer refused"
            );
            #[cfg(any(test, feature = "test-support"))]
            {
                let observer = state
                    .malformed_approval_observer
                    .lock()
                    .ok()
                    .and_then(|observer| observer.clone());
                if let Some(observer) = observer {
                    observer(
                        context.approval.as_ref().clone(),
                        crate::RefusedApprovalOffer {
                            request_id,
                            reason: refused.reason,
                            title: reviewed.title,
                            description: reviewed.description,
                            subject: reviewed.subject,
                            options: refused.options,
                        },
                    )
                    .await;
                }
            }
            return RequestPermissionOutcome::Cancelled;
        }
    };
    let canonical_request = ApprovalRequest {
        request_id,
        title: reviewed.title,
        description: reviewed.description,
        subject: reviewed.subject,
        options_origin: session_event_model::OptionsOrigin::AgentOffered,
        options,
    };
    let outcome = context
        .decide_approval(
            state.request_policy.as_ref(),
            canonical_request,
            request_cancellation,
        )
        .await;
    match outcome {
        ApprovalPortOutcome::Selected { option_id, .. } => {
            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option_id))
        }
        ApprovalPortOutcome::Cancelled => RequestPermissionOutcome::Cancelled,
        ApprovalPortOutcome::Unavailable => {
            record_permission_refusal(
                state,
                &provider_session_id,
                Some(context.operation_id.as_ref().clone()),
                ExternalProviderApprovalRefusalReason::ApprovalBrokerUnavailable,
            )
            .await;
            RequestPermissionOutcome::Cancelled
        }
    }
}
