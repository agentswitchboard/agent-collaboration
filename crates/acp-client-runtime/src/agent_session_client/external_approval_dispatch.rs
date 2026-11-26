//! Connection-task ownership and lifecycle for provider permission requests.

#[cfg(any(test, feature = "test-observation"))]
use super::ExternalProviderApprovalRefusalWarning;
use super::{ActiveApprovalContext, ExternalProviderApprovalRefusalReason};
use crate::{
    AcpRequestPayloads, AcpRequestPolicy, ApprovalPortOutcome, ProviderPersistenceTarget,
    map_permission_options, reviewed_approval_fields,
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
use tokio_util::sync::CancellationToken;

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
    if let Some(operation_id) = operation_id
        && let Ok(mut reasons) = state.refusal_reasons.lock()
    {
        reasons.insert(operation_id, reason);
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
    let request_cancellation = responder.cancellation();
    let response_guard = context.as_ref().map(|context| {
        context
            .responses
            .track("session/request_permission", responder.id())
    });
    connection.spawn(async move {
        if let (Some(context), Some(refusal)) =
            (context.as_ref(), state.request_policy.managed_refusal())
        {
            let provider_session_id = request.session_id.0.to_string();
            let response: RequestPermissionResponse =
                serde_json::from_value(refusal.acp_permission())
                    .map_err(|_| Error::internal_error())?;
            record_permission_refusal(
                &state,
                &provider_session_id,
                Some(context.operation_id.clone()),
                ExternalProviderApprovalRefusalReason::Managed(refusal),
            )
            .await;
            #[cfg(any(test, feature = "test-observation"))]
            state
                .permission_outcome
                .store(1, std::sync::atomic::Ordering::Relaxed);
            let queued = responder.respond(response);
            drop(response_guard);
            context
                .clone()
                .cancel_after_refusal(Arc::clone(&state.request_policy), refusal)
                .await;
            return queued;
        }
        let outcome = match context {
            Some(context) => {
                handle_contextual_permission_request(
                    context,
                    request,
                    request_id,
                    request_cancellation,
                    &state,
                )
                .await
            }
            None => RequestPermissionOutcome::Cancelled,
        };
        #[cfg(any(test, feature = "test-observation"))]
        state.permission_outcome.store(
            if matches!(outcome, RequestPermissionOutcome::Selected(_)) {
                2
            } else {
                1
            },
            std::sync::atomic::Ordering::Relaxed,
        );
        let response = responder.respond(RequestPermissionResponse::new(outcome));
        drop(response_guard);
        response
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
                        context.approval,
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
    let agent_cancellation = CancellationToken::new();
    let approval = context.decide_approval(
        state.request_policy.as_ref(),
        canonical_request,
        agent_cancellation.clone(),
    );
    tokio::pin!(approval);
    let outcome = tokio::select! {
        biased;
        () = request_cancellation.cancelled() => {
            agent_cancellation.cancel();
            approval.await
        }
        outcome = &mut approval => outcome,
    };
    match outcome {
        ApprovalPortOutcome::Selected { option_id, .. } => {
            RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(option_id))
        }
        ApprovalPortOutcome::Cancelled => RequestPermissionOutcome::Cancelled,
        ApprovalPortOutcome::Unavailable => {
            record_permission_refusal(
                state,
                &provider_session_id,
                Some(context.operation_id),
                ExternalProviderApprovalRefusalReason::ApprovalBrokerUnavailable,
            )
            .await;
            RequestPermissionOutcome::Cancelled
        }
    }
}
