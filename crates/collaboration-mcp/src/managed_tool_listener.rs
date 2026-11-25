//! HTTP authentication for the loopback-only, stateless managed MCP route.

use std::sync::Arc;

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Path, Request, State},
    http::{Method, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse as _, Response},
};
use collaboration_protocol::ManagedToolResolution;
use collaboration_service::CollaborationApplication;
use rmcp::{
    model::{ClientJsonRpcMessage, ClientNotification},
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
    },
};

use crate::{
    managed_message_tools::ManagedMessageServer,
    managed_tool_bindings::{MANAGED_TOOL_PATH, ManagedToolBindings, ManagedToolRequest},
    tool_call_registry::ToolCallRegistry,
};

#[derive(Clone)]
struct ManagedRouteAuth {
    bindings: Arc<ManagedToolBindings>,
    max_request_body_bytes: usize,
}

pub(crate) fn managed_tool_routes(
    bindings: Arc<ManagedToolBindings>,
    application: CollaborationApplication,
    calls: ToolCallRegistry,
    http_config: StreamableHttpServerConfig,
    max_request_body_bytes: usize,
) -> Router {
    let server = ManagedMessageServer::new(application, calls);
    let service = StreamableHttpService::new(
        move || Ok(server.clone()),
        Arc::new(NeverSessionManager::default()),
        http_config,
    );
    Router::new()
        .route_service(MANAGED_TOOL_PATH, service)
        .route_layer(middleware::from_fn_with_state(
            ManagedRouteAuth {
                bindings,
                max_request_body_bytes,
            },
            authenticate_descriptor,
        ))
}

async fn authenticate_descriptor(
    State(auth): State<ManagedRouteAuth>,
    Path(start): Path<String>,
    mut request: Request,
    next: Next,
) -> Response {
    if request.headers().get_all(AUTHORIZATION).iter().count() != 1 {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Some(credential) = request.headers_mut().remove(AUTHORIZATION) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(binding) = credential
        .as_bytes()
        .strip_prefix(b"Bearer ")
        .and_then(|bearer| auth.bindings.authenticate(&start, bearer))
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    // rmcp's stateless transport acknowledges notifications without a handler.
    // Observe the client's initialized acknowledgment only if rmcp accepts it.
    let initialized = if request.method() == Method::POST {
        let (parts, body) = request.into_parts();
        let Ok(body) = to_bytes(body, auth.max_request_body_bytes).await else {
            return StatusCode::PAYLOAD_TOO_LARGE.into_response();
        };
        let initialized = matches!(serde_json::from_slice::<ClientJsonRpcMessage>(&body),
            Ok(ClientJsonRpcMessage::Notification(notification))
            if matches!(notification.notification, ClientNotification::InitializedNotification(_)));
        request = Request::from_parts(parts, Body::from(body));
        initialized
    } else {
        false
    };
    let resolution = match auth
        .bindings
        .resolver
        .resolve_managed_start(&binding.binding)
        .await
    {
        Ok(ManagedToolResolution::Retired) => return StatusCode::NOT_FOUND.into_response(),
        Ok(resolution) => resolution,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    request
        .extensions_mut()
        .insert(Arc::new(ManagedToolRequest::new(resolution)));

    let response = next.run(request).await;
    if initialized && response.status() == StatusCode::ACCEPTED {
        binding.record_initialized();
    }
    response
}
