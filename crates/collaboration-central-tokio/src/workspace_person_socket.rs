//! Native socket wiring for the shared person-link service.

use super::{
    LinkState, Runtime, bearer_token,
    link_io::{IncomingText, LinkWrites, configure_upgrade, receive_text},
};
use axum::{
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket, close_code},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use central_link_protocol::PersonLinkCloseCode;
use collaboration_central_core::{
    PersonLinkAdmission, PersonLinkReply, PersonSocketAttachment, PersonSocketRegistration,
};
use fleet_identity_protocol::OrganizationId;
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub(super) async fn upgrade(
    State(state): State<Arc<LinkState>>,
    Path(organization): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let task = state.tasks.token();
    if state.stop.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Some(token) = bearer_token(&headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(organization) = OrganizationId::try_from(organization) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let Ok(runtime) = state.host.central(&organization).await else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let attachment = match runtime.admit_person_link(token).await {
        Ok(PersonLinkAdmission::Admitted(attachment)) => attachment,
        Ok(PersonLinkAdmission::Refused) => return StatusCode::UNAUTHORIZED.into_response(),
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    configure_upgrade(upgrade)
        .on_upgrade(move |socket| async move {
            let _task = task;
            serve(socket, state, runtime, attachment).await;
        })
        .into_response()
}

async fn serve(
    mut socket: WebSocket,
    state: Arc<LinkState>,
    runtime: Arc<Runtime>,
    attachment: PersonSocketAttachment,
) {
    let reconnect_requested = CancellationToken::new();
    let request_reconnect = reconnect_requested.clone();
    let registered = runtime
        .register_person_socket(&attachment, || {
            state
                .host
                .person_sockets
                .register(attachment.clone(), move || request_reconnect.cancel())
        })
        .await;
    let closed_machine = OnceLock::new();
    let writes = LinkWrites {
        stop: &state.stop,
        replaced: &reconnect_requested,
        authority_close: &closed_machine,
        clock: state.host.link_clock(),
        tasks: &state.tasks,
        #[cfg(feature = "test-support")]
        pauses: runtime.runtime().pause_points(),
    };
    let registered = match registered {
        Ok(PersonSocketRegistration::Registered(registered)) => registered,
        Ok(PersonSocketRegistration::Stale) => return reconnect(&mut socket, &writes).await,
        Err(_) => {
            return writes
                .close(&mut socket, close_code::ERROR, "central unavailable")
                .await;
        }
    };
    loop {
        let incoming = tokio::select! {
            biased;
            () = state.stop.cancelled() => return,
            () = reconnect_requested.cancelled() => return reconnect(&mut socket, &writes).await,
            incoming = socket.recv() => incoming,
        };
        let text = match receive_text(&mut socket, &writes, incoming).await {
            IncomingText::Text(text) => text,
            IncomingText::Control => continue,
            IncomingText::Ended => return,
        };
        let current = registered.current(state.host.current_time().unix_millis());
        let Some(attachment) = current else {
            return reconnect(&mut socket, &writes).await;
        };
        match runtime.handle_person_link_text(&attachment, &text).await {
            Ok(PersonLinkReply::Frames(frames)) => {
                for frame in frames {
                    if !writes.send(&mut socket, &frame).await {
                        if reconnect_requested.is_cancelled() && !state.stop.is_cancelled() {
                            reconnect(&mut socket, &writes).await;
                        }
                        return;
                    }
                }
            }
            Ok(PersonLinkReply::Reconnect) => return reconnect(&mut socket, &writes).await,
            Ok(PersonLinkReply::ProtocolViolation) => {
                return writes
                    .close(&mut socket, close_code::POLICY, "unsupported person frame")
                    .await;
            }
            Err(_) => {
                return writes
                    .close(&mut socket, close_code::ERROR, "central unavailable")
                    .await;
            }
        }
    }
}

async fn reconnect(socket: &mut WebSocket, writes: &LinkWrites<'_>) {
    const CLOSE_BUDGET: Duration = Duration::from_millis(250);
    let frame = Message::Close(Some(CloseFrame {
        code: PersonLinkCloseCode::Reconnect.code(),
        reason: "reconnect".into(),
    }));
    let _ = router_clock::timeout(writes.clock, CLOSE_BUDGET, socket.send(frame)).await;
}
