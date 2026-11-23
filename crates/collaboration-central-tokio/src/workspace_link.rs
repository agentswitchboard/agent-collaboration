//! Modern machine links share the native HTTP listener and retain core admission.
use crate::{TokioWorkspaceHost, TokioWorkspaceStores, object_storage::ObjectStorage};
use axum::{
    Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket, close_code},
    },
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::get,
};
use central_link_protocol::{
    DataFrame, FrameDirection, LinkFrame, LinkRequest, MachineLinkCloseCode, ProtocolError,
    ReceiptResult, ReplyPayload, RequestPayload, RequestRefusal, RequestStatusAnswer, TimeLeft,
    TokenRenewalRefusal, TokenRenewalResult, decode_text, encode_text,
};
use collaboration_central_core::{
    ApplyingRequestFailure, MachineLinkAdmission, MachineLinkRequestAuthority, RenewalTermination,
    WorkspaceRuntime, project_renewal_refusal,
};
use collaboration_central_core::{ValidatedMachineConnection, WorkspaceCaller};
use fleet_contracts::EnrollmentInvalidation;
use fleet_contracts::mail_link::{
    MailReceiptSettlement, WELCOME_PAGE_SIZE, WelcomeDeliveryPage, WindowedDeliveryDecision,
};
use fleet_identity_protocol::OrganizationId;
use futures_util::FutureExt;
use router_clock::RuntimeClock;
use std::future::Future;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[path = "workspace_typed_link.rs"]
mod typed_link;
use typed_link::{offer_mac_call, reply_to_typed_call};

type Runtime = WorkspaceRuntime<ObjectStorage, TokioWorkspaceStores>;

struct LinkState {
    host: Arc<TokioWorkspaceHost>,
    stop: CancellationToken,
    tasks: TaskTracker,
}

/// The listener owner cancels links before draining HTTP, then waits before closing stores.
pub struct WorkspaceLink {
    state: Arc<LinkState>,
}

impl WorkspaceLink {
    pub fn new(host: Arc<TokioWorkspaceHost>) -> Self {
        Self {
            state: Arc::new(LinkState {
                host,
                stop: CancellationToken::new(),
                tasks: TaskTracker::new(),
            }),
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/v1/organizations/{organization}/link", get(upgrade_link))
            .with_state(self.state.clone())
    }

    pub fn begin_shutdown(&self) {
        self.state.stop.cancel();
    }

    /// Call after the listener has drained HTTP admission handlers.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.state.tasks.close();
        self.state.tasks.wait().await;
    }
}

impl Drop for WorkspaceLink {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

async fn upgrade_link(
    State(state): State<Arc<LinkState>>,
    Path(organization): Path<String>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    // Track admission as well as the upgraded connection, including failures.
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
    let runtime = match state.host.central(&organization).await {
        Ok(runtime) => runtime,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let connection = match runtime.admit_machine_link(token).await {
        Ok(MachineLinkAdmission::Admitted(connection))
            if connection.organization_id() == &organization =>
        {
            connection
        }
        Ok(MachineLinkAdmission::Ended(ended)) => {
            let failed_state = state.clone();
            let failed_org = organization.clone();
            let failed_ended = ended.clone();
            return upgrade
                .on_failed_upgrade(move |_| {
                    close_ended_enrollment(&failed_state, &failed_org, &failed_ended)
                })
                .on_upgrade(move |mut socket| async move {
                    let _task = task;
                    attempt_close(&mut socket, ended.cause.code(), "");
                    close_ended_enrollment(&state, &organization, &ended);
                })
                .into_response();
        }
        Ok(_) => return StatusCode::UNAUTHORIZED.into_response(),
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };
    let Some(central_time) =
        chrono::DateTime::from_timestamp_millis(state.host.current_time().unix_millis())
    else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let Some(effective_expires_at) =
        chrono::DateTime::from_timestamp(connection.effective_expires_at().0, 0)
    else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let welcome = LinkFrame::Data(DataFrame::Welcome {
        machine_id: connection.machine_id().clone(),
        central_time,
        effective_expires_at,
    });
    if state.stop.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    upgrade
        .on_upgrade(move |socket| async move {
            let _task = task;
            serve_link(socket, state, runtime, connection, welcome).await;
        })
        .into_response()
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return None;
    }
    Some(token)
}

async fn serve_link(
    mut socket: WebSocket,
    state: Arc<LinkState>,
    runtime: Arc<Runtime>,
    mut connection: ValidatedMachineConnection,
    welcome: LinkFrame,
) {
    // Keep Welcome queued locally until its safety attempt finishes, so observing it is a barrier.
    if let Err(failure) = runtime.recompute_alarm().await {
        tracing::warn!(failure = ?failure, "post_welcome_alarm_recompute_failed");
    }
    let registration = runtime
        .register_active_machine(&connection.attachment(), || {
            state.host.push_registry.register(&connection)
        })
        .await;
    let mut registration = match registration {
        Ok(Ok(registration)) => registration,
        Ok(Err(cause)) => {
            close_link(&mut socket, &state.stop, cause.code(), "");
            return;
        }
        Err(_) => {
            close_link(
                &mut socket,
                &state.stop,
                close_code::ERROR,
                "central unavailable",
            );
            return;
        }
    };
    let writes = LinkWrites {
        stop: &state.stop,
        replaced: &registration.replaced,
        authority_close: &registration.authority_close,
        clock: state.host.link_clock(),
        tasks: &state.tasks,
    };
    if !writes.send(&mut socket, &welcome).await {
        return;
    }
    if !send_welcome_backlog(&mut socket, &state, &writes, &runtime, &connection).await {
        return;
    }
    if let Some(completion) = runtime.recover_approval_notices(connection.clone()) {
        writes.schedule_completion(completion);
    }
    loop {
        let message = tokio::select! {
            biased;
            () = state.stop.cancelled() => {
                writes.close_if_authority_cancelled(&mut socket);
                return;
            }
            () = registration.replaced.cancelled() => {
                writes.close(&mut socket, close_code::NORMAL, "machine link ended");
                return;
            }
            message = socket.recv() => message,
            ready = registration.receiver.recv() => {
                let Some(message_id) = ready else { return; };
                if !offer_windowed_delivery(&mut socket, &state, &writes, &runtime, &connection, &message_id).await {
                    return;
                }
                continue;
            }
            queued = registration.outbound_calls.recv() => {
                let Some(queued) = queued else { return; };
                if !offer_mac_call(&mut socket, &writes, &runtime, &connection, &registration.outbound_calls, queued).await {
                    return;
                }
                continue;
            }
        };
        let Some(Ok(message)) = message else {
            return;
        };
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => return,
            // axum handles RFC WebSocket control frames; application heartbeats
            // are the protocol's literal text ping and pong.
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Binary(_) => {
                writes.close(&mut socket, close_code::UNSUPPORTED, "text frames required");
                return;
            }
        };
        let frame = match decode_text(&text, FrameDirection::ToCentral) {
            Ok(frame) => frame,
            Err(ProtocolError::UnknownTypedCall { correlation_id }) => {
                #[cfg(feature = "test-support")]
                if runtime.fixture_link_calls_enabled()
                    && let Ok((correlation, request)) =
                        central_link_protocol::fixture_central_probe::decode_probe_call(&text)
                {
                    if !typed_link::reply_to_fixture_probe(
                        &mut socket,
                        &writes,
                        &runtime,
                        &connection,
                        correlation,
                        request,
                    )
                    .await
                    {
                        return;
                    }
                    continue;
                }
                if !reply_to_typed_call(
                    &mut socket,
                    &writes,
                    &runtime,
                    &connection,
                    &registration.outbound_calls,
                    correlation_id,
                    None,
                )
                .await
                {
                    return;
                }
                continue;
            }
            Err(ProtocolError::WrongDirection) => {
                writes.close(
                    &mut socket,
                    close_code::UNSUPPORTED,
                    "unsupported link direction",
                );
                return;
            }
            Err(_) => {
                writes.close(&mut socket, close_code::PROTOCOL, "invalid link frame");
                return;
            }
        };
        match frame {
            LinkFrame::Data(DataFrame::CallCentral {
                correlation_id,
                request,
            }) => {
                if !reply_to_typed_call(
                    &mut socket,
                    &writes,
                    &runtime,
                    &connection,
                    &registration.outbound_calls,
                    correlation_id,
                    Some(*request),
                )
                .await
                {
                    return;
                }
            }
            LinkFrame::Data(DataFrame::MacCallReply {
                correlation_id,
                result,
            }) => {
                registration
                    .outbound_calls
                    .answer(&correlation_id, *result, writes.clock);
            }
            LinkFrame::Ping => {
                if !writes.send(&mut socket, &LinkFrame::Pong).await {
                    return;
                }
            }
            LinkFrame::Data(DataFrame::Request { request }) => {
                match reply_to_request(&mut socket, &writes, &runtime, &connection, &request, false)
                    .await
                {
                    RequestOutcome::Replied => {}
                    RequestOutcome::AuthorityLapsed(cause) => {
                        writes.close_authority(&mut socket, cause);
                        return;
                    }
                    RequestOutcome::Stopped => return,
                }
            }
            LinkFrame::Data(DataFrame::Batch { requests }) => {
                // Every member is answered, as on the Worker; a lapsed authority
                // refuses each member and closes the link after the batch.
                let mut lapsed = None;
                for request in requests.into_requests() {
                    match reply_to_request(
                        &mut socket,
                        &writes,
                        &runtime,
                        &connection,
                        &request,
                        true,
                    )
                    .await
                    {
                        RequestOutcome::Replied => {}
                        RequestOutcome::AuthorityLapsed(cause) => {
                            lapsed = lapsed.or(Some(cause));
                        }
                        RequestOutcome::Stopped => return,
                    }
                }
                if let Some(cause) = lapsed {
                    writes.close_authority(&mut socket, cause);
                    return;
                }
            }
            LinkFrame::Data(DataFrame::RenewMachineToken {
                correlation_id,
                token,
            }) => {
                let (result, termination) = renew(&runtime, &mut connection, token.expose()).await;
                registration.update_authority(&connection);
                let answer = LinkFrame::Data(DataFrame::MachineTokenRenewal {
                    correlation_id,
                    result,
                });
                if let Some(termination) = termination {
                    #[cfg(feature = "test-support")]
                    let fail_send = matches!(termination, RenewalTermination::EndEnrollment(_))
                        && runtime.take_idle_renewal_send_failure();
                    #[cfg(not(feature = "test-support"))]
                    let fail_send = false;
                    if !fail_send {
                        writes.attempt_terminal_refusal(&mut socket, &answer).await;
                    }
                    writes.close_authority(&mut socket, termination.cause());
                    if let RenewalTermination::EndEnrollment(ended) = termination {
                        #[cfg(feature = "test-support")]
                        runtime
                            .runtime()
                            .pause_points()
                            .reached_central(fleet_runtime::PausePoint::BeforeIdleEnrollmentCleanup)
                            .await;
                        close_ended_enrollment(&state, connection.organization_id(), &ended);
                        #[cfg(feature = "test-support")]
                        runtime
                            .runtime()
                            .pause_points()
                            .reached_central(fleet_runtime::PausePoint::AfterIdleEnrollmentCleanup)
                            .await;
                    }
                    return;
                }
                if !writes.send(&mut socket, &answer).await {
                    return;
                }
            }
            LinkFrame::Data(DataFrame::RequestStatus {
                correlation_id,
                caller,
                query,
            }) => {
                let active = match runtime
                    .restore_machine_link_request(&connection.attachment())
                    .await
                {
                    Ok(MachineLinkRequestAuthority::Admitted(active)) => active,
                    Ok(MachineLinkRequestAuthority::Refused(cause)) => {
                        writes.close_authority(&mut socket, cause);
                        return;
                    }
                    Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) | Err(_) => {
                        writes.close(&mut socket, close_code::ERROR, "central unavailable");
                        return;
                    }
                };
                let result = match WorkspaceCaller::from_machine_link(active, caller) {
                    Ok(caller) => runtime.request_status(&caller, &query).await,
                    Err(reason) => Ok(RequestStatusAnswer::Refused(RequestRefusal::Workspace(
                        reason,
                    ))),
                };
                let Ok(result) = result else {
                    writes.close(&mut socket, close_code::ERROR, "central unavailable");
                    return;
                };
                let reply = LinkFrame::Data(DataFrame::RequestStatusReply {
                    correlation_id,
                    result: Box::new(result),
                });
                if !writes.send(&mut socket, &reply).await {
                    return;
                }
            }
            LinkFrame::Data(DataFrame::Receipts {
                correlation_id,
                receipts,
            }) => {
                let attachment = connection.attachment();
                let mut results = Vec::with_capacity(receipts.receipts().len());
                for receipt in receipts.receipts() {
                    match runtime
                        .apply_delivery_receipt(
                            &attachment,
                            &receipt.message_id,
                            receipt.kind.clone(),
                        )
                        .await
                    {
                        Ok(MailReceiptSettlement::Applied) => results.push(ReceiptResult::Recorded),
                        Ok(MailReceiptSettlement::Stop(result)) => results.push(result),
                        Ok(MailReceiptSettlement::AuthorityLapsed(cause)) => {
                            writes.close_authority(&mut socket, cause);
                            return;
                        }
                        Err(_) => {
                            writes.close(&mut socket, close_code::ERROR, "central unavailable");
                            return;
                        }
                    }
                }
                let reply = LinkFrame::Data(DataFrame::ReceiptResults {
                    correlation_id,
                    results,
                });
                if !writes.send(&mut socket, &reply).await {
                    return;
                }
            }
            // Direction validation also rejects server-only frames here.
            _ => {
                writes.close(
                    &mut socket,
                    close_code::UNSUPPORTED,
                    "unsupported link operation",
                );
                return;
            }
        }
    }
}

async fn send_welcome_backlog(
    socket: &mut WebSocket,
    state: &LinkState,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
) -> bool {
    let attachment = connection.attachment();
    let mut cursor = None;
    loop {
        let page = tokio::select! {
            biased;
            () = state.stop.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
            () = writes.replaced.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
            page = runtime.windowed_welcome_page(&attachment, cursor.as_ref()) => page,
        };
        let rows = match page {
            Ok(WelcomeDeliveryPage::Ready(rows)) => rows,
            Ok(WelcomeDeliveryPage::AuthorityLapsed(cause)) => {
                writes.close_authority(socket, cause);
                return false;
            }
            Err(_) => return false,
        };
        cursor = rows
            .last()
            .map(fleet_contracts::mail_link::WindowedDelivery::cursor);
        let count = rows.len();
        for row in rows {
            if !offer_windowed_delivery(
                socket,
                state,
                writes,
                runtime,
                connection,
                &row.message.message_id,
            )
            .await
            {
                return false;
            }
        }
        if count < WELCOME_PAGE_SIZE {
            return true;
        }
    }
}

async fn offer_windowed_delivery(
    socket: &mut WebSocket,
    state: &LinkState,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    message_id: &workspace_ids::MessageId,
) -> bool {
    let attachment = connection.attachment();
    let decision = tokio::select! {
        biased;
        () = state.stop.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
        () = writes.replaced.cancelled() => { writes.close_if_authority_cancelled(socket); return false; }
        decision = runtime.prepare_windowed_delivery(&attachment, message_id) => decision,
    };
    let delivery = match decision {
        Ok(WindowedDeliveryDecision::Ready(delivery)) => delivery,
        Ok(WindowedDeliveryDecision::Ineligible) => {
            return true;
        }
        Ok(WindowedDeliveryDecision::AuthorityLapsed(cause)) => {
            writes.close_authority(socket, cause);
            return false;
        }
        Err(_) => return false,
    };
    let Some(now) =
        chrono::DateTime::from_timestamp_millis(state.host.current_time().unix_millis())
    else {
        return false;
    };
    if now >= delivery.expires_at {
        return true;
    }
    let time_left = if matches!(
        delivery.message.body,
        workspace_model::central_mail::MessageBody::Direct { .. }
    ) {
        let Some(time_left) =
            TimeLeft::at_handoff(delivery.message.created_at, delivery.expires_at, now)
        else {
            return true;
        };
        Some(time_left)
    } else {
        None
    };
    let frame = LinkFrame::Data(DataFrame::Deliver {
        message: Box::new(central_link_protocol::DeliveryMessage::from(
            *delivery.message,
        )),
        sender: delivery.sender,
        time_left,
    });
    writes.send(socket, &frame).await
}

/// R40/V32 renewal on the open connection: a renewed connection replaces the
/// current one; on refusal the current connection and its expiry stand.
async fn renew(
    runtime: &Runtime,
    connection: &mut ValidatedMachineConnection,
    token: &str,
) -> (TokenRenewalResult, Option<RenewalTermination>) {
    let refused = |reason| (TokenRenewalResult::Refused { reason }, None);
    match runtime
        .renew_machine_connection(connection.attachment_ref(), token)
        .await
    {
        Ok(Ok(renewed)) => {
            let Some(effective_expires_at) =
                chrono::DateTime::from_timestamp(renewed.connection.effective_expires_at().0, 0)
            else {
                return refused(TokenRenewalRefusal::CentralUnavailable);
            };
            *connection = renewed.connection;
            (
                TokenRenewalResult::Renewed {
                    effective_expires_at,
                    central_time: renewed.central_time,
                },
                None,
            )
        }
        Ok(Err(refusal)) => {
            let (reason, termination) = project_renewal_refusal(refusal);
            (TokenRenewalResult::Refused { reason }, termination)
        }
        Err(_) => refused(TokenRenewalRefusal::CentralUnavailable),
    }
}

fn close_ended_enrollment(
    state: &LinkState,
    organization: &OrganizationId,
    ended: &EnrollmentInvalidation,
) {
    state
        .host
        .push_registry
        .invalidate_enrollment(organization, ended);
}

/// What became of one request frame on the link.
enum RequestOutcome {
    Replied,
    /// The connection's authority expired or was revoked: the request was
    /// answered Refused(MachineRevoked) and the link must close.
    AuthorityLapsed(MachineLinkCloseCode),
    /// The link stopped or failed; it is already closed or closing.
    Stopped,
}

async fn reply_to_request(
    socket: &mut WebSocket,
    writes: &LinkWrites<'_>,
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    request: &LinkRequest,
    in_batch: bool,
) -> RequestOutcome {
    if writes.stop.is_cancelled() {
        return RequestOutcome::Stopped;
    }
    // Each request restores the connection against live rows first, as the
    // Worker does: after the deadline or a revocation it is refused.
    let dispatched = match runtime
        .restore_machine_link_request(&connection.attachment())
        .await
    {
        Ok(MachineLinkRequestAuthority::Admitted(active)) => {
            dispatch_request(runtime, &active, request).await
        }
        Ok(MachineLinkRequestAuthority::Refused(cause)) => {
            Err(ApplyingRequestFailure::Authority(cause))
        }
        Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) if in_batch => {
            Ok(ReplyPayload::central_unavailable(request.payload()))
        }
        Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) | Err(_) => {
            writes.close(socket, close_code::ERROR, "central unavailable");
            return RequestOutcome::Stopped;
        }
    };
    let (result, outcome) = match dispatched {
        Ok(result) => (result, RequestOutcome::Replied),
        Err(ApplyingRequestFailure::Authority(cause)) => (
            ReplyPayload::authority_refused(request.payload(), cause),
            RequestOutcome::AuthorityLapsed(cause),
        ),
        Err(ApplyingRequestFailure::Store(_)) => {
            writes.close(socket, close_code::ERROR, "central unavailable");
            return RequestOutcome::Stopped;
        }
    };
    let sent = writes
        .send(
            socket,
            &LinkFrame::Data(DataFrame::Reply {
                correlation_id: request.correlation_id().clone(),
                result: Box::new(result),
            }),
        )
        .await;
    if sent {
        outcome
    } else {
        RequestOutcome::Stopped
    }
}

async fn dispatch_request(
    runtime: &Runtime,
    connection: &ValidatedMachineConnection,
    request: &LinkRequest,
) -> Result<ReplyPayload, ApplyingRequestFailure> {
    let caller =
        match WorkspaceCaller::from_machine_link(connection.clone(), request.caller().clone()) {
            Ok(caller) => caller,
            Err(reason) => return Ok(ReplyPayload::workspace_refused(request.payload(), reason)),
        };
    match request.payload() {
        RequestPayload::Workspace(request) => runtime
            .request_with_authority(&caller, request.clone())
            .await
            .map(ReplyPayload::Workspace),
        RequestPayload::Mail(request) => runtime
            .request_central_with_authority(&caller, request.clone())
            .await
            .map(ReplyPayload::Mail),
    }
}

struct LinkWrites<'a> {
    stop: &'a CancellationToken,
    replaced: &'a CancellationToken,
    authority_close: &'a OnceLock<MachineLinkCloseCode>,
    clock: &'a dyn RuntimeClock,
    tasks: &'a TaskTracker,
}

impl LinkWrites<'_> {
    fn schedule_completion(&self, completion: collaboration_central_core::LinkCallCompletion) {
        let stop = self.stop.clone();
        let replaced = self.replaced.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                biased;
                () = stop.cancelled() => {}
                () = replaced.cancelled() => {}
                () = completion => {}
            }
        });
    }

    async fn send(&self, socket: &mut WebSocket, frame: &LinkFrame) -> bool {
        let Ok(text) = encode_text(frame, FrameDirection::ToRouter) else {
            return false;
        };
        self.send_text(socket, text).await
    }

    async fn send_text(&self, socket: &mut WebSocket, text: String) -> bool {
        let sent = self
            .send_until_stopped(async { socket.send(Message::Text(text.into())).await.is_ok() })
            .await;
        if !sent {
            self.close_if_authority_cancelled(socket);
        }
        sent
    }

    async fn send_until_stopped(&self, send: impl Future<Output = bool>) -> bool {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => false,
            () = self.replaced.cancelled() => false,
            sent = send => sent,
        }
    }

    fn close_if_authority_cancelled(&self, socket: &mut WebSocket) {
        if let Some(cause) = self.authority_close.get() {
            self.close_authority(socket, *cause);
        }
    }

    async fn attempt_terminal_refusal(&self, socket: &mut WebSocket, frame: &LinkFrame) {
        const REFUSAL_SEND_BUDGET: Duration = Duration::from_millis(250);
        let Ok(text) = encode_text(frame, FrameDirection::ToRouter) else {
            return;
        };
        tokio::select! {
            biased;
            () = self.stop.cancelled() => {}
            _ = router_clock::timeout(self.clock, REFUSAL_SEND_BUDGET, socket.send(Message::Text(text.into()))) => {}
        }
    }

    fn close_authority(&self, socket: &mut WebSocket, cause: MachineLinkCloseCode) {
        let cause = self.authority_close.get().copied().unwrap_or(cause);
        attempt_close(socket, cause.code(), "");
    }

    fn close(&self, socket: &mut WebSocket, code: u16, reason: &'static str) {
        if let Some(cause) = self.authority_close.get() {
            self.close_authority(socket, *cause);
        } else if self.replaced.is_cancelled() {
            close_link(socket, self.stop, close_code::NORMAL, "machine link ended");
        } else {
            close_link(socket, self.stop, code, reason);
        }
    }
}

fn close_link(socket: &mut WebSocket, stop: &CancellationToken, code: u16, reason: &'static str) {
    if !stop.is_cancelled() {
        attempt_close(socket, code, reason);
    }
}

fn attempt_close(socket: &mut WebSocket, code: u16, reason: &'static str) {
    // Once polled on a writable socket the transport can deliver the typed reason.
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .now_or_never();
}
