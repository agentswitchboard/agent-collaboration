//! Hibernating machine connections dispatch independent requests through the owning core.
use super::workspace_machine_link_io::{renew, send};
use super::workspace_pending_mac_calls::PendingMacCalls;
use super::workspace_socket_attachment::{SOCKET_TAG_PREFIX, SocketAttachment};
use super::{CentralObject, OrgRuntime, OrganizationId};
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, LinkFrame, LinkRequest,
    LinkRequestBatch, ProtocolError, ReceiptBatch, ReceiptResult, ReplyPayload, RequestPayload,
    RequestRefusal, RequestStatusAnswer, RequestStatusQuery,
};
use collaboration_central_core::WorkspaceCaller;
use collaboration_central_core::{
    ApplyingRequestFailure, MachineLinkAdmission, MachineLinkRequestAuthority, RenewalTermination,
};
use fleet_contracts::mail_link::MailReceiptSettlement;
use fleet_contracts::{EnrollmentInvalidation, MachineConnectionAttachment};
use fleet_runtime_ports::{Clock, IdSource};
use worker::{
    Request, Response, Result, WebSocket, WebSocketIncomingMessage, WebSocketPair,
    WebSocketRequestResponsePair,
};

enum IncomingRequests {
    #[cfg(feature = "test-support")]
    FixtureProbe {
        correlation_id: CorrelationId,
        request: central_link_protocol::fixture_central_probe::ProbeRequestEnvelope,
    },
    Capabilities(central_link_protocol::MacCapabilities),
    MacReply {
        correlation_id: CorrelationId,
        result: Box<central_link_protocol::TypedCallResult<central_link_protocol::MacCallReply>>,
    },
    Single(Box<LinkRequest>),
    Batch(LinkRequestBatch),
    Typed {
        correlation_id: CorrelationId,
        request: Option<CentralCall>,
    },
    Renewal {
        correlation_id: central_link_protocol::CorrelationId,
        token: central_link_protocol::MachineTokenText,
    },
    Status {
        correlation_id: central_link_protocol::CorrelationId,
        caller: workspace_model::Caller,
        query: RequestStatusQuery,
    },
    Receipts {
        correlation_id: central_link_protocol::CorrelationId,
        receipts: ReceiptBatch,
    },
}
enum MachineRequestResult {
    Reply(ReplyPayload),
    AuthorityRefused(central_link_protocol::MachineLinkCloseCode),
    Unavailable,
}

impl CentralObject {
    pub(super) async fn upgrade_machine_link(
        &self,
        organization: OrganizationId,
        request: &Request,
    ) -> Result<Response> {
        if !crate::workspace_link_entry::is_upgrade(request)? {
            return Response::error("websocket upgrade required", 426);
        }
        let Some(token) = crate::workspace_link_entry::bearer(request)? else {
            return Response::error("machine token required", 401);
        };
        let runtime = self.runtime(organization.clone()).await?;
        let connection = match runtime.admit_machine_link(&token).await {
            Ok(MachineLinkAdmission::Admitted(connection))
                if connection.organization_id() == &organization =>
            {
                connection
            }
            Ok(MachineLinkAdmission::Ended(ended)) => {
                let upgrade = close_ended_connect(ended.cause);
                close_enrollment_sockets(&self.state, &self.pending_mac_calls, &ended, None);
                return upgrade;
            }
            Ok(_) => return Response::error("machine refused", 401),
            Err(_) => return Response::error("machine admission unavailable", 503),
        };
        let Some(central_time) =
            chrono::DateTime::from_timestamp_millis(self.clock.now().unix_millis())
        else {
            return Response::error("clock unavailable", 503);
        };
        let Some(effective_expires_at) =
            chrono::DateTime::from_timestamp(connection.effective_expires_at().0, 0)
        else {
            return Response::error("machine admission unavailable", 503);
        };
        let pair = WebSocketPair::new()?;
        SocketAttachment::new(connection.attachment()).save(&pair.server)?;
        if self.state.get_websocket_auto_response().is_none() {
            let heartbeat =
                WebSocketRequestResponsePair::new("ping", "pong").map_err(worker::Error::from)?;
            self.state.set_websocket_auto_response(&heartbeat);
        }
        // Queue before the safety call; only send after its attempt has completed.
        let welcome = DataFrame::Welcome {
            machine_id: connection.machine_id().clone(),
            central_time,
            effective_expires_at,
        };
        if let Err(failure) = runtime.recompute_alarm().await {
            worker::console_warn!("post_welcome_alarm_recompute_failed failure={failure:?}");
        }
        let tag = connection.machine_id().as_str();
        let physical_tag = format!("{SOCKET_TAG_PREFIX}{}", self.link_ids.new_id());
        let older = match runtime
            .register_active_machine(&connection.attachment(), || {
                let older = self.state.get_websockets_with_tag(tag);
                self.mail_offers.begin_welcome(physical_tag.clone());
                self.state
                    .accept_websocket_with_tags(&pair.server, &[tag, &physical_tag]);
                older
            })
            .await
        {
            Ok(Ok(older)) => older,
            Ok(Err(_)) => return Response::error("machine refused", 401),
            Err(_) => return Response::error("machine admission unavailable", 503),
        };
        #[cfg(feature = "test-support")]
        super::workspace_machine_link_io::hold_welcome(
            &runtime,
            fleet_runtime::PausePoint::BeforeMachineLinkWelcome,
        )
        .await;
        if let Err(error) = send(&pair.server, welcome) {
            self.mail_offers.forget(&physical_tag);
            return Err(error);
        }
        self.mail_offers.forget(&physical_tag);
        for socket in older {
            let _ = self.close_machine_link(&socket, 1000, Some("machine link replaced"));
        }
        #[cfg(feature = "test-support")]
        super::workspace_machine_link_io::hold_welcome(
            &runtime,
            fleet_runtime::PausePoint::AfterMachineLinkWelcome,
        )
        .await;
        self.send_welcome_mail(&runtime, &pair.server, false)
            .await?;
        Response::from_websocket(pair.client)
    }

    pub(super) async fn handle_machine_message(
        &self,
        socket: WebSocket,
        incoming: WebSocketIncomingMessage,
    ) -> Result<()> {
        let Some(text) = self.machine_text(&socket, incoming)? else {
            return Ok(());
        };
        let frame = match central_link_protocol::decode_text(&text, FrameDirection::ToCentral) {
            Ok(frame) => frame,
            Err(ProtocolError::UnknownTypedCall { correlation_id }) => {
                #[cfg(feature = "test-support")]
                if let Ok((correlation_id, request)) =
                    central_link_protocol::fixture_central_probe::decode_probe_call(&text)
                {
                    return self
                        .handle_machine_request(
                            &socket,
                            IncomingRequests::FixtureProbe {
                                correlation_id,
                                request,
                            },
                        )
                        .await;
                }
                return self
                    .handle_machine_request(
                        &socket,
                        IncomingRequests::Typed {
                            correlation_id,
                            request: None,
                        },
                    )
                    .await;
            }
            Err(_) => return self.close_machine_link(&socket, 1008, Some("invalid link frame")),
        };
        let requests = match frame {
            LinkFrame::Data(DataFrame::MacCapabilities(announcement)) => {
                IncomingRequests::Capabilities(announcement)
            }
            LinkFrame::Data(DataFrame::CallCentral {
                correlation_id,
                request,
            }) => IncomingRequests::Typed {
                correlation_id,
                request: Some(*request),
            },
            LinkFrame::Data(DataFrame::MacCallReply {
                correlation_id,
                result,
            }) => IncomingRequests::MacReply {
                correlation_id,
                result,
            },
            LinkFrame::Data(DataFrame::Request { request }) => IncomingRequests::Single(request),
            LinkFrame::Data(DataFrame::Batch { requests }) => IncomingRequests::Batch(requests),
            LinkFrame::Data(DataFrame::RenewMachineToken {
                correlation_id,
                token,
            }) => IncomingRequests::Renewal {
                correlation_id,
                token,
            },
            LinkFrame::Data(DataFrame::RequestStatus {
                correlation_id,
                caller,
                query,
            }) => IncomingRequests::Status {
                correlation_id,
                caller,
                query,
            },
            LinkFrame::Data(DataFrame::Receipts {
                correlation_id,
                receipts,
            }) => IncomingRequests::Receipts {
                correlation_id,
                receipts,
            },
            LinkFrame::Ping
            | LinkFrame::Pong
            | LinkFrame::Data(
                DataFrame::Welcome { .. }
                | DataFrame::Reply { .. }
                | DataFrame::RequestStatusReply { .. }
                | DataFrame::Deliver { .. }
                | DataFrame::ReceiptResults { .. }
                | DataFrame::MachineTokenRenewal { .. }
                | DataFrame::CentralCallReply { .. }
                | DataFrame::CallMac { .. },
            ) => return self.close_machine_link(&socket, 1008, Some("unsupported link event")),
        };
        self.handle_machine_request(&socket, requests).await
    }

    async fn handle_machine_request(
        &self,
        socket: &WebSocket,
        requests: IncomingRequests,
    ) -> Result<()> {
        let attachment = match SocketAttachment::read(socket) {
            Ok(Some(attachment)) => attachment.authority,
            _ => return self.close_machine_link(socket, 1008, Some("connection identity missing")),
        };
        let organization = attachment.organization_id().clone();
        if self.check_physical_organization(&organization).is_err() {
            return self.close_machine_link(socket, 1008, Some("connection organization mismatch"));
        }
        #[cfg(feature = "test-support")]
        super::signin_launcher_controls::sample_clock(self, &organization).await?;
        let runtime = self.runtime(organization).await?;
        #[cfg(feature = "test-support")]
        let checkpoint = match &requests {
            IncomingRequests::Capabilities(_) => Some("capabilities".to_owned()),
            IncomingRequests::Single(request) => {
                Some(String::from(request.correlation_id().clone()))
            }
            IncomingRequests::Receipts { correlation_id, .. } => {
                Some(String::from(correlation_id.clone()))
            }
            IncomingRequests::Batch(_)
            | IncomingRequests::Typed { .. }
            | IncomingRequests::MacReply { .. }
            | IncomingRequests::Renewal { .. }
            | IncomingRequests::Status { .. }
            | IncomingRequests::FixtureProbe { .. } => None,
        };
        let result = self
            .dispatch_machine_event(&runtime, socket.clone(), attachment, requests)
            .await;
        self.flush_ready_mail(&runtime).await;
        let result = match result {
            Ok(()) => self.resume_deferred_mail(&runtime, socket).await,
            Err(error) => Err(error),
        };
        #[cfg(feature = "test-support")]
        if let Some(key) = checkpoint {
            runtime
                .runtime()
                .pause_points()
                .reached_engine(fleet_runtime::PausePoint::AfterMachineFrameHandled, &key)
                .await;
        }
        result
    }

    async fn dispatch_machine_event(
        &self,
        runtime: &std::sync::Arc<OrgRuntime>,
        socket: WebSocket,
        attachment: MachineConnectionAttachment,
        requests: IncomingRequests,
    ) -> Result<()> {
        match requests {
            IncomingRequests::MacReply {
                correlation_id,
                result,
            } => {
                self.admit_mac_reply(runtime, &socket, correlation_id, *result)
                    .await
            }
            IncomingRequests::Capabilities(announcement) => {
                self.announce_mac_capabilities(runtime, &socket, &attachment, announcement)
                    .await
            }
            #[cfg(feature = "test-support")]
            IncomingRequests::FixtureProbe {
                correlation_id,
                request,
            } => {
                if runtime.fixture_link_calls_enabled() {
                    self.dispatch_fixture_probe(
                        runtime,
                        &socket,
                        &attachment,
                        correlation_id,
                        request,
                    )
                    .await
                } else {
                    self.dispatch_typed_machine_call(
                        runtime,
                        &socket,
                        &attachment,
                        correlation_id,
                        None,
                    )
                    .await
                }
            }
            IncomingRequests::Typed {
                correlation_id,
                request,
            } => {
                self.dispatch_typed_machine_call(
                    runtime,
                    &socket,
                    &attachment,
                    correlation_id,
                    request,
                )
                .await
            }
            IncomingRequests::Receipts {
                correlation_id,
                receipts,
            } => {
                let mut results = Vec::with_capacity(receipts.receipts().len());
                for receipt in receipts.receipts() {
                    let result = match runtime
                        .apply_delivery_receipt(
                            &attachment,
                            &receipt.message_id,
                            receipt.kind.clone(),
                        )
                        .await
                    {
                        Ok(MailReceiptSettlement::Applied) => ReceiptResult::Recorded,
                        Ok(MailReceiptSettlement::Stop(result)) => result,
                        Ok(MailReceiptSettlement::AuthorityLapsed(cause)) => {
                            return self.close_machine_authority(&socket, cause);
                        }
                        Err(_) => {
                            return self.close_machine_link(
                                &socket,
                                1011,
                                Some("central unavailable"),
                            );
                        }
                    };
                    self.release_mail_receipt(runtime, &socket, &receipt.message_id, result)
                        .await?;
                    results.push(result);
                }
                send(
                    &socket,
                    DataFrame::ReceiptResults {
                        correlation_id,
                        results,
                    },
                )
            }
            IncomingRequests::Status {
                correlation_id,
                caller,
                query,
            } => {
                let active = match runtime.restore_machine_link_request(&attachment).await {
                    Ok(MachineLinkRequestAuthority::Admitted(active)) => active,
                    Ok(MachineLinkRequestAuthority::Refused(cause)) => {
                        return self.close_machine_authority(&socket, cause);
                    }
                    Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) | Err(_) => {
                        return self.close_machine_link(&socket, 1011, Some("central unavailable"));
                    }
                };
                let result = match WorkspaceCaller::from_machine_link(active, caller) {
                    Ok(caller) => runtime.request_status(&caller, &query).await,
                    Err(reason) => Ok(RequestStatusAnswer::Refused(RequestRefusal::Workspace(
                        reason,
                    ))),
                };
                let Ok(result) = result else {
                    return self.close_machine_link(&socket, 1011, Some("central unavailable"));
                };
                send(
                    &socket,
                    DataFrame::RequestStatusReply {
                        correlation_id,
                        result: Box::new(result),
                    },
                )
            }
            IncomingRequests::Renewal {
                correlation_id,
                token,
            } => {
                let (result, termination) =
                    renew(&socket, runtime.as_ref(), &attachment, token.expose()).await?;
                #[cfg(feature = "test-support")]
                let fail_send = matches!(termination, Some(RenewalTermination::EndEnrollment(_)))
                    && runtime.take_idle_renewal_send_failure();
                #[cfg(not(feature = "test-support"))]
                let fail_send = false;
                let reply = if fail_send {
                    Err(worker::Error::RustError(
                        "fixture idle renewal send failed".into(),
                    ))
                } else {
                    send(
                        &socket,
                        DataFrame::MachineTokenRenewal {
                            correlation_id,
                            result,
                        },
                    )
                };
                #[cfg(feature = "test-support")]
                if !fail_send
                    && reply.is_err()
                    && matches!(termination, Some(RenewalTermination::EndEnrollment(_)))
                {
                    runtime
                        .runtime()
                        .pause_points()
                        .reached_central(
                            fleet_runtime::PausePoint::AfterIdleRenewalPlatformSendError,
                        )
                        .await;
                }
                if let Some(termination) = termination {
                    let closed = self.close_machine_authority(&socket, termination.cause());
                    if let RenewalTermination::EndEnrollment(ended) = termination {
                        #[cfg(feature = "test-support")]
                        runtime
                            .runtime()
                            .pause_points()
                            .reached_central(fleet_runtime::PausePoint::BeforeIdleEnrollmentCleanup)
                            .await;
                        close_enrollment_sockets(
                            &self.state,
                            &self.pending_mac_calls,
                            &ended,
                            Some(&socket),
                        );
                        #[cfg(feature = "test-support")]
                        runtime
                            .runtime()
                            .pause_points()
                            .reached_central(fleet_runtime::PausePoint::AfterIdleEnrollmentCleanup)
                            .await;
                    }
                    return reply.and(closed);
                }
                reply
            }
            IncomingRequests::Single(request) => {
                match self
                    .dispatch_machine_request(runtime.as_ref(), &attachment, &request)
                    .await?
                {
                    MachineRequestResult::Reply(reply) => {
                        self.send_reply_and_close_if_lapsed(&socket, &request, reply, None)
                    }
                    // The request is answered before the close, as on the native host.
                    MachineRequestResult::AuthorityRefused(cause) => {
                        let reply = ReplyPayload::authority_refused(request.payload(), cause);
                        self.send_reply_and_close_if_lapsed(&socket, &request, reply, Some(cause))
                    }
                    MachineRequestResult::Unavailable => self.close_machine_link(
                        &socket,
                        1008,
                        Some("machine admission unavailable"),
                    ),
                }
            }
            IncomingRequests::Batch(requests) => {
                let mut lapsed = None;
                let mut members = requests.requests().iter().peekable();
                while let Some(request) = members.next() {
                    // Every member restores live provenance and enters its own applying turn.
                    // Semantic refusals are replies, so they cannot skip later members.
                    let reply = match self
                        .dispatch_machine_request(runtime.as_ref(), &attachment, request)
                        .await?
                    {
                        MachineRequestResult::Reply(reply) => reply,
                        MachineRequestResult::AuthorityRefused(cause) => {
                            lapsed = lapsed.or(Some(cause));
                            ReplyPayload::authority_refused(request.payload(), cause)
                        }
                        MachineRequestResult::Unavailable => {
                            ReplyPayload::central_unavailable(request.payload())
                        }
                    };
                    // A lapsed authority closes the link with the last member's reply.
                    let close_after_reply = if members.peek().is_none() {
                        lapsed
                    } else {
                        None
                    };
                    self.send_reply_and_close_if_lapsed(
                        &socket,
                        request,
                        reply,
                        close_after_reply,
                    )?;
                }
                Ok(())
            }
        }
    }

    async fn dispatch_machine_request(
        &self,
        runtime: &OrgRuntime,
        attachment: &MachineConnectionAttachment,
        request: &LinkRequest,
    ) -> Result<MachineRequestResult> {
        let restored = runtime.restore_machine_link_request(attachment).await;
        #[cfg(feature = "test-support")]
        self.witness_restored(
            request.correlation_id(),
            match &restored {
                Ok(MachineLinkRequestAuthority::Admitted(_)) => {
                    super::workspace_link_witness::RestoreOutcome::Admitted
                }
                Ok(MachineLinkRequestAuthority::Refused(_)) => {
                    super::workspace_link_witness::RestoreOutcome::Refused
                }
                Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) | Err(_) => {
                    super::workspace_link_witness::RestoreOutcome::Unavailable
                }
            },
        );
        let connection = match restored {
            Ok(MachineLinkRequestAuthority::Admitted(connection)) => connection,
            Ok(MachineLinkRequestAuthority::Refused(cause)) => {
                return Ok(MachineRequestResult::AuthorityRefused(cause));
            }
            Ok(MachineLinkRequestAuthority::TemporarilyUnavailable) => {
                return Ok(MachineRequestResult::Unavailable);
            }
            Err(error) => return Err(worker::Error::RustError(error.to_string())),
        };
        let caller = match WorkspaceCaller::from_machine_link(connection, request.caller().clone())
        {
            Ok(caller) => caller,
            Err(reason) => {
                return Ok(MachineRequestResult::Reply(
                    ReplyPayload::workspace_refused(request.payload(), reason),
                ));
            }
        };
        let result = match request.payload() {
            RequestPayload::Workspace(operation) => runtime
                .request_with_authority(&caller, operation.clone())
                .await
                .map(ReplyPayload::Workspace),
            RequestPayload::Mail(operation) => runtime
                .request_central_with_authority(&caller, operation.clone())
                .await
                .map(ReplyPayload::Mail),
        };
        match result {
            Ok(reply) => Ok(MachineRequestResult::Reply(reply)),
            Err(ApplyingRequestFailure::Authority(cause)) => {
                Ok(MachineRequestResult::AuthorityRefused(cause))
            }
            Err(ApplyingRequestFailure::Store(error)) => {
                Err(worker::Error::RustError(error.to_string()))
            }
        }
    }

    fn send_reply_and_close_if_lapsed(
        &self,
        socket: &WebSocket,
        request: &LinkRequest,
        reply: ReplyPayload,
        close_after_reply: Option<central_link_protocol::MachineLinkCloseCode>,
    ) -> Result<()> {
        let sent = send(
            socket,
            DataFrame::Reply {
                correlation_id: request.correlation_id().clone(),
                result: Box::new(reply),
            },
        );
        sent.and(
            close_after_reply.map_or(Ok(()), |cause| self.close_machine_authority(socket, cause)),
        )?;

        #[cfg(feature = "test-support")]
        if let Err(error) = self.witness_alarm_outcome(request.correlation_id()) {
            worker::console_warn!("Post-reply alarm diagnostic failed: {error}");
        }
        Ok(())
    }
}
pub(super) fn close_enrollment_sockets(
    state: &worker::State,
    pending_mac_calls: &PendingMacCalls,
    invalidation: &EnrollmentInvalidation,
    current: Option<&WebSocket>,
) {
    for socket in state.get_websockets_with_tag(invalidation.machine_id.as_str()) {
        if current.is_some_and(|current| current.as_ref() == socket.as_ref()) {
            continue;
        }
        if matches!(
            SocketAttachment::read(&socket),
            Ok(Some(attachment)) if attachment.authority.enrollment_id() == &invalidation.enrollment_id
        ) {
            let _ = pending_mac_calls.close(state, &socket, invalidation.cause.code(), None);
        }
    }
}

fn close_ended_connect(cause: central_link_protocol::MachineLinkCloseCode) -> Result<Response> {
    let pair = WebSocketPair::new()?;
    pair.server.accept()?;
    pair.server.close(Some(cause.code()), None::<&str>)?;
    Response::from_websocket(pair.client)
}
