//! Typed Mac calls keep their waiter on the physical socket in the owning object.

use super::workspace_machine_link::send;
use super::workspace_socket_attachment::SocketAttachment;
use super::{CentralObject, OrgRuntime, OrganizationId};
use crate::{cloudflare_transport, object_location::ObjectAddress};
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, FrameDirection, FreshCorrelationId, LinkFrame, MacCall,
    MacCallAddress, MacCallReply, MacLinkRpc, TypedCallCorrelations, TypedCallResult, encode_text,
};
use collaboration_central_core::{MacCallResult, TYPED_LINK_CALL_TIMEOUT, finish_mac_call};
use fleet_contracts::MachineConnectionAttachment;
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::{Clock, SyncTime};
use futures_channel::oneshot;
use futures_util::future::{Either, select};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    pin::pin,
    sync::{Arc, Mutex, PoisonError},
};
use worker::{Env, Method, Request, Response, State, WebSocket};
use workspace_ids::{MachineId, RouterRunId};

pub(super) const SOCKET_TAG_PREFIX: &str = "typed-link:";
pub(super) const MACHINE_CALL_PATH: &str = "/internal/machine-link/call";

type LinkReply = TypedCallResult<MacCallReply>;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MacCallInvocation {
    machine_id: MachineId,
    request: Box<MacCall>,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum MacCallAnswer {
    Replied { result: LinkReply },
    NeverReached,
    AnswerLost,
}

/// Call a registered RPC on a machine attached to the given organization.
///
/// The awaiting object event owns the waiter and prevents idle hibernation.
/// A reset loses that event. Its caller receives `AnswerLost` at the invocation
/// boundary, and a waking object has no waiter for the old attempt.
pub async fn call_machine<R: MacLinkRpc>(
    env: &Env,
    organization: OrganizationId,
    machine_id: MachineId,
    request: Envelope<R::Target, R::Request>,
) -> MacCallResult<R> {
    let target = request.target.clone();
    let answer = invoke_mac_call::<R>(env, organization, machine_id, request).await?;
    match answer {
        MacCallAnswer::Replied { result } => finish_mac_call::<R>(&target, result),
        MacCallAnswer::NeverReached => Err(CallFailure::NeverReached),
        MacCallAnswer::AnswerLost => Err(CallFailure::AnswerLost),
    }
}

async fn invoke_mac_call<R: MacLinkRpc>(
    env: &Env,
    organization: OrganizationId,
    machine_id: MachineId,
    request: Envelope<R::Target, R::Request>,
) -> Result<MacCallAnswer, CallFailure> {
    let request = R::request(request);
    let address = MacCallAddress::from(&request);
    if !address_agrees(&address, &organization, &machine_id) {
        return Err(CallFailure::NeverReached);
    }
    let organization = address.organization_id().cloned().unwrap_or(organization);
    cloudflare_transport::object_exchange(
        env,
        ObjectAddress::Central(organization),
        MACHINE_CALL_PATH,
        &MacCallInvocation {
            machine_id,
            request: Box::new(request),
        },
        None,
    )
    .await
}

fn address_agrees(
    address: &MacCallAddress,
    organization: &OrganizationId,
    machine: &MachineId,
) -> bool {
    address.machine_id() == machine
        && address
            .organization_id()
            .is_none_or(|target| target == organization)
}

impl CentralObject {
    pub(super) async fn dispatch_typed_machine_call(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        authority: &MachineConnectionAttachment,
        correlation_id: CorrelationId,
        request: Option<CentralCall>,
    ) -> worker::Result<()> {
        let Ok(dispatched) = runtime
            .dispatch_typed_central_call(authority, request)
            .await
        else {
            return self.close_machine_link(socket, 1011, Some("central unavailable"));
        };
        if let Some(run) = dispatched.registered_run {
            if !socket_open(socket) {
                return Ok(());
            }
            let result = bind_registered_run(socket, authority, run);
            if !matches!(result, Ok(true)) {
                let closed =
                    self.close_machine_link(socket, 1008, Some("socket registration changed"));
                return result.map(|_| ()).and(closed);
            }
        }
        self.send_central_call_result(socket, correlation_id, dispatched.result)
    }

    #[cfg(feature = "test-support")]
    pub(super) async fn dispatch_fixture_probe(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        authority: &MachineConnectionAttachment,
        correlation_id: CorrelationId,
        request: central_link_protocol::fixture_central_probe::ProbeRequestEnvelope,
    ) -> worker::Result<()> {
        match runtime
            .dispatch_fixture_central_probe(authority, request)
            .await
        {
            Ok(answer @ TypedCallResult::Answered { .. }) => {
                let text = central_link_protocol::fixture_central_probe::encode_probe_reply(
                    correlation_id,
                    answer,
                )
                .map_err(|error| worker::Error::RustError(error.to_string()))?;
                socket.send_with_str(&text)
            }
            Ok(TypedCallResult::Refused { reason }) => self.send_central_call_result(
                socket,
                correlation_id,
                TypedCallResult::Refused { reason },
            ),
            Err(_) => self.close_machine_link(socket, 1011, Some("central unavailable")),
        }
    }

    fn send_central_call_result(
        &self,
        socket: &WebSocket,
        correlation_id: CorrelationId,
        result: TypedCallResult<central_link_protocol::CentralCallReply>,
    ) -> worker::Result<()> {
        let lapsed = match &result {
            TypedCallResult::Refused { reason } => reason.authority_close(),
            TypedCallResult::Answered { .. } => None,
        };
        let frame = DataFrame::CentralCallReply {
            correlation_id,
            result: Box::new(result),
        };
        #[cfg(feature = "test-support")]
        let fail_send = lapsed.is_some() && self.controls.fail_typed_refusal_send.replace(false);
        #[cfg(not(feature = "test-support"))]
        let fail_send = false;
        let reply = if fail_send {
            Err(worker::Error::RustError(
                "fixture typed refusal send failed".into(),
            ))
        } else {
            send(socket, frame)
        };
        if let Some(cause) = lapsed {
            let closed = self.close_machine_authority(socket, cause);
            return reply.and(closed);
        }
        reply
    }

    pub(super) fn handle_mac_call_reply(
        &self,
        socket: &WebSocket,
        correlation: &CorrelationId,
        result: LinkReply,
    ) {
        self.pending_mac_calls
            .answer(&self.state, socket, correlation, result, self.clock.now());
    }
    pub(super) async fn handle_mac_call(
        &self,
        mut request: Request,
        organization: OrganizationId,
    ) -> worker::Result<Response> {
        if request.method() != Method::Post {
            return Response::error("method not allowed", 405);
        }
        let deadline = self.clock.now().after(TYPED_LINK_CALL_TIMEOUT);
        let Ok(invocation) = request.json::<MacCallInvocation>().await else {
            return Response::error("invalid call", 400);
        };
        let result = self.send_mac_call(organization, invocation, deadline).await;
        Response::from_json(&match result {
            Ok(result) => MacCallAnswer::Replied { result },
            Err(CallFailure::NeverReached) => MacCallAnswer::NeverReached,
            Err(CallFailure::AnswerLost) => MacCallAnswer::AnswerLost,
        })
    }

    async fn send_mac_call(
        &self,
        organization: OrganizationId,
        invocation: MacCallInvocation,
        deadline: SyncTime,
    ) -> Result<LinkReply, CallFailure> {
        let prepare = self.write_mac_call(organization, invocation, deadline);
        let written = match select(pin!(self.clock.wait_until(deadline)), pin!(prepare)).await {
            Either::Left(_) => return Err(CallFailure::NeverReached),
            Either::Right((written, _)) => written?,
        };
        let WrittenMacCall { pending, answer } = written;
        let answer = match select(answer, pin!(self.clock.wait_until(deadline))).await {
            Either::Left((answer, _)) => answer.map_err(|_| CallFailure::AnswerLost),
            Either::Right(_) => Err(CallFailure::AnswerLost),
        };
        drop(pending);
        answer
    }

    async fn write_mac_call(
        &self,
        organization: OrganizationId,
        invocation: MacCallInvocation,
        deadline: SyncTime,
    ) -> Result<WrittenMacCall<'_>, CallFailure> {
        let address = MacCallAddress::from(invocation.request.as_ref());
        if !address_agrees(&address, &organization, &invocation.machine_id) {
            return Err(CallFailure::NeverReached);
        }
        let socket = self
            .state
            .get_websockets_with_tag(address.machine_id().as_str())
            .into_iter()
            .find(socket_open)
            .ok_or(CallFailure::NeverReached)?;
        let attachment = matching_attachment(&socket, &address, &organization)?;
        let socket_tag = socket_tag(&self.state, &socket).ok_or(CallFailure::NeverReached)?;
        self.migrate()
            .await
            .map_err(|_| CallFailure::NeverReached)?;
        use collaboration_central_core::{CentralOrganizationIdentity, read_central_organization};
        let identity = read_central_organization(self.statement_host().as_ref())
            .await
            .map_err(|_| CallFailure::NeverReached)?;
        if matches!(identity, CentralOrganizationIdentity::Stamped { organization_id } if organization_id != organization)
        {
            return Err(CallFailure::NeverReached);
        }
        let runtime = self
            .runtime(organization.clone())
            .await
            .map_err(|_| CallFailure::NeverReached)?;
        runtime
            .register_active_machine(&attachment.authority, || {
                let current = matching_attachment(&socket, &address, &organization)?;
                if current.authority != attachment.authority {
                    return Err(CallFailure::NeverReached);
                }
                self.begin_mac_call(&socket, socket_tag, invocation.request, address, deadline)
            })
            .await
            .map_err(|_| CallFailure::NeverReached)?
            .map_err(|_| CallFailure::NeverReached)?
    }

    fn begin_mac_call(
        &self,
        socket: &WebSocket,
        socket_tag: String,
        request: Box<MacCall>,
        address: MacCallAddress,
        deadline: SyncTime,
    ) -> Result<WrittenMacCall<'_>, CallFailure> {
        if !socket_open(socket) || self.clock.now() >= deadline {
            return Err(CallFailure::NeverReached);
        }
        let correlation = FreshCorrelationId::mint_at(self.clock.now().unix_millis());
        let frame = LinkFrame::Data(DataFrame::CallMac {
            correlation_id: correlation.wire_id().clone(),
            request,
        });
        let text =
            encode_text(&frame, FrameDirection::ToRouter).map_err(|_| CallFailure::NeverReached)?;
        let written =
            self.pending_mac_calls
                .register(socket_tag, correlation, address, deadline)?;
        if self.clock.now() >= deadline {
            return Err(CallFailure::NeverReached);
        }
        socket
            .send_with_str(text)
            .map_err(|_| CallFailure::AnswerLost)?;
        Ok(written)
    }
}

fn matching_attachment(
    socket: &WebSocket,
    address: &MacCallAddress,
    organization: &OrganizationId,
) -> Result<SocketAttachment, CallFailure> {
    SocketAttachment::read(socket)
        .map_err(|_| CallFailure::NeverReached)?
        .filter(|attachment| {
            let authority = &attachment.authority;
            authority.organization_id() == organization
                && address.permits(
                    authority.organization_id(),
                    authority.machine_id(),
                    authority.enrollment_id(),
                    attachment.registered_run.as_ref(),
                )
        })
        .ok_or(CallFailure::NeverReached)
}

fn bind_registered_run(
    socket: &WebSocket,
    authenticated: &MachineConnectionAttachment,
    run: RouterRunId,
) -> worker::Result<bool> {
    let Some(mut current) = SocketAttachment::read(socket)? else {
        return Ok(false);
    };
    let authority = &current.authority;
    if authority.organization_id() != authenticated.organization_id()
        || authority.machine_id() != authenticated.machine_id()
        || authority.enrollment_id() != authenticated.enrollment_id()
        || authority.client_id() != authenticated.client_id()
    {
        return Ok(false);
    }
    let same_run = current
        .registered_run
        .as_ref()
        .is_none_or(|previous| previous == &run);
    current.registered_run = same_run.then_some(run);
    current.save(socket)?;
    Ok(same_run)
}

struct WrittenMacCall<'a> {
    pending: PendingAttempt<'a>,
    answer: oneshot::Receiver<LinkReply>,
}

struct Waiter {
    address: MacCallAddress,
    deadline: SyncTime,
    answer: oneshot::Sender<LinkReply>,
}

type SocketWaiters = BTreeMap<String, TypedCallCorrelations<Waiter>>;

#[derive(Clone, Default)]
pub(super) struct PendingMacCalls(Arc<Mutex<SocketWaiters>>);

impl PendingMacCalls {
    fn register(
        &self,
        socket_tag: String,
        correlation: FreshCorrelationId,
        address: MacCallAddress,
        deadline: SyncTime,
    ) -> Result<WrittenMacCall<'_>, CallFailure> {
        let (answer, receiver) = oneshot::channel();
        let correlation_id = correlation.wire_id().clone();
        self.lock()
            .entry(socket_tag.clone())
            .or_default()
            .register(
                correlation,
                Waiter {
                    address,
                    deadline,
                    answer,
                },
            )
            .map_err(|_| CallFailure::NeverReached)?;
        Ok(WrittenMacCall {
            pending: PendingAttempt {
                calls: self,
                socket_tag,
                correlation_id,
            },
            answer: receiver,
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SocketWaiters> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn retire(&self, socket_tag: &str, correlation_id: &CorrelationId) -> Option<Waiter> {
        self.lock().get_mut(socket_tag)?.retire(correlation_id)
    }

    fn answer(
        &self,
        state: &State,
        socket: &WebSocket,
        correlation_id: &CorrelationId,
        result: LinkReply,
        now: SyncTime,
    ) {
        let waiter = socket_tag(state, socket).and_then(|tag| {
            self.lock()
                .get_mut(&tag)?
                .take_if(correlation_id, |waiter| {
                    now < waiter.deadline && waiter.address.matches_reply(&result)
                })
        });
        if let Some(waiter) = waiter {
            let _ = waiter.answer.send(result);
        }
    }

    pub(super) fn close(
        &self,
        state: &State,
        socket: &WebSocket,
        code: u16,
        reason: Option<&str>,
    ) -> worker::Result<()> {
        if let Some(socket_tag) = socket_tag(state, socket) {
            self.lock().remove(&socket_tag);
        }
        socket.close(Some(code), reason)
    }

    #[cfg(feature = "test-support")]
    pub(super) fn is_waiting(&self, correlation: &CorrelationId) -> bool {
        self.lock().values().any(|socket| {
            socket
                .pending(correlation)
                .is_some_and(|waiter| !waiter.answer.is_canceled())
        })
    }
}

struct PendingAttempt<'a> {
    calls: &'a PendingMacCalls,
    socket_tag: String,
    correlation_id: CorrelationId,
}

impl Drop for PendingAttempt<'_> {
    fn drop(&mut self) {
        self.calls.retire(&self.socket_tag, &self.correlation_id);
    }
}

fn socket_tag(state: &State, socket: &WebSocket) -> Option<String> {
    state
        .as_raw()
        .get_tags(socket.as_ref())
        .ok()?
        .into_iter()
        .find(|tag| tag.starts_with(SOCKET_TAG_PREFIX))
}

fn socket_open(socket: &WebSocket) -> bool {
    socket.as_ref().ready_state() == web_sys::WebSocket::OPEN
}

#[cfg(feature = "test-support")]
pub(super) async fn probe(
    env: &Env,
    organization: OrganizationId,
    mut request: Request,
) -> worker::Result<Response> {
    use central_link_protocol::test_calls::{MacProbe, ProbeRequest};
    let request = request.json::<Envelope<MachineId, ProbeRequest>>().await?;
    fixture_response(
        call_machine::<MacProbe>(env, organization, request.target.clone(), request).await,
    )
}

#[cfg(feature = "test-support")]
pub(super) async fn probe_ending(
    env: &Env,
    organization: OrganizationId,
    mut request: Request,
) -> worker::Result<Response> {
    use central_link_protocol::approval_rpc::DeliverEndingRpc;
    use fleet_rpc::Rpc;
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct EndingProbe {
        machine_id: MachineId,
        request: Envelope<<DeliverEndingRpc as Rpc>::Target, <DeliverEndingRpc as Rpc>::Request>,
    }
    let probe = request.json::<EndingProbe>().await?;
    fixture_response(
        call_machine::<DeliverEndingRpc>(env, organization, probe.machine_id, probe.request).await,
    )
}

#[cfg(feature = "test-support")]
fn fixture_response<Reply: Serialize>(
    answer: Result<TypedCallResult<Reply>, CallFailure>,
) -> worker::Result<Response> {
    match answer {
        Ok(TypedCallResult::Answered { reply }) => Response::from_json(&reply),
        Ok(TypedCallResult::Refused { reason }) => {
            Response::from_json(&TypedCallResult::<()>::Refused { reason })
        }
        Err(CallFailure::NeverReached) => {
            Ok(Response::from_json(&MacCallAnswer::NeverReached)?.with_status(409))
        }
        Err(CallFailure::AnswerLost) => {
            Ok(Response::from_json(&MacCallAnswer::AnswerLost)?.with_status(503))
        }
    }
}
