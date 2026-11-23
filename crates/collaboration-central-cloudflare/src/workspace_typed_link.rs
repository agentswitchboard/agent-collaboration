//! Typed Mac calls keep their waiter on the physical socket in the owning object.

use super::workspace_machine_link::send;
#[path = "workspace_approval_delivery.rs"]
pub(super) mod approval_delivery;
#[path = "workspace_pending_mac_calls.rs"]
mod pending_mac_calls;
use super::workspace_socket_attachment::SocketAttachment;
use super::{CentralObject, OrgRuntime, OrganizationId};
use crate::{cloudflare_transport, object_location::ObjectAddress};
use central_link_protocol::{
    CentralCall, CorrelationId, DataFrame, MacCall, MacCallAddress, MacCallReply, MacLinkRpc,
    TypedCallResult,
};
use collaboration_central_core::{
    MacCallResult, TYPED_LINK_CALL_TIMEOUT, TypedCallCarrier, finish_mac_call,
    is_approval_holder_call,
};
use fleet_contracts::MachineConnectionAttachment;
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::{Clock, SyncTime};
use futures_util::future::{Either, select};
pub(super) use pending_mac_calls::PendingMacCalls;
use pending_mac_calls::WrittenMacCall;
use serde::{Deserialize, Serialize};
use std::pin::pin;
use worker::{Env, Method, Request, Response, State, WebSocket};
use workspace_ids::{MachineId, RouterRunId};

pub(super) const SOCKET_TAG_PREFIX: &str = "typed-link:";
pub(super) const MACHINE_CALL_PATH: &str = "/internal/machine-link/call";
#[cfg(feature = "test-support")]
pub(super) const FIXTURE_ENDING_CALL_PATH: &str = "/test/fixture-mac-ending-transport";

pub(super) fn is_mac_call_path(path: &str) -> bool {
    #[cfg(feature = "test-support")]
    if path == FIXTURE_ENDING_CALL_PATH {
        return true;
    }
    path == MACHINE_CALL_PATH
}

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
    let call = R::request(request);
    if matches!(call, MacCall::DeliverEnding(_)) {
        return Err(CallFailure::NeverReached);
    }
    let answer = invoke_mac_call(env, organization, machine_id, call, MACHINE_CALL_PATH).await?;
    project_mac_call_answer::<R>(&target, answer)
}

fn project_mac_call_answer<R: MacLinkRpc>(
    target: &R::Target,
    answer: MacCallAnswer,
) -> MacCallResult<R> {
    match answer {
        MacCallAnswer::Replied { result } => finish_mac_call::<R>(target, result),
        MacCallAnswer::NeverReached => Err(CallFailure::NeverReached),
        MacCallAnswer::AnswerLost => Err(CallFailure::AnswerLost),
    }
}

async fn invoke_mac_call(
    env: &Env,
    organization: OrganizationId,
    machine_id: MachineId,
    request: MacCall,
    path: &str,
) -> Result<MacCallAnswer, CallFailure> {
    let address = MacCallAddress::from(&request);
    if !address_agrees(&address, &organization, &machine_id) {
        return Err(CallFailure::NeverReached);
    }
    let organization = address.organization_id().cloned().unwrap_or(organization);
    cloudflare_transport::object_exchange(
        env,
        ObjectAddress::Central(organization),
        path,
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
    pub(super) async fn announce_mac_capabilities(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        attachment: &MachineConnectionAttachment,
        announcement: central_link_protocol::MacCapabilities,
    ) -> worker::Result<()> {
        let turn = runtime.runtime().take_central_turn_for_admission().await;
        if !is_current_socket(&self.state, socket, attachment.machine_id()) {
            return Ok(());
        }
        let Some(mut current) = SocketAttachment::read(socket)? else {
            return self.close_machine_link(socket, 1008, Some("connection identity missing"));
        };
        if current.mac_capabilities.announce(announcement).is_err() {
            return self.close_machine_link(socket, 1008, Some("duplicate Mac capabilities"));
        }
        current.save(socket)?;
        drop(turn);
        if current
            .mac_capabilities
            .handles(central_link_protocol::MacCallKind::DeliverEnding)
            && let Some(run) = current.registered_run
        {
            match runtime
                .restore_machine_link_request(&current.authority)
                .await
            {
                Ok(collaboration_central_core::MachineLinkRequestAuthority::Admitted(machine)) => {
                    if let Some(recovery) = runtime.recover_approval_endings(machine, run) {
                        self.pending_mac_calls
                            .run_socket_completion(&self.state, socket, recovery);
                    }
                }
                Ok(collaboration_central_core::MachineLinkRequestAuthority::Refused(cause)) => {
                    return self.close_machine_authority(socket, cause);
                }
                Ok(
                    collaboration_central_core::MachineLinkRequestAuthority::TemporarilyUnavailable,
                )
                | Err(_) => {
                    return self.close_machine_link(socket, 1011, Some("central unavailable"));
                }
            }
        }
        Ok(())
    }

    pub(super) async fn dispatch_typed_machine_call(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        authority: &MachineConnectionAttachment,
        correlation_id: CorrelationId,
        request: Option<CentralCall>,
    ) -> worker::Result<()> {
        let (carrier, current_authority, turn) = if is_approval_holder_call(request.as_ref()) {
            #[cfg(feature = "test-support")]
            runtime
                .runtime()
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::BeforeApprovalCarrierTurn)
                .await;
            let turn = runtime.runtime().take_central_turn_for_admission().await;
            if !is_current_socket(&self.state, socket, authority.machine_id()) {
                return Ok(());
            }
            let Some(current) = SocketAttachment::read(socket)? else {
                return self.close_machine_link(socket, 1008, Some("connection identity missing"));
            };
            let recover_after_registration = current.registered_run.is_none()
                && current
                    .mac_capabilities
                    .handles(central_link_protocol::MacCallKind::DeliverEnding);
            (
                TypedCallCarrier::Approval {
                    registered_run: current.registered_run,
                    recover_after_registration,
                },
                current.authority,
                Some(turn),
            )
        } else {
            (TypedCallCarrier::Session, authority.clone(), None)
        };
        let Ok(dispatched) = runtime
            .dispatch_typed_central_call(&current_authority, request, carrier)
            .await
        else {
            return self.close_machine_link(socket, 1011, Some("central unavailable"));
        };
        if let Some(run) = dispatched.registered_run {
            if !socket_open(socket) {
                return Ok(());
            }
            let result = bind_registered_run(socket, &current_authority, run);
            if !matches!(result, Ok(true)) {
                let closed =
                    self.close_machine_link(socket, 1008, Some("socket registration changed"));
                return result.map(|_| ()).and(closed);
            }
        }
        drop(turn);
        self.send_central_call_result(socket, correlation_id, dispatched.result)?;
        if let Some(completion) = dispatched.completion {
            self.pending_mac_calls
                .run_socket_completion(&self.state, socket, completion);
        }
        Ok(())
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
        let is_raw_ending = matches!(&*invocation.request, MacCall::DeliverEnding(_));
        #[cfg(not(feature = "test-support"))]
        let fixture_ending = false;
        #[cfg(feature = "test-support")]
        let fixture_ending = request.path() == FIXTURE_ENDING_CALL_PATH;
        let result = if is_raw_ending && !fixture_ending {
            Err(CallFailure::NeverReached)
        } else {
            self.send_mac_call(organization, invocation, deadline).await
        };
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
                self.pending_mac_calls.begin_mac_call(
                    &self.clock,
                    &socket,
                    socket_tag,
                    invocation.request,
                    address,
                    deadline,
                )
            })
            .await
            .map_err(|_| CallFailure::NeverReached)?
            .map_err(|_| CallFailure::NeverReached)?
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
    if same_run {
        current.registered_run = Some(run);
        current.save(socket)?;
    }
    Ok(same_run)
}

fn is_current_socket(state: &State, socket: &WebSocket, machine: &MachineId) -> bool {
    let Some(physical_tag) = socket_tag(state, socket) else {
        return false;
    };
    state
        .get_websockets_with_tag(machine.as_str())
        .into_iter()
        .find(socket_open)
        .and_then(|current| socket_tag(state, &current))
        .is_some_and(|tag| tag == physical_tag)
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
    path: &str,
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
    let target = probe.request.target.clone();
    let call = DeliverEndingRpc::request(probe.request);
    fixture_response(
        invoke_mac_call(env, organization, probe.machine_id, call, path)
            .await
            .and_then(|answer| project_mac_call_answer::<DeliverEndingRpc>(&target, answer)),
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
