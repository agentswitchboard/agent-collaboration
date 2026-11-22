//! Typed Mac calls keep their waiter on the physical socket in the owning object.

use super::{CentralObject, OrganizationId};
use crate::{cloudflare_transport, object_location::ObjectAddress};
use central_link_protocol::{
    CorrelationId, DataFrame, MacCall, MacCallReply, MacLinkRpc, TypedCallCorrelations,
    TypedCallResult,
};
use collaboration_central_core::{TYPED_LINK_CALL_TIMEOUT, finish_mac_call};
use fleet_contracts::MachineConnectionAttachment;
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::{Clock, IdSource, SyncTime};
use futures_channel::oneshot;
use futures_util::future::{Either, select};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    pin::pin,
    sync::{Arc, Mutex, PoisonError},
};
use worker::{Env, Method, Request, Response, State, WebSocket};
use workspace_ids::MachineId;

pub(super) const SOCKET_TAG_PREFIX: &str = "typed-link:";
pub(super) const MACHINE_CALL_PATH: &str = "/internal/machine-link/call";

type LinkReply = TypedCallResult<MacCallReply>;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MacCallInvocation {
    machine_id: MachineId,
    request: MacCall,
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
) -> Result<Envelope<R::Target, R::Reply>, CallFailure> {
    let target = request.target.clone();
    let request = MacCallInvocation {
        machine_id,
        request: R::request(request),
    };
    let answer = cloudflare_transport::object_exchange(
        env,
        ObjectAddress::Central(organization),
        MACHINE_CALL_PATH,
        &request,
        None,
    )
    .await?;
    match answer {
        MacCallAnswer::Replied { result } => finish_mac_call::<R>(&target, result),
        MacCallAnswer::NeverReached => Err(CallFailure::NeverReached),
        MacCallAnswer::AnswerLost => Err(CallFailure::AnswerLost),
    }
}

impl CentralObject {
    pub(super) async fn handle_mac_call(
        &self,
        mut request: Request,
        organization: OrganizationId,
    ) -> worker::Result<Response> {
        if request.method() != Method::Post {
            return Response::error("method not allowed", 405);
        }
        let Ok(invocation) = request.json::<MacCallInvocation>().await else {
            return Response::error("invalid call", 400);
        };
        let result = self.send_mac_call(organization, invocation).await;
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
    ) -> Result<LinkReply, CallFailure> {
        let socket = self
            .state
            .get_websockets_with_tag(invocation.machine_id.as_str())
            .into_iter()
            .find(socket_open)
            .ok_or(CallFailure::NeverReached)?;
        let attachment = socket
            .deserialize_attachment::<MachineConnectionAttachment>()
            .map_err(|_| CallFailure::NeverReached)?
            .filter(|attachment| {
                attachment.machine_id() == &invocation.machine_id
                    && attachment.organization_id() == &organization
            })
            .ok_or(CallFailure::NeverReached)?;
        let socket_tag = socket_tag(&self.state, &socket).ok_or(CallFailure::NeverReached)?;
        let runtime = self
            .runtime(organization)
            .await
            .map_err(|_| CallFailure::NeverReached)?;
        let (pending, answer, deadline) = runtime
            .register_active_machine(&attachment, || {
                self.begin_mac_call(&socket, socket_tag, invocation.request)
            })
            .await
            .map_err(|_| CallFailure::NeverReached)?
            .map_err(|_| CallFailure::NeverReached)??;
        let answer = match select(answer, pin!(self.clock.wait_until(deadline))).await {
            Either::Left((answer, _)) => answer.map_err(|_| CallFailure::AnswerLost),
            Either::Right(_) => Err(CallFailure::AnswerLost),
        };
        drop(pending);
        answer
    }

    fn begin_mac_call(
        &self,
        socket: &WebSocket,
        socket_tag: String,
        request: MacCall,
    ) -> Result<(PendingAttempt<'_>, oneshot::Receiver<LinkReply>, SyncTime), CallFailure> {
        if !socket_open(socket) {
            return Err(CallFailure::NeverReached);
        }
        let correlation_id: CorrelationId = self
            .link_ids
            .new_id()
            .to_string()
            .parse()
            .map_err(|_| CallFailure::NeverReached)?;
        let frame = DataFrame::CallMac {
            correlation_id: correlation_id.clone(),
            request: Box::new(request),
        };
        let text = central_link_protocol::encode_text(
            &central_link_protocol::LinkFrame::Data(frame),
            central_link_protocol::FrameDirection::ToRouter,
        )
        .map_err(|_| CallFailure::NeverReached)?;
        let deadline = self.clock.now().after(TYPED_LINK_CALL_TIMEOUT);
        let (pending, answer) =
            self.pending_mac_calls
                .register(socket_tag, correlation_id, deadline)?;
        // Register before sending. A send error cannot establish whether the Mac read it.
        socket
            .send_with_str(text)
            .map_err(|_| CallFailure::AnswerLost)?;
        Ok((pending, answer, deadline))
    }
}

struct Waiter {
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
        correlation_id: CorrelationId,
        deadline: SyncTime,
    ) -> Result<(PendingAttempt<'_>, oneshot::Receiver<LinkReply>), CallFailure> {
        let (answer, receiver) = oneshot::channel();
        self.lock()
            .entry(socket_tag.clone())
            .or_default()
            .register(correlation_id.clone(), Waiter { deadline, answer })
            .map_err(|_| CallFailure::NeverReached)?;
        Ok((
            PendingAttempt {
                calls: self,
                socket_tag,
                correlation_id,
            },
            receiver,
        ))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SocketWaiters> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn retire(&self, socket_tag: &str, correlation_id: &CorrelationId) -> Option<Waiter> {
        self.lock().get_mut(socket_tag)?.retire(correlation_id)
    }

    pub(super) fn answer(
        &self,
        state: &State,
        socket: &WebSocket,
        correlation_id: &CorrelationId,
        result: LinkReply,
        now: SyncTime,
    ) {
        if let Some(socket_tag) = socket_tag(state, socket)
            && let Some(waiter) = self.retire(&socket_tag, correlation_id)
            && now < waiter.deadline
        {
            let _ = waiter.answer.send(result);
        }
    }

    pub(super) fn close_socket(&self, state: &State, socket: &WebSocket) {
        if let Some(socket_tag) = socket_tag(state, socket) {
            self.lock().remove(&socket_tag);
        }
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
    match call_machine::<MacProbe>(env, organization, request.target.clone(), request).await {
        Ok(answer) => Response::from_json(&answer),
        Err(CallFailure::NeverReached) => {
            Ok(Response::from_json(&MacCallAnswer::NeverReached)?.with_status(409))
        }
        Err(CallFailure::AnswerLost) => {
            Ok(Response::from_json(&MacCallAnswer::AnswerLost)?.with_status(503))
        }
    }
}
