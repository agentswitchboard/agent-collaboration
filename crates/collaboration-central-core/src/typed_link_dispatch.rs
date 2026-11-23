//! Authenticate typed link calls before delegating them to their registered owner.
use crate::{MachineLinkRequestAuthority, ValidatedMachineConnection, WorkspaceRuntime};
use central_link_protocol::{
    CentralCall, CentralCallReply, CentralLinkRpc, LinkCallRefusal, MacCallReply, MacLinkRpc,
    TypedCallResult, approval_rpc::RegisterRunReply,
};
use central_storage::StoreFailure;
use fleet_contracts::MachineConnectionAttachment;
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use workspace_ids::RouterRunId;

pub const TYPED_LINK_CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub type MacCallResult<R> = Result<
    TypedCallResult<Envelope<<R as fleet_rpc::Rpc>::Target, <R as fleet_rpc::Rpc>::Reply>>,
    CallFailure,
>;

/// Dispatches a registered call using the machine identity verified on this connection.
pub trait CentralLinkDispatcher: Send + Sync {
    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
    ) -> Pin<Box<dyn Future<Output = TypedCallResult<CentralCallReply>> + Send + 'call>>;
}

/// A correlated answer and any run binding authorized for the originating socket.
pub struct DispatchedCentralCall {
    pub result: TypedCallResult<CentralCallReply>,
    pub registered_run: Option<RouterRunId>,
}

impl DispatchedCentralCall {
    fn refused(reason: LinkCallRefusal) -> Self {
        Self {
            result: TypedCallResult::Refused { reason },
            registered_run: None,
        }
    }
}

/// Check the receiver before applying the typed route and tag the reply with its marker.
pub async fn dispatch_registered_central_call<R, Handler, Answer>(
    expected_target: &R::Target,
    request: Envelope<R::Target, R::Request>,
    handler: Handler,
) -> TypedCallResult<CentralCallReply>
where
    R: CentralLinkRpc,
    Handler: FnOnce(R::Request) -> Answer,
    Answer: Future<Output = R::Reply>,
{
    match fleet_rpc::dispatch::<R, _, _>(expected_target, request, handler).await {
        Ok(envelope) => TypedCallResult::Answered {
            reply: R::reply(envelope),
        },
        Err(fleet_rpc::TargetMismatch) => TypedCallResult::Refused {
            reason: LinkCallRefusal::WrongTarget,
        },
    }
}

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub fn with_link_call_dispatcher(mut self, dispatcher: Arc<dyn CentralLinkDispatcher>) -> Self {
        self.link_call_dispatcher = Some(dispatcher);
        self
    }

    /// A missing request is a well-formed but unregistered frame header from the decoder.
    pub async fn dispatch_typed_central_call(
        &self,
        attachment: &MachineConnectionAttachment,
        request: Option<CentralCall>,
    ) -> Result<DispatchedCentralCall, StoreFailure> {
        let machine = match self.restore_machine_link_request(attachment).await? {
            MachineLinkRequestAuthority::Admitted(machine) => machine,
            MachineLinkRequestAuthority::Refused(cause) => {
                return Ok(DispatchedCentralCall::refused(
                    LinkCallRefusal::AuthorityLapsed { cause },
                ));
            }
            MachineLinkRequestAuthority::TemporarilyUnavailable => {
                return Err(StoreFailure::Unavailable);
            }
        };
        let (Some(dispatcher), Some(request)) = (self.link_call_dispatcher.as_deref(), request)
        else {
            return Ok(DispatchedCentralCall::refused(LinkCallRefusal::UnknownCall));
        };
        let requested_run = match &request {
            CentralCall::RegisterRun(envelope)
                if &envelope.target.organization_id == machine.organization_id() =>
            {
                Some(envelope.body.router_run.clone())
            }
            _ => None,
        };
        let result = dispatcher.dispatch(&machine, request).await;
        let registered_run = requested_run.filter(|_| {
            matches!(
                &result,
                TypedCallResult::Answered {
                    reply: CentralCallReply::RegisterRun(envelope),
                } if &envelope.target.organization_id == machine.organization_id()
                    && matches!(envelope.body, RegisterRunReply::Registered {})
            )
        });
        Ok(DispatchedCentralCall {
            result,
            registered_run,
        })
    }
}

/// Preserve link refusals separately from the service answer and transport failure.
pub fn finish_mac_call<R: MacLinkRpc>(
    expected_target: &R::Target,
    result: TypedCallResult<MacCallReply>,
) -> MacCallResult<R> {
    match result {
        TypedCallResult::Answered { reply } => R::take_reply(reply)
            .filter(|envelope| &envelope.target == expected_target)
            .map(|reply| TypedCallResult::Answered { reply })
            .ok_or(CallFailure::AnswerLost),
        TypedCallResult::Refused { reason } => Ok(TypedCallResult::Refused { reason }),
    }
}

#[cfg(feature = "test-support")]
pub(crate) struct FixtureLinkDispatcher;

#[cfg(feature = "test-support")]
impl CentralLinkDispatcher for FixtureLinkDispatcher {
    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
    ) -> Pin<Box<dyn Future<Output = TypedCallResult<CentralCallReply>> + Send + 'call>> {
        use central_link_protocol::{
            approval_rpc::{ApprovalServiceTarget, RegisterRunRpc},
            test_calls::{CentralProbe, ProbeReply},
        };
        Box::pin(async move {
            match request {
                CentralCall::CentralProbe(envelope) => {
                    dispatch_registered_central_call::<CentralProbe, _, _>(
                        machine.machine_id(),
                        envelope,
                        |body| async move {
                            ProbeReply {
                                echoed: body.sequence,
                            }
                        },
                    )
                    .await
                }
                CentralCall::RegisterRun(envelope) => {
                    let target = ApprovalServiceTarget {
                        organization_id: machine.organization_id().clone(),
                    };
                    dispatch_registered_central_call::<RegisterRunRpc, _, _>(
                        &target,
                        envelope,
                        |_| async { RegisterRunReply::Registered {} },
                    )
                    .await
                }
                CentralCall::Raise(_) | CentralCall::Stop(_) => TypedCallResult::Refused {
                    reason: LinkCallRefusal::UnknownCall,
                },
            }
        })
    }
}
