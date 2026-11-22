//! Authenticate typed link calls before delegating them to their registered owner.
use crate::{MachineLinkRequestAuthority, ValidatedMachineConnection, WorkspaceRuntime};
use central_link_protocol::{
    CentralCall, CentralCallReply, CentralLinkRpc, LinkCallRefusal, MacCallReply, MacLinkRpc,
    TypedCallResult,
};
use central_storage::StoreFailure;
use fleet_contracts::MachineConnectionAttachment;
use fleet_rpc::{CallFailure, Envelope};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub const TYPED_LINK_CALL_TIMEOUT: Duration = Duration::from_secs(5);

/// Dispatches a registered call using the machine identity verified on this connection.
pub trait CentralLinkDispatcher: Send + Sync {
    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
    ) -> Pin<Box<dyn Future<Output = TypedCallResult<CentralCallReply>> + Send + 'call>>;
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
    ) -> Result<TypedCallResult<CentralCallReply>, StoreFailure> {
        let machine = match self.restore_machine_link_request(attachment).await? {
            MachineLinkRequestAuthority::Admitted(machine) => machine,
            MachineLinkRequestAuthority::Refused(cause) => {
                return Ok(TypedCallResult::Refused {
                    reason: LinkCallRefusal::AuthorityLapsed { cause },
                });
            }
            MachineLinkRequestAuthority::TemporarilyUnavailable => {
                return Err(StoreFailure::Unavailable);
            }
        };
        match (self.link_call_dispatcher.as_deref(), request) {
            (Some(dispatcher), Some(request)) => Ok(dispatcher.dispatch(&machine, request).await),
            _ => Ok(TypedCallResult::Refused {
                reason: LinkCallRefusal::UnknownCall,
            }),
        }
    }
}

/// A refusal happens before dispatch. A reply with the wrong route or target is lost.
pub fn finish_mac_call<R: MacLinkRpc>(
    expected_target: &R::Target,
    result: TypedCallResult<MacCallReply>,
) -> Result<Envelope<R::Target, R::Reply>, CallFailure> {
    match result {
        TypedCallResult::Answered { reply } => R::take_reply(reply)
            .filter(|envelope| &envelope.target == expected_target)
            .ok_or(CallFailure::AnswerLost),
        TypedCallResult::Refused {
            reason:
                LinkCallRefusal::UnknownCall
                | LinkCallRefusal::WrongTarget
                | LinkCallRefusal::AuthorityLapsed { .. },
        } => Err(CallFailure::NeverReached),
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
        use central_link_protocol::test_calls::{CentralProbe, ProbeReply};
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
                CentralCall::RegisterRun(_) | CentralCall::Raise(_) | CentralCall::Stop(_) => {
                    TypedCallResult::Refused {
                        reason: LinkCallRefusal::UnknownCall,
                    }
                }
            }
        })
    }
}
