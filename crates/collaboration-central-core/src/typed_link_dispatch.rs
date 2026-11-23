//! Authenticate typed link calls before delegating them to their registered owner.
use crate::{MachineLinkRequestAuthority, ValidatedMachineConnection, WorkspaceRuntime};
use central_link_protocol::{
    CentralCall, CentralCallReply, CentralLinkRpc, LinkCallRefusal, MacCallReply, MacLinkRpc,
    TypedCallResult, approval_rpc::RegisterRunReply,
};
use central_storage::StoreFailure;
use fleet_contracts::MachineConnectionAttachment;
use fleet_rpc::{CallFailure, CorrelatedRpc, Envelope, IndependentReplyRpc};
use fleet_runtime_ports::{CentralStore, WorkspaceStores};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use workspace_ids::RouterRunId;

pub const TYPED_LINK_CALL_TIMEOUT: Duration = Duration::from_secs(5);

pub type MacCallResult<R> = Result<
    TypedCallResult<Envelope<<R as fleet_rpc::Rpc>::Target, <R as fleet_rpc::Rpc>::Reply>>,
    CallFailure,
>;

pub type CentralCallDispatch = Result<TypedCallResult<CentralCallReply>, StoreFailure>;

/// Dispatches a registered call using the machine identity verified on this connection.
pub trait CentralLinkDispatcher: Send + Sync {
    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
    ) -> Pin<Box<dyn Future<Output = CentralCallDispatch> + Send + 'call>>;
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
    R: CentralLinkRpc + IndependentReplyRpc,
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

/// A call addressed to the authenticated organization, checked before borrowing its store.
pub struct TargetCheckedCentralCall<R: CentralLinkRpc>(Envelope<R::Target, R::Request>);

impl<R: CentralLinkRpc> TargetCheckedCentralCall<R> {
    pub fn check(
        expected_target: &R::Target,
        request: Envelope<R::Target, R::Request>,
    ) -> Result<Self, LinkCallRefusal> {
        if &request.target == expected_target {
            Ok(Self(request))
        } else {
            Err(LinkCallRefusal::WrongTarget)
        }
    }
}

/// Apply a checked route whose reply depends only on its target.
pub async fn dispatch_fallible_central_call<R, Handler, Answer, Failure>(
    request: TargetCheckedCentralCall<R>,
    handler: Handler,
) -> Result<TypedCallResult<CentralCallReply>, Failure>
where
    R: CentralLinkRpc + IndependentReplyRpc,
    Handler: FnOnce(R::Request) -> Answer,
    Answer: Future<Output = Result<R::Reply, Failure>>,
{
    dispatch_fallible(request, handler, Ok).await
}

/// Validate a correlated service answer while the caller still owns the transaction.
pub async fn dispatch_fallible_correlated_central_call<R, Handler, Answer, Failure>(
    request: TargetCheckedCentralCall<R>,
    handler: Handler,
) -> Result<TypedCallResult<CentralCallReply>, Failure>
where
    R: CentralLinkRpc + CorrelatedRpc,
    Handler: FnOnce(R::Request) -> Answer,
    Answer: Future<Output = Result<R::Reply, Failure>>,
    Failure: From<R::InvalidReply>,
{
    let context = R::reply_context(&request.0.body);
    dispatch_fallible(request, handler, |reply| {
        R::check_reply(context, reply)
            .map(R::into_wire)
            .map_err(Failure::from)
    })
    .await
}

async fn dispatch_fallible<R, Handler, Answer, Failure>(
    request: TargetCheckedCentralCall<R>,
    handler: Handler,
    verify_reply: impl FnOnce(R::Reply) -> Result<R::Reply, Failure>,
) -> Result<TypedCallResult<CentralCallReply>, Failure>
where
    R: CentralLinkRpc,
    Handler: FnOnce(R::Request) -> Answer,
    Answer: Future<Output = Result<R::Reply, Failure>>,
{
    let Envelope { target, body } = request.0;
    let reply = verify_reply(handler(body).await?)?;
    Ok(TypedCallResult::Answered {
        reply: R::reply(Envelope {
            target,
            body: reply,
        }),
    })
}

impl<Host: CentralStore, Stores: WorkspaceStores> WorkspaceRuntime<Host, Stores> {
    pub fn with_link_call_dispatcher(mut self, dispatcher: Arc<dyn CentralLinkDispatcher>) -> Self {
        self.link_call_dispatcher = Some(dispatcher);
        self
    }

    #[cfg(feature = "test-support")]
    pub fn with_fixture_link_call_dispatcher(
        mut self,
        dispatcher: Arc<dyn CentralLinkDispatcher>,
    ) -> Self {
        self.fixture_link_calls = true;
        self.with_link_call_dispatcher(Arc::new(FixtureLinkDispatcher { dispatcher }))
    }

    #[cfg(feature = "test-support")]
    pub fn fixture_link_calls_enabled(&self) -> bool {
        self.fixture_link_calls
    }

    #[cfg(feature = "test-support")]
    pub async fn dispatch_fixture_central_probe(
        &self,
        attachment: &MachineConnectionAttachment,
        request: central_link_protocol::fixture_central_probe::ProbeRequestEnvelope,
    ) -> Result<
        TypedCallResult<central_link_protocol::fixture_central_probe::ProbeReplyEnvelope>,
        StoreFailure,
    > {
        use central_link_protocol::test_calls::ProbeReply;
        if !self.fixture_link_calls {
            return Ok(TypedCallResult::Refused {
                reason: LinkCallRefusal::UnknownCall,
            });
        }
        let result = match self.restore_machine_link_request(attachment).await? {
            MachineLinkRequestAuthority::Admitted(machine)
                if machine.machine_id() == &request.target =>
            {
                TypedCallResult::Answered {
                    reply: Envelope {
                        target: request.target,
                        body: ProbeReply {
                            echoed: request.body.sequence,
                        },
                    },
                }
            }
            MachineLinkRequestAuthority::Admitted(_) => TypedCallResult::Refused {
                reason: LinkCallRefusal::WrongTarget,
            },
            MachineLinkRequestAuthority::Refused(cause) => TypedCallResult::Refused {
                reason: LinkCallRefusal::AuthorityLapsed { cause },
            },
            MachineLinkRequestAuthority::TemporarilyUnavailable => {
                return Err(StoreFailure::Unavailable);
            }
        };
        Ok(result)
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
        #[cfg(feature = "test-support")]
        if matches!(
            &request,
            CentralCall::CreateFleetSession(_)
                | CentralCall::BindFleetSession(_)
                | CentralCall::EndFleetSession(_)
        ) {
            self.execution
                .pause_points()
                .reached_central(fleet_runtime::PausePoint::BeforeSessionTransaction)
                .await;
        }
        let result = dispatcher.dispatch(&machine, request).await?;
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
struct FixtureLinkDispatcher {
    dispatcher: Arc<dyn CentralLinkDispatcher>,
}

#[cfg(feature = "test-support")]
impl CentralLinkDispatcher for FixtureLinkDispatcher {
    fn dispatch<'call>(
        &'call self,
        machine: &'call ValidatedMachineConnection,
        request: CentralCall,
    ) -> Pin<Box<dyn Future<Output = CentralCallDispatch> + Send + 'call>> {
        use central_link_protocol::approval_rpc::{ApprovalServiceTarget, RegisterRunRpc};
        Box::pin(async move {
            Ok(match request {
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
                other @ (CentralCall::CreateFleetSession(_)
                | CentralCall::BindFleetSession(_)
                | CentralCall::EndFleetSession(_)
                | CentralCall::Raise(_)
                | CentralCall::Stop(_)) => return self.dispatcher.dispatch(machine, other).await,
            })
        })
    }
}
