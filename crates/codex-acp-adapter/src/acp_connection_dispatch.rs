//! ACP connection routing over bounded carriers and one shared native backend.
use crate::session_setup_task::{
    SetupOutcome, SetupTaskInputs, SetupTaskOutput, run_held_adoption, run_session_setup,
};
use crate::{
    AcpConnectionContext, AcpRouteFuture, AcpRouterChannels, AcpSchemaCatalog, AcpSessionBinding,
    AcpSessionRegistry, AcpSessionRoute, ConversationOperationRecorder, HeldBindingCheckout,
    UnmaterializedBindingStore, serve_acp_router_connection,
};
use codex_native_integration::NativePayloadSchemas;
use collaboration_protocol::{CodexGeneration, OperationId, UuidIdentity};
use serde_json::{Value, json};
use std::{future::Future, io, path::PathBuf, pin::Pin, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

/// Implemented by the shared stored-catalog owner; ACP never scans a second history store.
pub trait AcpStoredSessions: Send + Sync {
    fn list(&self, params: Value) -> Pin<Box<dyn Future<Output = io::Result<Value>> + Send + '_>>;
}
pub struct AcpConnectionInputs {
    pub backend_path: PathBuf,
    pub generation: CodexGeneration,
    pub schemas: Arc<NativePayloadSchemas>,
    pub stored_sessions: Arc<dyn AcpStoredSessions>,
    pub retired: CancellationToken,
    pub holder: Arc<dyn UnmaterializedBindingStore>,
    pub recorder: Arc<dyn ConversationOperationRecorder>,
    /// The Host's record of scratch scopes with an open session.
    pub scratch_scopes: Arc<dyn crate::ScratchScopeRegister>,
}
pub async fn serve_acp_connection<TStream: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
    stream: TStream,
    inputs: AcpConnectionInputs,
) -> io::Result<()> {
    serve_acp_router_connection(stream, vec![Box::new(CodexAcpSessionRoute { inputs })]).await
}

struct CodexAcpSessionRoute {
    inputs: AcpConnectionInputs,
}

impl AcpSessionRoute for CodexAcpSessionRoute {
    fn endpoint_id(&self) -> &str {
        "codex-local"
    }

    fn run(
        self: Box<Self>,
        router: AcpRouterChannels,
        _context: AcpConnectionContext,
    ) -> AcpRouteFuture {
        Box::pin(route_codex_sessions(router, self.inputs, None))
    }
}

pub(crate) async fn route_codex_sessions(
    mut router: AcpRouterChannels,
    inputs: AcpConnectionInputs,
    local_service: Option<UuidIdentity>,
) -> io::Result<()> {
    let mut schema = AcpSchemaCatalog::load().map_err(io::Error::other)?;
    let actor_retirement = inputs.retired.child_token();
    let mut sessions = AcpSessionRegistry::new(
        router.output.clone(),
        actor_retirement,
        Arc::clone(&inputs.holder),
    );
    let mut catalog_requests: tokio::task::JoinSet<(Value, io::Result<Value>)> =
        tokio::task::JoinSet::new();
    let mut setup_requests: tokio::task::JoinSet<(Value, Option<String>, SetupCompletion)> =
        tokio::task::JoinSet::new();
    let result=async {
        loop {
            let collect_setup = !setup_requests.is_empty();
            #[cfg(feature = "test-support")]
            let collect_setup = collect_setup && !crate::test_support::binding_preservation::hold_collection();
            let frame=tokio::select! {
                _=router.closed.cancelled()=>break Ok(()),
                _=inputs.retired.cancelled()=>break Ok(()),
                completed=catalog_requests.join_next(),if !catalog_requests.is_empty()=>{
                    let Some(Ok((id, result)))=completed else {break Err(io::Error::other("ACP catalog task failed"));};
                    let response=match result {
                        Ok(result) if schema.validate("ListSessionsResponse",&result).unwrap_or(false)=>json!({"jsonrpc":"2.0","id":id,"result":result}),
                        Err(failure) if failure.kind()==io::ErrorKind::InvalidInput=>error(id,-32602,"Invalid stored session cursor or scope"),
                        _=>error(id,-32603,"Stored session discovery unavailable"),
                    };
                    router.output.send(response).await?;
                    continue;
                },
                completed=setup_requests.join_next(),if collect_setup=>{
                    let Some(Ok((id, requested_session, completed)))=completed else {break Err(io::Error::other("ACP setup task failed"));};
                    if let Some(session_id)=requested_session.as_ref() {
                        sessions.finish_failed_load(session_id);
                        if completed.outcome().is_ok() { sessions.clear_cancellation_barrier(session_id); }
                    }
                    let outcome = match completed {
                        SetupCompletion::Ordinary(completed) => {
                            if let Some(binding) = completed.binding && let Err((_, binding)) = sessions.insert(binding) {
                                inputs.holder.hold(*binding);
                                router.output.send(error(id, -32603, "Session capacity unavailable")).await?;
                                continue;
                            }
                            completed.outcome
                        }
                        SetupCompletion::Held { checkout, outcome } => {
                            if outcome.is_ok() && checkout.install_in(&mut sessions).is_err() {
                                router.output.send(error(id, -32603, "Session capacity unavailable")).await?;
                                continue;
                            }
                            outcome
                        }
                    };
                    match outcome {
                        Ok((history,result))=>{
                            if requested_session.is_none() {
                                let Some(session_id)=result.get("sessionId").and_then(Value::as_str) else {
                                    router.output.send(error(id,-32603,"Created session identity unavailable")).await?;
                                    continue;
                                };
                                match inputs.holder.checkout(session_id) {
                                    HeldBindingCheckout::Ready(binding) => {
                                        if CheckedOutBinding::new(Arc::clone(&inputs.holder), *binding).install_in(&mut sessions).is_err() {
                                            router.output.send(error(id,-32603,"Session capacity unavailable")).await?;
                                            continue;
                                        }
                                    }
                                    HeldBindingCheckout::Busy | HeldBindingCheckout::Missing => {
                                        router.output.send(error(id,-32603,"Created session binding unavailable")).await?;
                                        continue;
                                    }
                                }
                            }
                            for update in history {router.output.send(update).await?;}
                            router.output.send(json!({"jsonrpc":"2.0","id":id,"result":result})).await?;
                        },
                        Err(failure)=>router.output.send(setup_error(id,&failure)).await?,
                    }
                    continue;
                },
                result=sessions.complete_next(),if sessions.has_pending()=>{result.map_err(io::Error::other)?;continue;},
                frame=router.input.recv()=>match frame {Some(frame)=>frame,None=>break Ok(())},
            };
            let method=frame.get("method").and_then(Value::as_str).unwrap_or("");
            let params=frame.get("params").cloned().unwrap_or_else(||json!({}));
            let Some(id)=frame.get("id").cloned() else {
                if method=="session/cancel"&&schema.validate("CancelNotification",&params).unwrap_or(false)
                    && let Some(session)=params.get("sessionId").and_then(Value::as_str) {let _cancel=sessions.cancel(session);}
                continue;
            };
            match method {
                "session/new"|"session/load"=>{
                    let create_new=method=="session/new";
                    let definition=if create_new {"NewSessionRequest"} else {"LoadSessionRequest"};
                    if !schema.validate(definition,&params).unwrap_or(false) {router.output.send(error(id,-32602,"Invalid session setup parameters")).await?;continue;}
                    let operation_id = if create_new {
                        match params.pointer("/_meta/codexRouter/operationId").and_then(Value::as_str)
                            .and_then(|value| OperationId::try_from(value.to_owned()).ok()) {
                            Some(value) => Some(value),
                            None => {router.output.send(error(id,-32602,"Create requires a UUIDv7 operation ID")).await?;continue;}
                        }
                    } else {None};
                    if create_new && let Some(service) = &local_service
                        && let Err(failure) = crate::session_creation::validate_create_actors(&params, Some(service)) {
                        router.output.send(setup_error(id, &failure)).await?;
                        continue;
                    }
                    if setup_requests.len()>=64 || (create_new && !sessions.has_setup_capacity(setup_requests.len())) {
                        router.output.send(error(id,-32603,"Session setup capacity unavailable")).await?;continue;
                    }
                    let requested_session=if create_new {None} else {params.get("sessionId").and_then(Value::as_str).map(str::to_owned)};
                    let known_session=if let Some(session_id)=requested_session.as_ref() {
                        match sessions.reserve_load(session_id,setup_requests.len()) {
                            Ok(binding)=>binding,
                            Err(crate::SessionRegistryError::Busy)=>{router.output.send(busy_error(id,"Session busy or capacity unavailable")).await?;continue;},
                            Err(_)=>{router.output.send(error(id,-32600,"Session busy or capacity unavailable")).await?;continue;},
                        }
                    } else {None};
                    let mut checked_out = None;
                    if known_session.is_none() && let Some(session_id) = requested_session.as_ref() {
                        match inputs.holder.checkout(session_id) {
                            HeldBindingCheckout::Ready(binding) => {
                                checked_out = Some(CheckedOutBinding::new(Arc::clone(&inputs.holder), *binding));
                            }
                            HeldBindingCheckout::Busy => {
                                sessions.finish_failed_load(session_id);
                                router.output.send(busy_error(id,"Held session is busy")).await?;
                                continue;
                            }
                            HeldBindingCheckout::Missing => {}
                        }
                    }
                    if let Some(checkout) = checked_out {
                        let generation = inputs.generation.clone();
                        let setup_work = async move {
                            let outcome = checkout.adopt(&generation, &params).await;
                            drop(frame);
                            #[cfg(feature = "test-support")]
                            if outcome.is_ok() {
                                crate::test_support::binding_preservation::observe_setup_completed();
                            }
                            (id, requested_session, SetupCompletion::Held { checkout, outcome })
                        };
                        #[cfg(feature = "test-support")]
                        let setup_work = crate::test_support::binding_preservation::scope(
                            crate::test_support::binding_preservation::current(), async move {
                                crate::test_support::binding_preservation::hold_at(crate::test_support::binding_preservation::PreservationPhase::BeforeSetupPoll).await;
                                setup_work.await
                            },
                        );
                        setup_requests.spawn(setup_work);
                        continue;
                    }
                    let cancellation_barrier=requested_session.as_ref().and_then(|session| sessions.cancellation_barrier(session));
                    let setup=SetupTaskInputs { cancellation_barrier, known_session, backend_path:inputs.backend_path.clone(), schemas:Arc::clone(&inputs.schemas), generation:inputs.generation.clone(), params, create_new, operation_id, recorder:Arc::clone(&inputs.recorder), scratch_scopes:Arc::clone(&inputs.scratch_scopes) };
                    if create_new {
                        let holder=Arc::clone(&inputs.holder);
                        let retirement = inputs.retired.clone();
                        let (result_sender,result_receiver)=tokio::sync::oneshot::channel();
                        holder.host_tasks().spawn(async move {
                            let mut outcome=run_session_setup(setup).await;
                            if let Some(mut binding)=outcome.binding.take() {
                                binding.admission_retirement = Some(retirement);
                                holder.hold(binding);
                            }
                            let _sent=result_sender.send(outcome);
                        });
                        setup_requests.spawn(async move {
                            let outcome=result_receiver.await.unwrap_or(SetupTaskOutput {
                                binding:None,
                                outcome:Err(crate::SessionSetupError::OutcomeUnknown),
                            });
                            drop(frame);
                            (id,None,SetupCompletion::Ordinary(outcome))
                        });
                    } else {
                        setup_requests.spawn(async move {
                            let outcome = run_session_setup(setup).await;
                            drop(frame);
                            (id, requested_session, SetupCompletion::Ordinary(outcome))
                        });
                    }
                },
                "session/prompt"=>{
                    match sessions.begin_prompt(&mut schema,id.clone(),params) {
                        Ok(())=>{},
                        Err(crate::SessionRegistryError::Busy)=>router.output.send(busy_error(id,"Session prompt unavailable or already pending")).await?,
                        Err(_)=>router.output.send(error(id,-32600,"Session prompt unavailable or already pending")).await?,
                    }
                },
                "session/list"=>{
                    if !schema.validate("ListSessionsRequest",&params).unwrap_or(false) {router.output.send(error(id,-32602,"Invalid session list parameters")).await?;continue;}
                    if catalog_requests.len()>=64 {router.output.send(error(id,-32603,"Stored session request capacity unavailable")).await?;continue;}
                    let provider=Arc::clone(&inputs.stored_sessions);
                    catalog_requests.spawn(async move {
                        // Retain the inbound frame's byte permit through asynchronous work.
                        let result=provider.list(params).await;
                        drop(frame);
                        (id,result)
                    });
                },
                _=>router.output.send(error(id,-32601,"ACP method not supported")).await?,
            }
        }
    }.await;
    setup_requests.abort_all();
    while setup_requests.join_next().await.is_some() {}
    catalog_requests.abort_all();
    while catalog_requests.join_next().await.is_some() {}
    #[cfg(feature = "test-support")]
    crate::test_support::binding_preservation::hold_registry_handoff().await;
    #[cfg(feature = "test-support")]
    crate::test_support::binding_preservation::hold_at(
        crate::test_support::binding_preservation::PreservationPhase::HostStopBeforeHandoff,
    )
    .await;
    sessions.shutdown().await;
    result
}
enum SetupCompletion {
    Ordinary(SetupTaskOutput),
    Held {
        checkout: CheckedOutBinding,
        outcome: SetupOutcome,
    },
}
impl SetupCompletion {
    fn outcome(&self) -> &SetupOutcome {
        match self {
            Self::Ordinary(completed) => &completed.outcome,
            Self::Held { outcome, .. } => outcome,
        }
    }
}

struct CheckedOutBinding {
    holder: Arc<dyn UnmaterializedBindingStore>,
    binding: Option<AcpSessionBinding>,
}
impl CheckedOutBinding {
    fn new(holder: Arc<dyn UnmaterializedBindingStore>, binding: AcpSessionBinding) -> Self {
        Self {
            holder,
            binding: Some(binding),
        }
    }

    async fn adopt(&self, generation: &CodexGeneration, params: &Value) -> SetupOutcome {
        let binding = self
            .binding
            .as_ref()
            .ok_or(crate::SessionSetupError::OutcomeUnknown)?;
        run_held_adoption(binding, generation, params).await
    }

    fn install_in(
        mut self,
        sessions: &mut AcpSessionRegistry,
    ) -> Result<(), crate::SessionRegistryError> {
        let binding = self
            .binding
            .take()
            .ok_or(crate::SessionRegistryError::NotLoaded)?;
        let session_id = binding.session_id().to_owned();
        let generation = binding.generation().clone();
        match sessions.insert(binding) {
            Ok(()) => {
                self.holder.finish(&session_id, &generation);
                Ok(())
            }
            Err((error, binding)) => {
                self.binding = Some(*binding);
                Err(error)
            }
        }
    }
}
impl Drop for CheckedOutBinding {
    fn drop(&mut self) {
        if let Some(binding) = self.binding.take() {
            self.holder.restore(binding);
        }
    }
}
fn error(id: Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}
fn busy_error(id: Value, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32600,"message":message,"data":{"kind":"busy"}}})
}
fn setup_error(id: Value, failure: &crate::SessionSetupError) -> Value {
    if matches!(failure, crate::SessionSetupError::Busy) {
        return busy_error(id, "Native session has an active turn");
    }
    if matches!(
        failure,
        crate::SessionSetupError::NativeThreadStatusUnavailable
    ) {
        return json!({"jsonrpc":"2.0","id":id,"error":{"code":-32603,"message":"Native session status unavailable","data":{"kind":"invalidThreadStatus"}}});
    }
    let code = if matches!(
        failure,
        crate::SessionSetupError::InvalidParameters
            | crate::SessionSetupError::ConfigurationMismatch
    ) {
        -32602
    } else {
        -32603
    };
    match failure {
        crate::SessionSetupError::ModelMismatch {
            requested,
            effective,
        } => json!({
            "jsonrpc":"2.0","id":id,
            "error":{"code":code,"message":"Native session setup rejected","data":{
                "kind":"modelMismatch","requested":requested,"effective":effective
            }}
        }),
        crate::SessionSetupError::AccessMismatch {
            requested,
            effective,
        } => json!({
            "jsonrpc":"2.0","id":id,
            "error":{"code":code,"message":"Native session setup rejected","data":{
                "kind":"accessMismatch","requested":requested,"effective":effective
            }}
        }),
        crate::SessionSetupError::HostLocationsUnavailable { location } => json!({
            "jsonrpc":"2.0","id":id,
            "error":{"code":code,"message":"Router session setup rejected","data":{
                "kind":"hostLocationsUnavailable","location":location
            }}
        }),
        _ => error(id, code, "Native session setup rejected"),
    }
}
