//! Codex route admission scoped to active Codex Sessions, not the ACP socket.
use crate::acp_connection_dispatch::route_codex_sessions;
use crate::{
    AcpConnectionContext, AcpConnectionInputs, AcpOutputSender, AcpRouteFuture, AcpRouterChannels,
    AcpSessionRoute, bounded_acp_output,
};
use collaboration_protocol::UuidIdentity;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// The Host owns the generation gate and constructs one admitted Codex view.
pub trait CodexAdmissionSource: Send + Sync {
    fn acquire(&self) -> io::Result<AcpConnectionInputs>;
}

pub fn lazy_codex_session_route(
    admission: Arc<dyn CodexAdmissionSource>,
    service_id: UuidIdentity,
) -> Box<dyn AcpSessionRoute> {
    Box::new(LazyCodexSessionRoute {
        admission,
        service_id,
    })
}

struct LazyCodexSessionRoute {
    service_id: UuidIdentity,
    admission: Arc<dyn CodexAdmissionSource>,
}

impl AcpSessionRoute for LazyCodexSessionRoute {
    fn endpoint_id(&self) -> &str {
        "codex-local"
    }

    fn run(
        self: Box<Self>,
        router: AcpRouterChannels,
        _context: AcpConnectionContext,
    ) -> AcpRouteFuture {
        Box::pin(serve_lazy_codex_sessions(
            router,
            self.admission,
            self.service_id,
        ))
    }
}

struct ActiveGeneration {
    epoch: u64,
    input: AcpOutputSender,
    closed: CancellationToken,
    retirement: CancellationToken,
    task: JoinHandle<()>,
    session_ids: BTreeSet<String>,
    pending: BTreeMap<String, Value>,
}

enum GenerationEvent {
    Frame { epoch: u64, frame: Value },
    Closed { epoch: u64 },
}

fn unavailable(id: Value, reason: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":reason}})
}

async fn retire_active_generation(
    active: &mut Option<ActiveGeneration>,
    retired_sessions: &mut BTreeSet<String>,
    output: &AcpOutputSender,
) -> io::Result<()> {
    let Some(current) = active.take() else {
        return Ok(());
    };
    close_generation(current.closed, current.task).await;
    retired_sessions.extend(current.session_ids);
    for (_, id) in current.pending {
        output
            .send(unavailable(id, "Codex generation retired"))
            .await?;
    }
    Ok(())
}

async fn close_generation(closed: CancellationToken, task: JoinHandle<()>) {
    closed.cancel();
    let _generation_stopped = task.await;
}

async fn serve_lazy_codex_sessions(
    mut router: AcpRouterChannels,
    admission: Arc<dyn CodexAdmissionSource>,
    service_id: UuidIdentity,
) -> io::Result<()> {
    let (event_sender, mut event_receiver) = mpsc::channel::<GenerationEvent>(1024);
    let mut active: Option<ActiveGeneration> = None;
    let mut retired_sessions = BTreeSet::<String>::new();
    let mut next_epoch = 0_u64;
    let result = async {
        loop {
            let retired = active.as_ref().map(|current| current.retirement.clone());
            tokio::select! {
                _ = router.closed.cancelled() => break,
                () = wait_for_retirement(retired) => {
                    retire_active_generation(&mut active, &mut retired_sessions, &router.output).await?;
                }
            event = event_receiver.recv() => {
                let Some(event) = event else { break; };
                match event {
                    GenerationEvent::Frame { epoch, frame } => {
                        let Some(current) = active.as_mut().filter(|current| current.epoch == epoch) else { continue; };
                        if let Some(id) = frame.get("id") {
                            let key = id.to_string();
                            if current.pending.remove(&key).is_some()
                                && let Some(session_id) = frame.pointer("/result/sessionId").and_then(Value::as_str) {
                                retired_sessions.remove(session_id);
                                current.session_ids.insert(session_id.to_owned());
                            }
                        }
                        router.output.send(frame).await?;
                    }
                    GenerationEvent::Closed { epoch } => {
                        if active.as_ref().is_none_or(|current| current.epoch != epoch) { continue; }
                        retire_active_generation(&mut active, &mut retired_sessions, &router.output).await?;
                    }
                }
            }
            frame = router.input.recv() => {
                let Some(frame) = frame else { break; };
                if active.as_ref().is_some_and(|current| current.retirement.is_cancelled()) {
                    retire_active_generation(&mut active, &mut retired_sessions, &router.output).await?;
                }
                let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
                let id = frame.get("id").cloned();
                let session_id = frame.pointer("/params/sessionId").and_then(Value::as_str);
                let admission_request = matches!(method, "session/new" | "session/load" | "session/resume" | "session/list");
                if !admission_request && session_id.is_some_and(|id| retired_sessions.contains(id)) {
                    if let Some(id) = id { router.output.send(unavailable(id, "Codex session generation retired")).await?; }
                    continue;
                }
                if active.is_none() && admission_request {
                    match admission.acquire() {
                        Err(error) => {
                            if let Some(id) = id { router.output.send(unavailable(id, &format!("Codex backend unavailable: {error}"))).await?; }
                            continue;
                        }
                        Ok(inputs) => {
                            next_epoch = next_epoch.saturating_add(1);
                            let epoch = next_epoch;
                            let retirement = inputs.retired.clone();
                            let closed = router.closed.child_token();
                            let (input, input_receiver) = bounded_acp_output(closed.clone());
                            let (output, mut output_receiver) = bounded_acp_output(closed.clone());
                            let inner = AcpRouterChannels { input: input_receiver, output, closed: closed.clone() };
                            let sender = event_sender.clone();
                            let local_service = service_id.clone();
                            let generation_closed = closed.clone();
                            let task = tokio::spawn(async move {
                                let run = route_codex_sessions(inner, inputs, Some(local_service));
                                tokio::pin!(run);
                                let forward = async {
                                    while let Some(frame) = output_receiver.recv().await {
                                        if sender.send(GenerationEvent::Frame { epoch, frame: (*frame).clone() }).await.is_err() { break; }
                                    }
                                };
                                tokio::select! {
                                    _routing_ended = &mut run => {}
                                    () = generation_closed.cancelled() => {
                                        let _handoff = run.await;
                                        return;
                                    }
                                    () = forward => {
                                        generation_closed.cancel();
                                        let _handoff = run.await;
                                        return;
                                    }
                                }
                                tokio::select! {
                                    () = generation_closed.cancelled() => {}
                                    _sent = sender.send(GenerationEvent::Closed { epoch }) => {}
                                }
                            });
                            active = Some(ActiveGeneration { epoch, input, closed, retirement, task, session_ids: BTreeSet::new(), pending: BTreeMap::new() });
                        }
                    }
                }
                let Some(current) = active.as_mut() else {
                    if let Some(id) = id { router.output.send(unavailable(id, "Codex session unavailable")).await?; }
                    continue;
                };
                if let Some(id) = id {
                    current.pending.insert(id.to_string(), id);
                }
                current.input.send((*frame).clone()).await?;
            }
            }
        }
        Ok(())
    }.await;
    drop(event_receiver);
    if let Some(current) = active {
        close_generation(current.closed, current.task).await;
    }
    result
}

async fn wait_for_retirement(retired: Option<CancellationToken>) {
    match retired {
        Some(retired) => retired.cancelled().await,
        None => std::future::pending().await,
    }
}
