use super::workspace_link_frame::send;
use super::workspace_socket_attachment::SocketAttachment;
use super::workspace_typed_link::PendingMacCalls;
use super::{CentralObject, OrgRuntime, WorkspaceClock};
use central_link_protocol::{DataFrame, TimeLeft};
use fleet_contracts::{MachineConnectionAttachment, MailDeliveryReady};
use fleet_runtime::{DeferredDueTask, ReplyDueWork, ReplyDueWorkOwner};
use fleet_runtime_ports::Clock;
use std::sync::{Arc, Mutex};
use worker::{Response, ResponseBody, Result, State, WebSocket, send::SendWrapper};

pub(super) struct WorkerReplyDueWork {
    runtime: std::sync::Weak<OrgRuntime>,
    mail: WorkerMailHandoff,
    #[cfg(feature = "test-support")]
    activity: Arc<ReplyDueActivity>,
}

impl ReplyDueWorkOwner for WorkerReplyDueWork {
    fn run_after_reply(&self, task: DeferredDueTask) {
        let mail = self.mail.clone();
        let runtime = self.runtime.clone();
        #[cfg(feature = "test-support")]
        let pending = self.activity.begin();
        self.mail.state.wait_until(async move {
            #[cfg(feature = "test-support")]
            let _pending = pending;
            if super::worker_due_yield::yield_worker_due_batches()
                .await
                .is_err()
            {
                worker::console_error!(
                    "Central stopped deferred due work because its platform turn failed"
                );
                return;
            }
            task.await;
            if let Some(runtime) = runtime.upgrade() {
                mail.flush_ready_mail(&runtime).await;
            }
        });
    }
}

pub(super) fn write_signin_reply(
    response: Response,
    work: ReplyDueWork,
    scheduler: impl FnOnce() -> Arc<WorkerReplyDueWork>,
) -> Result<Response> {
    if work.is_empty() {
        return Ok(response);
    }
    let (builder, body) = response.into_parts();
    let writer = web_sys::TransformStream::new().map_err(worker::Error::from)?;
    let scheduler = scheduler();
    let readable = writer.readable();
    #[cfg(feature = "test-support")]
    let readable = hold_private_response_sink(readable, &scheduler)?;
    let writable = writer.writable();
    // The writer can settle only as the response sink reads the pipe. Its
    // rejection or cancellation drops the unaccepted handoff.
    worker::wasm_bindgen_futures::spawn_local(async move {
        use js_sys::futures::JsFuture;
        let accepted = match body {
            ResponseBody::Stream(body) => JsFuture::from(body.pipe_to(&writable)).await.is_ok(),
            ResponseBody::Body(bytes) => match writable.get_writer() {
                Ok(writer) => {
                    let bytes = js_sys::Uint8Array::from(bytes.as_slice());
                    JsFuture::from(writer.write_with_chunk(bytes.as_ref()))
                        .await
                        .is_ok()
                        && JsFuture::from(writer.close()).await.is_ok()
                }
                Err(_) => false,
            },
            ResponseBody::Empty => JsFuture::from(writable.close()).await.is_ok(),
        };
        if accepted {
            work.handoff(scheduler);
        }
    });
    Ok(builder.stream(readable))
}

#[cfg(feature = "test-support")]
fn hold_private_response_sink(
    readable: web_sys::ReadableStream,
    scheduler: &WorkerReplyDueWork,
) -> Result<web_sys::ReadableStream> {
    use fleet_runtime::PausePoint;
    let Some(runtime) = scheduler.runtime.upgrade() else {
        return Ok(readable);
    };
    let point = PausePoint::BeforePrivateReplySinkReads;
    if !runtime.runtime().pause_points().central_pause_armed(point) {
        return Ok(readable);
    }
    // Add a blocked downstream sink to the real reply stream. The normal
    // pipe remains backpressured while this sink is unavailable.
    let sink = web_sys::TransformStream::new().map_err(worker::Error::from)?;
    let writable = sink.writable();
    worker::wasm_bindgen_futures::spawn_local(async move {
        runtime
            .runtime()
            .pause_points()
            .reached_central(point)
            .await;
        let _ = js_sys::futures::JsFuture::from(readable.pipe_to(&writable)).await;
    });
    Ok(sink.readable())
}

#[derive(Clone)]
pub(super) struct WorkerMailHandoff {
    state: Arc<SendWrapper<State>>,
    clock: WorkspaceClock,
    pending_mac_calls: PendingMacCalls,
    mail_pushes: Arc<Mutex<Vec<MailDeliveryReady>>>,
}

impl CentralObject {
    pub(super) fn reply_due_work_owner(
        &self,
        runtime: &Arc<OrgRuntime>,
    ) -> Arc<WorkerReplyDueWork> {
        Arc::new(WorkerReplyDueWork {
            runtime: Arc::downgrade(runtime),
            mail: self.due_work_mail(),
            #[cfg(feature = "test-support")]
            activity: self.reply_due_activity.clone(),
        })
    }

    pub(super) fn due_work_mail(&self) -> WorkerMailHandoff {
        WorkerMailHandoff {
            state: self.state.clone(),
            clock: self.clock.clone(),
            pending_mac_calls: self.pending_mac_calls.clone(),
            mail_pushes: self.mail_pushes.clone(),
        }
    }

    pub(super) async fn flush_ready_mail(&self, runtime: &OrgRuntime) {
        self.due_work_mail().flush_ready_mail(runtime).await;
    }
}

impl WorkerMailHandoff {
    pub(super) async fn flush_ready_mail(&self, runtime: &OrgRuntime) {
        let ready = std::mem::take(
            &mut *self
                .mail_pushes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for message in ready {
            for socket in self
                .state
                .get_websockets_with_tag(message.machine_id.as_str())
            {
                if socket.as_ref().ready_state() != web_sys::WebSocket::OPEN {
                    continue;
                }
                if let Ok(Some(attachment)) = SocketAttachment::read(&socket)
                    && let Err(error) = self
                        .offer_windowed_mail(
                            runtime,
                            &socket,
                            &attachment.authority,
                            &message.message_id,
                        )
                        .await
                {
                    worker::console_warn!("central delivery offer failed: {error}");
                }
            }
        }
    }

    pub(super) async fn offer_windowed_mail(
        &self,
        runtime: &OrgRuntime,
        socket: &WebSocket,
        attachment: &MachineConnectionAttachment,
        message_id: &workspace_ids::MessageId,
    ) -> Result<bool> {
        use fleet_contracts::mail_link::WindowedDeliveryDecision;
        let decision = runtime
            .prepare_windowed_delivery(attachment, message_id)
            .await
            .map_err(|error| worker::Error::RustError(error.to_string()))?;
        match decision {
            WindowedDeliveryDecision::Ready(delivery) => {
                if socket.as_ref().ready_state() != web_sys::WebSocket::OPEN {
                    return Ok(false);
                }
                let now = chrono::DateTime::from_timestamp_millis(self.clock.now().unix_millis())
                    .ok_or_else(|| {
                    worker::Error::RustError("offer clock unavailable".into())
                })?;
                if now >= delivery.expires_at {
                    return Ok(true);
                }
                let time_left = if matches!(
                    delivery.message.body,
                    workspace_model::central_mail::MessageBody::Direct { .. }
                ) {
                    let Some(time_left) =
                        TimeLeft::at_handoff(delivery.message.created_at, delivery.expires_at, now)
                    else {
                        return Ok(true);
                    };
                    Some(time_left)
                } else {
                    None
                };
                send(
                    socket,
                    DataFrame::Deliver {
                        message: Box::new(central_link_protocol::DeliveryMessage::from(
                            *delivery.message,
                        )),
                        sender: delivery.sender,
                        time_left,
                    },
                )?;
                Ok(true)
            }
            WindowedDeliveryDecision::Ineligible => Ok(true),
            WindowedDeliveryDecision::AuthorityLapsed(cause) => {
                self.pending_mac_calls
                    .close(&self.state, socket, cause.code(), None)?;
                Ok(false)
            }
        }
    }
}

#[cfg(feature = "test-support")]
#[derive(Default)]
pub(super) struct ReplyDueActivity {
    scheduled: std::sync::atomic::AtomicU64,
    pending: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "test-support")]
impl ReplyDueActivity {
    fn begin(self: &Arc<Self>) -> PendingReplyDueWork {
        use std::sync::atomic::Ordering;
        self.scheduled.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_add(1, Ordering::Relaxed);
        PendingReplyDueWork(self.clone())
    }

    pub(super) fn counts(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.scheduled.load(Ordering::Relaxed),
            self.pending.load(Ordering::Relaxed),
        )
    }
}

#[cfg(feature = "test-support")]
struct PendingReplyDueWork(Arc<ReplyDueActivity>);

#[cfg(feature = "test-support")]
impl Drop for PendingReplyDueWork {
    fn drop(&mut self) {
        self.0
            .pending
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}
