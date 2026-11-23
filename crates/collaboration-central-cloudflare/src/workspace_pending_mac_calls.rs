//! Owns pending calls and completion tasks for each physical socket.
use super::{LinkReply, SocketAttachment, socket_open, socket_tag};
use crate::worker_clock::WorkspaceClock;
use central_link_protocol::{
    CorrelationId, DataFrame, FrameDirection, FreshCorrelationId, LinkFrame, MacCall,
    MacCallAddress, TypedCallCorrelations, encode_text,
};
use fleet_rpc::CallFailure;
use fleet_runtime_ports::{Clock, SyncTime};
use futures_channel::oneshot;
use futures_util::future::select;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};
use worker::{State, WebSocket};

pub(super) struct WrittenMacCall<'a> {
    pub(super) pending: PendingAttempt<'a>,
    pub(super) answer: oneshot::Receiver<LinkReply>,
}

struct Waiter {
    address: MacCallAddress,
    deadline: SyncTime,
    answer: oneshot::Sender<LinkReply>,
}

type SocketWaiters = BTreeMap<String, TypedCallCorrelations<Waiter>>;

type SocketCompletions = BTreeMap<String, Vec<oneshot::Sender<()>>>;

#[derive(Clone, Default)]
pub(in super::super) struct PendingMacCalls {
    waiters: Arc<Mutex<SocketWaiters>>,
    completions: Arc<Mutex<SocketCompletions>>,
    #[cfg(feature = "test-support")]
    approval_before_write: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl PendingMacCalls {
    pub(super) fn begin_mac_call(
        &self,
        clock: &WorkspaceClock,
        socket: &WebSocket,
        socket_tag: String,
        request: Box<MacCall>,
        address: MacCallAddress,
        deadline: SyncTime,
    ) -> Result<WrittenMacCall<'_>, CallFailure> {
        if !socket_open(socket) || clock.now() >= deadline {
            return Err(CallFailure::NeverReached);
        }
        SocketAttachment::read(socket)
            .map_err(|_| CallFailure::NeverReached)?
            .ok_or(CallFailure::NeverReached)?
            .mac_capabilities
            .admit_outbound(&request)?;
        let correlation = FreshCorrelationId::mint_at(clock.now().unix_millis());
        let frame = LinkFrame::Data(DataFrame::CallMac {
            correlation_id: correlation.wire_id().clone(),
            request,
        });
        let text =
            encode_text(&frame, FrameDirection::ToRouter).map_err(|_| CallFailure::NeverReached)?;
        let written = self.register(socket_tag, correlation, address, deadline)?;
        if clock.now() >= deadline {
            return Err(CallFailure::NeverReached);
        }
        socket
            .send_with_str(text)
            .map_err(|_| CallFailure::AnswerLost)?;
        Ok(written)
    }

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
        self.waiters.lock().unwrap_or_else(PoisonError::into_inner)
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

    pub(in super::super) fn run_socket_completion(
        &self,
        state: &State,
        socket: &WebSocket,
        completion: collaboration_central_core::LinkCallCompletion,
    ) {
        let Some(tag) = socket_tag(state, socket) else {
            return;
        };
        let stopped = self.socket_ended(tag);
        state.wait_until(async move {
            let _ = select(stopped, completion).await;
        });
    }

    pub(super) fn socket_ended(&self, tag: String) -> oneshot::Receiver<()> {
        let (stop, stopped) = oneshot::channel();
        let mut completions = self
            .completions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let tasks = completions.entry(tag).or_default();
        tasks.retain(|task| !task.is_canceled());
        tasks.push(stop);
        stopped
    }

    #[cfg(feature = "test-support")]
    pub(super) fn approval_before_write(&self) -> Option<oneshot::Receiver<()>> {
        let (cancel, canceled) = oneshot::channel();
        let mut current = self
            .approval_before_write
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if current.as_ref().is_some_and(|cancel| !cancel.is_canceled()) {
            return None;
        }
        *current = Some(cancel);
        Some(canceled)
    }

    #[cfg(feature = "test-support")]
    pub(in super::super) fn drop_approval_before_write(&self) -> bool {
        self.approval_before_write
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some_and(|cancel| cancel.send(()).is_ok())
    }

    pub(in super::super) fn close(
        &self,
        state: &State,
        socket: &WebSocket,
        code: u16,
        reason: Option<&str>,
    ) -> worker::Result<()> {
        if let Some(socket_tag) = socket_tag(state, socket) {
            self.lock().remove(&socket_tag);
            self.completions
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&socket_tag);
        }
        socket.close(Some(code), reason)
    }

    #[cfg(feature = "test-support")]
    pub(in super::super) fn is_waiting(&self, correlation: &CorrelationId) -> bool {
        self.lock().values().any(|socket| {
            socket
                .pending(correlation)
                .is_some_and(|waiter| !waiter.answer.is_canceled())
        })
    }
}

pub(super) struct PendingAttempt<'a> {
    calls: &'a PendingMacCalls,
    socket_tag: String,
    correlation_id: CorrelationId,
}

impl Drop for PendingAttempt<'_> {
    fn drop(&mut self) {
        self.calls.retire(&self.socket_tag, &self.correlation_id);
    }
}
